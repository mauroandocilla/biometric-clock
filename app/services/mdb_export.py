# app/services/mdb_export.py
import csv
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import pandas as pd


def find_access_driver():
    try:
        import pyodbc

        drivers = [d.lower() for d in pyodbc.drivers()]
        for name in [
            "microsoft access driver (*.mdb, *.accdb)",
            "microsoft access driver (*.mdb)",
            "microsoft access driver (*.accdb)",
            "mdbtools",
        ]:
            if name in drivers:
                return name
    except Exception:
        pass
    return None


def read_with_pyodbc(mdb_path: Path):
    import pyodbc

    driver = find_access_driver()
    if not driver:
        raise RuntimeError(
            "No se encontró un driver ODBC de Access válido para pyodbc."
        )
    conn_str = f"DRIVER={{{driver}}};DBQ={mdb_path};"
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        users = pd.read_sql("SELECT USERID, Name FROM USERINFO", conn)
        inout = pd.read_sql("SELECT USERID, CHECKTIME FROM CHECKINOUT", conn)
    return users, inout


def read_with_mdbtools(mdb_path: Path):
    def export_table(table):
        # -D fecha; -R '\n' separador de filas; -d ',' separador cols; -Q fuerza comillas
        cmd = [
            "mdb-export",
            "-D",
            "%Y-%m-%d %H:%M:%S",
            "-R",
            "\\n",
            "-d",
            ",",
            "-Q",
            str(mdb_path),
            table,
        ]
        out = subprocess.check_output(cmd, text=True)
        from io import StringIO

        return pd.read_csv(StringIO(out))

    users = export_table("USERINFO")
    inout = export_table("CHECKINOUT")
    users.columns = [c.strip() for c in users.columns]
    inout.columns = [c.strip() for c in inout.columns]
    # Asegura columnas exactas
    users = users.rename(columns={"UserID": "USERID", "NAME": "Name"})
    inout = inout.rename(columns={"UserID": "USERID", "CheckTime": "CHECKTIME"})
    return users[["USERID", "Name"]], inout[["USERID", "CHECKTIME"]]


def parse_dt(value):
    if isinstance(value, datetime):
        return value
    s = str(value).strip().replace("T", " ")
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%y %I:%M:%S %p",
        "%m/%d/%y %I:%M %p",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        raise ValueError(f"No puedo parsear CHECKTIME: {value!r}")
    return dt.to_pydatetime()


def fmt_date(dt: datetime):
    y = dt.strftime("%y")
    m = str(int(dt.strftime("%m")))
    d = str(int(dt.strftime("%d")))
    return f"{m}/{d}/{y}"


def fmt_time(dt: datetime):
    h = str(int(dt.strftime("%H")))
    mm = dt.strftime("%M")
    return f"{h}:{mm}"


def _parse_checktime_series(s: pd.Series) -> pd.Series:
    # Primer intento: mes/día/año (común en Access)
    dt1 = pd.to_datetime(
        s.astype(str).str.replace("T", " ", regex=False),
        errors="coerce",
        dayfirst=False,
    )
    # Segundo intento: día/mes/año
    need = dt1.isna()
    if need.any():
        dt2 = pd.to_datetime(
            s[need].astype(str).str.replace("T", " ", regex=False),
            errors="coerce",
            dayfirst=True,
        )
        dt1 = dt1.copy()
        dt1[need] = dt2
    return dt1


def build_rows(
    users_df, inout_df, *, year: int | None = None, month: int | None = None
):
    # Normaliza tipos
    users = users_df.rename(columns=str.strip).copy()
    inout = inout_df.rename(columns=str.strip).copy()

    users["USERID"] = users["USERID"].astype(str)
    inout["USERID"] = inout["USERID"].astype(str)

    # Une para tener Name en cada fila de CHECKINOUT
    df = inout.merge(users[["USERID", "Name"]], on="USERID", how="left")
    df = df.dropna(subset=["Name"])  # descarta registros sin usuario

    # Parseo de fecha/hora robusto
    dt = _parse_checktime_series(df["CHECKTIME"])
    df = df.assign(_dt=dt)
    if year is not None:
        df = df[df["_dt"].dt.year == year]
    if month is not None:
        df = df[df["_dt"].dt.month == month]

    # Formatos de salida
    def fmt_mdyy(d: datetime.date) -> str:
        return f"{int(d.month)}/{int(d.day)}/{str(d.year)[-2:]}"

    df["_date_str"] = df["_date"].map(fmt_mdyy)

    # Ordena por hora y agrupa
    df["_mins"] = df["_time"].str.split(":").map(lambda x: int(x[0]) * 60 + int(x[1]))
    df = df.sort_values(by=["_name", "_date", "_mins"])

    grouped = df.groupby(["_name", "_date_str"])["_time"].apply(list).reset_index()

    # Construye filas: [Nombre, Fecha, Hora1, Hora2, ...]
    rows = [
        [row["_name"], row["_date_str"], *row["_time"]] for _, row in grouped.iterrows()
    ]

    # Orden final por nombre y fecha
    def key(row):
        # row[0]=Nombre, row[1]=M/D/YY
        m, d, y = row[1].split("/")
        y = int("20" + y) if len(y) == 2 else int(y)
        return (row[0].upper(), y, int(m), int(d))

    rows.sort(key=key)
    return rows


def write_csv(rows, out_path: Path):
    headers = ["Nombre", "Fecha"]
    max_hours = max((len(r) - 2 for r in rows), default=0)
    headers += [f"Hora{i}" for i in range(1, max_hours + 1)]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            filled = r + [""] * (len(headers) - len(r))
            w.writerow(filled)


def export_mdb_to_csv(
    mdb_path: Path, out_path: Path, *, year: int | None = None, month: int | None = None
):
    try:
        users_df, inout_df = read_with_pyodbc(mdb_path)
    except Exception as e1:
        try:
            users_df, inout_df = read_with_mdbtools(mdb_path)
        except Exception as e2:
            raise RuntimeError(
                f"No pude leer la base .mdb. "
                f"Intento pyodbc falló: {e1}\nIntento mdbtools falló: {e2}"
            )
    rows = build_rows(users_df, inout_df, year=year, month=month)
    write_csv(rows, out_path)
    return out_path
