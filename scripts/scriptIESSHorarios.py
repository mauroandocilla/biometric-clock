#!/usr/bin/env python3
"""
Genera un CSV con columnas: Nombre, Fecha, Hora1, Hora2, ...
a partir de USERINFO(USERID, Name) y CHECKINOUT(USERID, CHECKTIME)
de un archivo MS Access .mdb.

Uso:
    python export_mdb_inout.py database.mdb salida.csv
Dependencias:
    - Opción Windows (recomendada): pyodbc
      * pip install pyodbc pandas
      * Instalar "Microsoft Access Database Engine 2016 Redistributable"
    - Opción Mac/Linux: mdbtools (brew install mdbtools / apt-get install mdbtools)
      * pip install pandas
"""

import csv
import sys
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

def find_access_driver():
    """Intenta detectar un driver ODBC de Access disponible (Windows o MDBTools ODBC)."""
    try:
        import pyodbc
        drivers = [d.lower() for d in pyodbc.drivers()]
        # Prioridades comunes en Windows
        for name in [
            "microsoft access driver (*.mdb, *.accdb)",
            "microsoft access driver (*.mdb)",
            "microsoft access driver (*.accdb)",
            "mdbtools"
        ]:
            if name in drivers:
                return name
    except Exception:
        pass
    return None

def read_with_pyodbc(mdb_path):
    import pyodbc
    driver = find_access_driver()
    if not driver:
        raise RuntimeError("No se encontró un driver ODBC de Access válido para pyodbc.")
    # Conexión ODBC. Para MDBTools ODBC en Mac/Linux, el nombre puede requerir DSN previo.
    conn_str = f"DRIVER={{{driver}}};DBQ={mdb_path};"
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        users = pd.read_sql("SELECT USERID, Name FROM USERINFO", conn)
        inout = pd.read_sql("SELECT USERID, CHECKTIME FROM CHECKINOUT", conn)
    return users, inout

def read_with_mdbtools(mdb_path):
    """Lee tablas con mdb-export (sin ODBC)."""
    def export_table(table):
        cmd = ["mdb-export", "-D", "%Y-%m-%d %H:%M:%S", str(mdb_path), table]
        out = subprocess.check_output(cmd, text=True)
        # mdb-export produce CSV
        from io import StringIO
        return pd.read_csv(StringIO(out))
    users = export_table("USERINFO")
    inout = export_table("CHECKINOUT")
    # Normalizar nombres de columnas por si vienen con mayúsculas/espacios
    users.columns = [c.strip() for c in users.columns]
    inout.columns = [c.strip() for c in inout.columns]
    return users[["USERID", "Name"]], inout[["USERID", "CHECKTIME"]]

def parse_dt(value):
    """Convierte CHECKTIME a datetime (soporta segundos y varios formatos)."""
    if isinstance(value, datetime):
        return value

    s = str(value).strip().replace("T", " ")
    # Intentos comunes (con y sin segundos, Y-m-d y m/d/Y y d/m/Y)
    formats = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
        "%m/%d/%y %H:%M:%S",  "%m/%d/%y %H:%M",
        "%m/%d/%Y %H:%M:%S",  "%m/%d/%Y %H:%M",
        "%d/%m/%y %H:%M:%S",  "%d/%m/%y %H:%M",
        "%d/%m/%Y %H:%M:%S",  "%d/%m/%Y %H:%M",
        # Por si hubiera AM/PM (no suele pasar, pero no estorba)
        "%m/%d/%y %I:%M:%S %p", "%m/%d/%y %I:%M %p",
        "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    # Último recurso: usar pandas.to_datetime, primero asumiendo month/day
    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        raise ValueError(f"No puedo parsear CHECKTIME: {value!r}")
    return dt.to_pydatetime()

def fmt_date(dt):
    """Formatea fecha como M/D/YY (sin ceros a la izquierda, estilo de tu ejemplo)."""
    y = dt.strftime("%y")
    m = str(int(dt.strftime("%m")))
    d = str(int(dt.strftime("%d")))
    return f"{m}/{d}/{y}"

def fmt_time(dt):
    """Formatea hora como H:MM (24h, sin cero a la izquierda en la hora)."""
    h = str(int(dt.strftime("%H")))
    mm = dt.strftime("%M")
    return f"{h}:{mm}"

def build_rows(users_df, inout_df):
    # Asegurar tipos correctos
    users_df["USERID"] = users_df["USERID"].astype(str)
    inout_df["USERID"] = inout_df["USERID"].astype(str)

    # Diccionario USERID -> Name
    user_map = dict(zip(users_df["USERID"], users_df["Name"]))

    # Grupo (USERID, date_str) -> lista de times_str
    grouped = defaultdict(list)

    for _, row in inout_df.iterrows():
        uid = row["USERID"]
        if uid not in user_map:
            continue
        dt = parse_dt(row["CHECKTIME"])
        date_str = fmt_date(dt)
        time_str = fmt_time(dt)
        grouped[(uid, date_str)].append(time_str)

    # Construir filas finales: [Nombre, Fecha, Hora1..]
    rows = []
    for (uid, date_str), times in grouped.items():
        # Ordenar por hora:minuto
        def to_minutes(t):
            h, m = t.split(":")
            return int(h) * 60 + int(m)
        times_sorted = sorted(times, key=to_minutes)
        rows.append([user_map[uid], date_str, *times_sorted])

    # ---- Ordenar por Nombre y Fecha ----
    def parse_date_key(d):
        # M/D/YY -> datetime
        m, d, y = d.split("/")
        y = "20" + y if len(y) == 2 else y
        return datetime(int(y), int(m), int(d))

    rows.sort(key=lambda r: (r[0].upper(), parse_date_key(r[1])))
    return rows

def write_csv(rows, out_path):
    headers = ["Nombre", "Fecha"]
    max_hours = max((len(r) - 2 for r in rows), default=0)
    headers += [f"Hora{i}" for i in range(1, max_hours + 1)]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            filled = r + [""] * (len(headers) - len(r))
            w.writerow(filled)

def main():
    if len(sys.argv) < 3:
        print("Uso: python export_mdb_inout.py database.mdb salida.csv")
        sys.exit(1)

    mdb_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    if not mdb_path.exists():
        print(f"No existe el archivo: {mdb_path}")
        sys.exit(1)

    # Intento 1: pyodbc
    try:
        users_df, inout_df = read_with_pyodbc(mdb_path)
    except Exception as e1:
        # Intento 2: mdbtools
        try:
            users_df, inout_df = read_with_mdbtools(mdb_path)
        except Exception as e2:
            print("No pude leer la base .mdb.")
            print(f"Intento pyodbc falló: {e1}")
            print(f"Intento mdbtools falló: {e2}")
            sys.exit(1)

    rows = build_rows(users_df, inout_df)
    write_csv(rows, out_path)
    print(f"Listo. CSV generado en: {out_path}")

if __name__ == "__main__":
    main()