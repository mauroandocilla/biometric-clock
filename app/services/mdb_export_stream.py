# app/services/mdb_export_stream.py
import csv
import subprocess
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Tuple

try:
    from dateutil import parser as dtparser  # viene con pandas, pero lo usamos directo
except Exception:
    dtparser = None  # fallback simple más abajo


def mdb_export_to_file(mdb_path: Path, table: str, out_csv: Path) -> None:
    # Exporta tabla -> CSV con comillas y separadores explícitos
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
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    out_csv.write_text(proc.stdout, encoding="utf-8")


def parse_dt(s: str) -> datetime:
    s = s.strip().replace("T", " ")
    if dtparser:
        return dtparser.parse(s, dayfirst=False)  # intent month/day
    # Fallback simple
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    # último intento: d/m/y
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"No puedo parsear fecha: {s!r}")


def fmt_mdyy(d: date) -> str:
    return f"{int(d.month)}/{int(d.day)}/{str(d.year)[-2:]}"


def export_mdb_to_csv_stream(
    mdb_path: Path, out_path: Path, *, year: int | None = None, month: int | None = None
) -> Path:
    tmp_dir = out_path.parent
    users_csv = tmp_dir / "_users.csv"
    inout_csv = tmp_dir / "_inout.csv"

    # 1) Exporta tablas a CSV (en disco, no en memoria)
    mdb_export_to_file(mdb_path, "USERINFO", users_csv)
    mdb_export_to_file(mdb_path, "CHECKINOUT", inout_csv)

    # 2) Carga USERINFO -> mapa USERID->Name (streaming)
    user_map: Dict[str, str] = {}
    with users_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        headers = next(r, None)
        # normaliza nombres comunes
        idx_uid = None
        idx_name = None
        for i, h in enumerate(headers or []):
            hh = h.strip().lower()
            if hh == "userid":
                idx_uid = i
            if hh in ("name", "nombre"):
                idx_name = i
        if idx_uid is None or idx_name is None:
            raise RuntimeError(
                f"USERINFO: no encuentro columnas USERID/Name, headers={headers}"
            )

        for row in r:
            if not row:
                continue
            uid = str(row[idx_uid]).strip()
            name = str(row[idx_name]).strip()
            if uid:
                user_map[uid] = name

    # 3) Recorre CHECKINOUT y agrupa (uid, fecha_mdyy) -> [HH:MM,...]
    groups: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    with inout_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        headers = next(r, None)
        idx_uid = None
        idx_ct = None
        for i, h in enumerate(headers or []):
            hh = h.strip().lower()
            if hh == "userid":
                idx_uid = i
            if hh in ("checktime", "check_time", "check time"):
                idx_ct = i
        if idx_uid is None or idx_ct is None:
            raise RuntimeError(
                f"CHECKINOUT: no encuentro columnas USERID/CHECKTIME, headers={headers}"
            )

        for row in r:
            if not row:
                continue
            uid = str(row[idx_uid]).strip()
            if uid not in user_map:
                continue
            dt = parse_dt(str(row[idx_ct]))
            if year and dt.year != year:
                continue
            if month and dt.month != month:
                continue
            key = (user_map[uid], fmt_mdyy(dt.date()))
            groups[key].append(f"{dt.hour}:{dt.minute:02d}")

    # 4) Ordena horas y arma filas
    def to_min(t: str) -> int:
        h, m = t.split(":")
        return int(h) * 60 + int(m)

    rows = []
    for (name, fdate), times in groups.items():
        times.sort(key=to_min)
        rows.append([name, fdate, *times])

    def row_key(r):
        name, d = r[0], r[1]
        m, d_, y = d.split("/")
        y = int("20" + y) if len(y) == 2 else int(y)
        return (name.upper(), y, int(m), int(d_))

    rows.sort(key=row_key)

    # 5) Escribe CSV de salida
    max_hours = max((len(r) - 2 for r in rows), default=0)
    headers_out = ["Nombre", "Fecha"] + [f"Hora{i}" for i in range(1, max_hours + 1)]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers_out)
        for r in rows:
            w.writerow(r + [""] * (len(headers_out) - len(r)))
    return out_path
