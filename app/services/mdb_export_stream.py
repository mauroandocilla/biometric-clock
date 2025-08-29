# app/services/mdb_export_stream.py
import csv
import subprocess
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Tuple
from openpyxl import Workbook

try:
    from dateutil import parser as dtparser
except Exception:
    dtparser = None


def mdb_export_to_file(mdb_path: Path, table: str, out_csv: Path) -> None:
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
        return dtparser.parse(s, dayfirst=False)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"No puedo parsear fecha: {s!r}")


# ✅ Formato ISO 8601 YYYY-MM-DD
def fmt_iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def export_mdb_to_csv_stream(
    mdb_path: Path, out_path: Path, *, year: int | None = None, month: int | None = None
) -> Path:
    tmp_dir = out_path.parent
    users_csv = tmp_dir / "_users.csv"
    inout_csv = tmp_dir / "_inout.csv"

    # 1) Exporta tablas
    mdb_export_to_file(mdb_path, "USERINFO", users_csv)
    mdb_export_to_file(mdb_path, "CHECKINOUT", inout_csv)

    # 2) USERINFO -> mapa con UID -> (Name, Badge, SSN)
    user_map: Dict[str, Tuple[str, str, str]] = {}
    with users_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        headers = next(r, None)
        idx_uid = idx_name = idx_badge = idx_ssn = None
        for i, h in enumerate(headers or []):
            hh = h.strip().lower()
            if hh == "userid":
                idx_uid = i
            if hh == "name":
                idx_name = i
            # 👇 corrige el uso de 'in' con tuplas (antes "in ('badgenumber')" evaluaba carácter por carácter)
            if hh in ("badgenumber"):
                idx_badge = i
            if hh in ("ssn"):
                idx_ssn = i
        if idx_uid is None or idx_name is None:
            raise RuntimeError(f"USERINFO: falta USERID/Name, headers={headers}")

        for row in r:
            if not row:
                continue
            uid = str(row[idx_uid]).strip()
            if not uid:
                continue
            name = str(row[idx_name]).strip() if idx_name is not None else ""
            badge = str(row[idx_badge]).strip() if idx_badge is not None else ""
            ssn = str(row[idx_ssn]).strip() if idx_ssn is not None else ""
            user_map[uid] = (name, badge, ssn)

    # 3) CHECKINOUT -> agrupar
    groups: Dict[Tuple[str, str, str, str, str], List[str]] = defaultdict(list)
    with inout_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        headers = next(r, None)
        idx_uid = idx_ct = None
        for i, h in enumerate(headers or []):
            hh = h.strip().lower()
            if hh == "userid":
                idx_uid = i
            if hh in ("checktime", "check_time", "check time"):
                idx_ct = i
        if idx_uid is None or idx_ct is None:
            raise RuntimeError(f"CHECKINOUT: falta USERID/CHECKTIME, headers={headers}")

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
            name, badge, ssn = user_map[uid]
            # ✅ clave con fecha ISO
            key = (name, badge, ssn, uid, fmt_iso(dt.date()))
            groups[key].append(f"{dt.hour}:{dt.minute:02d}")

    # 4) Armar filas
    def to_min(t: str) -> int:
        h, m = t.split(":")
        return int(h) * 60 + int(m)

    rows = []
    for (name, badge, ssn, uid, fdate_iso), times in groups.items():
        times.sort(key=to_min)
        # ✅ guardamos fecha ya en ISO
        rows.append([badge, ssn, name, fdate_iso, *times])

    # Ordenar por Name y luego Fecha (ISO ordena bien lexicográficamente, pero hacemos seguro)
    def row_key(r):
        name, d = r[2], r[3]  # [badge, ssn, name, date_iso, ...]
        y, m, d_ = d.split("-")
        return (name.upper(), int(y), int(m), int(d_))

    rows.sort(key=row_key)

    # 5) Escribir Excel con nuevas columnas
    max_hours = max((len(r) - 4 for r in rows), default=0)
    headers_out = [
        "Codigo (Badgenumber)",
        "Cedula (SSN)",
        "Nombre",
        "Fecha (YYYY-MM-DD)",
    ] + [f"Hora{i}" for i in range(1, max_hours + 1)]

    wb = Workbook()
    ws = wb.active
    ws.title = "Asistencias"

    # escribir encabezados
    ws.append(headers_out)

    # escribir filas
    for r in rows:
        ws.append(r + [""] * (len(headers_out) - len(r)))

    # forzar extensión .xlsx
    if out_path.suffix.lower() != ".xlsx":
        out_path = out_path.with_suffix(".xlsx")

    wb.save(out_path)
    return out_path
