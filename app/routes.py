# app/routes.py
from flask import (
    Blueprint,
    render_template,
    request,
    current_app,
    redirect,
    flash,
    send_file,
)
from werkzeug.utils import secure_filename
from pathlib import Path
from datetime import datetime
import gc

from .services.mdb_export_stream import export_mdb_to_csv_stream
from .services.mdb_export import export_mdb_to_csv  # fallback

bp = Blueprint("main", __name__)
ALLOWED_EXTENSIONS = {".mdb"}  # añade ".accdb" si quieres


def allowed_file(filename: str) -> bool:
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


@bp.route("/")
def home():
    return render_template("index.html", title="Home")


@bp.route("/upload", methods=["GET", "POST"])
def upload():
    now = datetime.now()
    years = list(
        range(now.year, now.year - 10, -1)
    )  # últimos 10 años incluyendo actual
    months_all = list(range(1, 13))
    months_current = list(range(1, now.month + 1))

    if request.method == "GET":
        return render_template(
            "upload.html",
            title="Subir MDB",
            years=years,
            current_year=now.year,
            current_month=now.month,
            months_initial=months_current,  # por defecto limitar al mes actual si está seleccionado el año actual
        )

    # --- POST ---
    file = request.files.get("file")
    if not file or file.filename == "":
        flash("Selecciona un archivo .mdb", "error")
        return redirect(request.url)
    if not allowed_file(file.filename):
        flash("Solo se permiten archivos .mdb", "error")
        return redirect(request.url)

    # --- Procesar ---
    flash("Procesando archivo...", "info")

    # Año/Mes seleccionados
    try:
        sel_year = int(request.form.get("year", now.year))
        sel_month = int(request.form.get("month", now.month))
        if sel_year == now.year and sel_month > now.month:
            flash("No puedes seleccionar un mes futuro del año actual.", "error")
            return redirect(request.url)
        if sel_month < 1 or sel_month > 12:
            raise ValueError
    except Exception:
        flash("Selecciona un año y mes válidos.", "error")
        return redirect(request.url)

    upload_dir = Path(current_app.config["UPLOAD_FOLDER"])
    upload_dir.mkdir(parents=True, exist_ok=True)

    safe_name = secure_filename(file.filename)
    mdb_path = upload_dir / safe_name
    file.save(mdb_path)

    flash(f"Archivo subido: {mdb_path}", "success")

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    csv_name = f"{mdb_path.stem}_inout_{sel_year:04d}-{sel_month:02d}_{ts}.csv"
    csv_path = upload_dir / csv_name

    try:
        current_app.logger.info(
            "Procesando MDB (stream): %s (year=%s, month=%s)",
            mdb_path,
            sel_year,
            sel_month,
        )
        flash(
            f"Procesando MDB (stream): {mdb_path} (year={sel_year}, month={sel_month})",
            "info",
        )
        export_mdb_to_csv_stream(mdb_path, csv_path, year=sel_year, month=sel_month)
        current_app.logger.info("CSV generado (stream): %s", csv_path)
        flash(f"CSV generado (stream): {csv_path}", "success")
    except Exception:
        current_app.logger.exception(
            "Fallo stream, intentando fallback con pandas/pyodbc"
        )
        try:
            # fallback también filtrará por año/mes (ver siguiente sección)
            export_mdb_to_csv(mdb_path, csv_path, year=sel_year, month=sel_month)
            flash(
                f"Procesando MDB (fallback pandas): {mdb_path} (year={sel_year}, month={sel_month})",
                "info",
            )
            current_app.logger.info("CSV generado (fallback pandas): %s", csv_path)
            flash(f"CSV generado (fallback pandas): {csv_path}", "success")
        except Exception as e_fb:
            current_app.logger.exception("Error procesando MDB")
            flash(f"Error procesando MDB: {e_fb}", "error")
            return redirect(request.url)
    finally:
        gc.collect()

    return send_file(
        csv_path,
        as_attachment=True,
        download_name=csv_name,
        mimetype="text/csv",
        max_age=0,
        conditional=True,
    )
