# app/routes.py
from flask import (
    Blueprint,
    render_template,
    request,
    current_app,
    jsonify,
    Response,
    stream_with_context,
    send_file,
)
from werkzeug.utils import secure_filename
from pathlib import Path
from datetime import datetime
from uuid import uuid4
from queue import Queue, Empty
from threading import Thread
import gc

from .services.mdb_export_stream import export_mdb_to_csv_stream

bp = Blueprint("main", __name__)
ALLOWED_EXTENSIONS = {".mdb"}


def allowed_file(filename: str) -> bool:
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


@bp.route("/")
def home():
    return render_template("index.html", title="Home")


# --- Infra simple en memoria para SSE ---
EVENT_QUEUES = {}  # task_id -> Queue[str]
TASK_OUTPUTS = {}  # task_id -> Path
TASK_ERRORS = {}  # task_id -> str


def _emit(task_id: str, msg: str):
    q = EVENT_QUEUES.get(task_id)
    if q:
        q.put(msg)


@bp.get("/upload", endpoint="upload")
def upload_get():
    now = datetime.now()
    years = list(range(now.year, now.year - 10, -1))
    months_current = list(range(1, now.month + 1))
    return render_template(
        "upload.html",
        title="Subir MDB",
        years=years,
        current_year=now.year,
        current_month=now.month,
        months_initial=months_current,
    )


@bp.post("/upload")
def upload_post():
    now = datetime.now()

    file = request.files.get("file")
    if not file or file.filename == "" or not allowed_file(file.filename):
        return (
            jsonify({"ok": False, "error": "Debes subir un archivo .mdb válido"}),
            400,
        )

    try:
        sel_year = int(request.form.get("year", now.year))
        sel_month = int(request.form.get("month", now.month))
        if sel_year == now.year and sel_month > now.month:
            return jsonify({"ok": False, "error": "Mes futuro no permitido"}), 400
        if not (1 <= sel_month <= 12):
            raise ValueError
    except Exception:
        return jsonify({"ok": False, "error": "Año/Mes inválidos"}), 400

    upload_dir = Path(current_app.config.get("UPLOAD_FOLDER", "/tmp/uploads"))
    upload_dir.mkdir(parents=True, exist_ok=True)

    safe_name = secure_filename(file.filename)
    mdb_path = upload_dir / safe_name
    file.save(mdb_path)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    xlsx_name = f"{mdb_path.stem}_inout_{sel_year:04d}-{sel_month:02d}_{ts}.xlsx"
    xlsx_path = upload_dir / xlsx_name

    task_id = str(uuid4())
    q = Queue()
    EVENT_QUEUES[task_id] = q

    def worker():
        try:
            _emit(task_id, f"Archivo recibido: {safe_name}")
            _emit(task_id, f"Procesando (año={sel_year}, mes={sel_month})…")
            export_mdb_to_csv_stream(
                mdb_path, xlsx_path, year=sel_year, month=sel_month
            )
            TASK_OUTPUTS[task_id] = xlsx_path
            _emit(task_id, "DONE")
        except Exception as e:
            TASK_ERRORS[task_id] = str(e)
            _emit(task_id, f"ERROR: {e}")
        finally:
            gc.collect()

    Thread(target=worker, daemon=True).start()

    return (
        jsonify(
            {
                "ok": True,
                "task_id": task_id,
                "events_url": f"/events/{task_id}",
                "download_url": f"/download/{task_id}",
            }
        ),
        202,
    )


@bp.get("/events/<task_id>")
def events(task_id: str):
    q = EVENT_QUEUES.get(task_id)
    if q is None:
        return Response(
            "event: error\ndata: Tarea no encontrada\n\n", mimetype="text/event-stream"
        )

    @stream_with_context
    def gen():
        yield "event: ping\ndata: ready\n\n"
        while True:
            try:
                msg = q.get(timeout=25)
                if msg == "DONE":
                    yield "event: done\ndata: done\n\n"
                    break
                elif msg.startswith("ERROR:"):
                    yield f"event: error\ndata: {msg}\n\n"
                    break
                else:
                    yield f"event: message\ndata: {msg}\n\n"
            except Empty:
                yield "event: ping\ndata: keep-alive\n\n"

    return Response(gen(), mimetype="text/event-stream")


@bp.get("/download/<task_id>")
def download(task_id: str):
    if task_id in TASK_ERRORS:
        return jsonify({"ok": False, "error": TASK_ERRORS[task_id]}), 400
    path = TASK_OUTPUTS.get(task_id)
    if not path or not Path(path).exists():
        return jsonify({"ok": False, "error": "Archivo no disponible"}), 404
    return send_file(
        path,
        as_attachment=True,
        download_name=Path(path).name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
