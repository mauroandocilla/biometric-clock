# app/__init__.py
from app.config import get_config
from flask import Flask
from dotenv import load_dotenv


def create_app():
    load_dotenv()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_object(get_config())

    # Blueprints
    from .routes import bp as main_bp

    app.register_blueprint(main_bp)

    # ---- LOGGING: integra con gunicorn ----
    import logging, sys

    gunicorn_error = logging.getLogger("gunicorn.error")
    if gunicorn_error.handlers:
        app.logger.handlers = gunicorn_error.handlers
        app.logger.setLevel(gunicorn_error.level)
    else:
        # fallback (por si corres sin gunicorn)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        app.logger.addHandler(handler)
        app.logger.setLevel(logging.INFO)

    app.logger.info("Flask app inicializada y logger conectado a gunicorn")
    return app
