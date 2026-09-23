import os
from pathlib import Path


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
    # Ejemplo de config extra:
    # SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///app.db")
    # SQLALCHEMY_TRACK_MODIFICATIONS = False
    UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", str(Path(__file__).resolve().parents[1] / "uploads"))
    MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", 10 * 1024 * 1024))
    # Pausa la app: bloquea /upload, /events y /download y muestra la
    # pantalla de "Server down" en la home. Poner en "false" para reactivar.
    MAINTENANCE_MODE = os.getenv("MAINTENANCE_MODE", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


class DevConfig(Config):
    DEBUG = True


class ProdConfig(Config):
    DEBUG = False


def get_config():
    env = os.getenv("FLASK_ENV", "development").lower()
    return DevConfig if env == "development" else ProdConfig
