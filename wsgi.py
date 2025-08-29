# wsgi.py
from app import create_app
app = create_app()

@app.get("/ping")
def ping():
    return "pong", 200