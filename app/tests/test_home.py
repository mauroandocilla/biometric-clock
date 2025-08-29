from app import create_app


def test_home_ok():
    app = create_app()
    client = app.test_client()
    r = client.get("/")
    assert r.status_code == 200
    assert b"Hola" in r.data or b"Hola Flask" in r.data
