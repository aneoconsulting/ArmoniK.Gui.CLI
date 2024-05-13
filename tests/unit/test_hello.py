from armonik_cli.admin import hello


def test_hello():
    assert hello() == "Hello, World!"
