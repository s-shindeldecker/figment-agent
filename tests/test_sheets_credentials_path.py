from outputs.sheets_writer import _REPO_ROOT, _google_service_account_path


def test_google_service_account_path_default(monkeypatch):
    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    p = _google_service_account_path()
    assert p == _REPO_ROOT / "config" / "google_service_account.json"


def test_google_service_account_path_from_env_relative(monkeypatch):
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", "config/figment-e100-writer-aabe7d1b2311.json")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    p = _google_service_account_path()
    assert p == _REPO_ROOT / "config" / "figment-e100-writer-aabe7d1b2311.json"


def test_google_application_credentials_fallback(monkeypatch):
    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "config/other.json")
    p = _google_service_account_path()
    assert p == _REPO_ROOT / "config" / "other.json"
