from regional_metrics.settings import load_arcgis_settings, load_project_config, publish_enabled


def test_load_project_config():
    config = load_project_config()
    assert "jobs_by_auto" in config.metrics
    assert config.publish.boundaries.join_field == "GeoName"
    assert "county_fips_regions" in config.geography_groups


def test_load_arcgis_settings_without_profile(monkeypatch):
    monkeypatch.setenv("ARCGIS_URL", "https://example.maps.arcgis.com")
    monkeypatch.delenv("ARCGIS_PROFILE", raising=False)
    monkeypatch.setenv("ARCGIS_USERNAME", "user1")
    monkeypatch.setenv("ARCGIS_PASSWORD", "secret1")

    settings = load_arcgis_settings()

    assert settings.profile is None
    assert settings.username == "user1"
    assert settings.password == "secret1"


def test_publish_enabled_flag(monkeypatch):
    monkeypatch.setenv("ARCGIS_PUBLISH_ENABLED", "false")
    assert publish_enabled() is False

    monkeypatch.setenv("ARCGIS_PUBLISH_ENABLED", "true")
    assert publish_enabled() is True
