"""ReportingConfig parsing + defaults."""

from pathlib import Path

from stdf_platform.config import Config, ReportingConfig


def test_reporting_defaults():
    cfg = Config()
    assert isinstance(cfg.reporting, ReportingConfig)
    assert cfg.reporting.histogram_top_n == 20
    assert cfg.reporting.always_include_tests == {}


def test_reporting_parsed_from_yaml(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "reporting:\n"
        "  histogram_top_n: 5\n"
        "  always_include_tests:\n"
        "    PRODUCT_A: [1234, 5678]\n",
        encoding="utf-8",
    )
    cfg = Config.load(cfg_file)
    assert cfg.reporting.histogram_top_n == 5
    assert cfg.reporting.always_include_tests == {"PRODUCT_A": [1234, 5678]}


def test_reports_dir_under_data():
    cfg = Config()
    assert cfg.storage.reports_dir == cfg.storage.data_dir / "reports"
