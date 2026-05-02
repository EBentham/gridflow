from __future__ import annotations

import logging

from gridflow.utils.logging import setup_logging


def test_setup_logging_keeps_info_out_of_console(tmp_path, capsys) -> None:
    log_dir = tmp_path / "logs"
    setup_logging(log_dir, level="INFO", console_level="WARNING")

    logger = logging.getLogger("gridflow.test")
    logger.info("info detail")
    logger.warning("warning detail")

    for handler in logging.getLogger("gridflow").handlers:
        handler.flush()

    captured = capsys.readouterr()
    assert "info detail" not in captured.err
    assert "warning detail" in captured.err

    log_file = next(log_dir.glob("gridflow_*.log"))
    contents = log_file.read_text()
    assert "info detail" in contents
    assert "warning detail" in contents


def test_setup_logging_allows_more_verbose_console_than_file(tmp_path, capsys) -> None:
    log_dir = tmp_path / "logs"
    setup_logging(log_dir, level="INFO", console_level="DEBUG")

    logger = logging.getLogger("gridflow.test")
    logger.debug("debug detail")

    for handler in logging.getLogger("gridflow").handlers:
        handler.flush()

    captured = capsys.readouterr()
    assert "debug detail" in captured.err

    log_file = next(log_dir.glob("gridflow_*.log"))
    contents = log_file.read_text()
    assert "debug detail" not in contents
