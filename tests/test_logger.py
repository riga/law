from __future__ import annotations

__all__ = ["TestLogger"]

import io
import logging
import sys

from law.logger import (
    LogFormatter,
    Logger,
    create_stream_handler,
    get_logger,
    get_tty_handlers,
    is_tty_handler,
    setup_logger,
)


class TestLogger:

    def make_logger(self, name: str) -> tuple[Logger, io.StringIO]:
        # create a logger with a handler writing to a string stream
        logger = get_logger(f"law_test_logger.{name}", skip_setup=True)
        stream = io.StringIO()
        handler = create_stream_handler(handler_kwargs={"stream": stream})
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        return logger, stream

    def test_get_logger(self) -> None:
        orig_cls = logging.getLoggerClass()
        logger = get_logger("law_test_logger.get")
        assert isinstance(logger, Logger)
        assert get_logger("law_test_logger.get") is logger
        # the global logger class is restored
        assert logging.getLoggerClass() is orig_cls

    def test_once_methods(self) -> None:
        logger, stream = self.make_logger("once")
        for _ in range(3):
            logger.info_once("id1", "message %s", 1)
            logger.warning_once("only a message")
        logger.info_once("id2", "message %s", 2)
        lines = stream.getvalue().strip().split("\n")
        assert len(lines) == 3
        assert lines[0].endswith("message 1")
        assert lines[1].endswith("only a message")
        assert lines[2].endswith("message 2")

        # ids are tracked per level
        logger.error_once("id1", "error")
        assert stream.getvalue().strip().split("\n")[-1].endswith("error")

    def test_formatter(self) -> None:
        logger, stream = self.make_logger("formatter")
        logger.warning("something %s", "happened")
        assert stream.getvalue() == "WARNING: law_test_logger.formatter - something happened\n"

    def test_formatter_custom(self) -> None:
        formatter = LogFormatter(
            log_template="[{level}] {msg}",
            format_level=lambda record: record.levelname.lower(),
        )
        record = logging.LogRecord("name", logging.INFO, "file", 1, "msg %d", (5,), None)
        assert formatter.format(record) == "[info] msg 5"

    def test_formatter_traceback(self) -> None:
        logger, stream = self.make_logger("traceback")
        try:
            raise ValueError("bad")
        except ValueError:
            logger.exception("failed")
        output = stream.getvalue()
        assert output.startswith("ERROR: law_test_logger.traceback - failed\n")
        assert "ValueError: bad" in output

    def test_setup_logger(self) -> None:
        logger = setup_logger("law_test_logger.setup", level="warning")
        assert logger.level == logging.WARNING
        assert not logger.propagate
        # set up only once
        assert setup_logger("law_test_logger.setup", level="debug").level == logging.WARNING

        logger = setup_logger("law_test_logger.setup_int", level=logging.ERROR, add_console_handler=True)
        assert logger.level == logging.ERROR
        assert len(logger.handlers) == 1

        # unknown level names fall back to the level of the law logger
        logger = setup_logger("law_test_logger.setup_unknown", level="not_a_level")
        assert logger.level == get_logger("law").level

    def test_setup_logger_force(self) -> None:
        name = "law_test_logger.force"
        setup_logger(name, level="info", add_console_handler=True)
        logger = setup_logger(name, level="debug", add_console_handler=False, force=True)
        assert logger.level == logging.DEBUG
        assert len(logger.handlers) == 1

        # forcing again adds another handler unless existing ones are cleared
        logger = setup_logger(name, level="debug", add_console_handler=True, force=True)
        assert len(logger.handlers) == 2
        logger = setup_logger(name, level="debug", add_console_handler=True, force=True, clear=True)
        assert len(logger.handlers) == 1

    def test_tty_handlers(self) -> None:
        handler = create_stream_handler(handler_kwargs={"stream": io.StringIO()})
        assert isinstance(handler.formatter, LogFormatter)
        assert not is_tty_handler(handler)

        console_handler = logging.Handler()
        console_handler.console = True  # type: ignore[attr-defined]
        assert is_tty_handler(console_handler)

        class TTY(io.StringIO):
            def isatty(self):
                return True

        tty_handler = logging.StreamHandler(TTY())
        assert is_tty_handler(tty_handler)

        logger = get_logger("law_test_logger.tty", skip_setup=True)
        logger.addHandler(handler)
        logger.addHandler(tty_handler)
        assert get_tty_handlers(logger) == [tty_handler]
        assert get_tty_handlers("law_test_logger.tty") == [tty_handler]

        assert create_stream_handler(formatter_cls=None).formatter is None  # type: ignore[arg-type]
        assert create_stream_handler().stream is sys.stderr  # type: ignore[attr-defined]
