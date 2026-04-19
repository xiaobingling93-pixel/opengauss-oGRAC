import os
from pathlib import Path


def _default_log_dir():
    if os.environ.get("DEPLOY_LOG_DIR"):
        return os.environ["DEPLOY_LOG_DIR"]
    if os.environ.get("OGRAC_HOME"):
        return str(Path(os.environ["OGRAC_HOME"]) / "log" / "deploy")
    try:
        return str(Path(__file__).resolve().parents[4] / "log" / "deploy")
    except IndexError:
        return str(Path.cwd() / "log" / "deploy")


CONSOLE_CONF = {
    "log": {
        "use_syslog": False,
        "debug": False,
        "log_dir": _default_log_dir(),
        "log_file_max_size": 1048576,
        "log_file_backup_count": 5,
        "log_date_format": "%Y-%m-%d %H:%M:%S",
        "logging_default_format_string": "%(asctime)s console %(levelname)s [pid:%(process)d] [%(threadName)s] "
                                         "[tid:%(thread)d] [%(filename)s:%(lineno)d %(funcName)s] %(message)s",
        "logging_context_format_string": "%(asctime)s console %(levelname)s [pid:%(process)d] [%(threadName)s] "
                                         "[tid:%(thread)d] [%(filename)s:%(lineno)d %(funcName)s] %(message)s"
    }
}
