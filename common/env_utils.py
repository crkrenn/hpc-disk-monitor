import os
import getpass
import socket
from pathlib import Path
from dotenv import dotenv_values


def preprocess_env(path=".env", use_shell_env=False):
    """
    Load .env file and replace {{whoami}}, {{HOME}}, {{hostname}}
    if not set in shell.
    """
    username = getpass.getuser()
    home_dir = str(Path.home())
    hostname = socket.gethostname()

    # Load .env only if use_shell_env is False or to fill in missing vars
    raw_env = dotenv_values(path)

    for key, value in raw_env.items():
        if use_shell_env and key in os.environ:
            continue  # keep existing shell value
        if value is not None:
            processed = (
                value.replace("{{whoami}}", username)
                .replace("{{HOME}}", home_dir)
                .replace("{{hostname}}", hostname)
            )
            os.environ[key] = processed
