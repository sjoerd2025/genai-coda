import subprocess
import shlex
import logging
from typing import Tuple, Optional

log = logging.getLogger("sandbox_runner")


def _make_docker_cmd(repo_dir: str, workdir: str = "/workspace", image: str = "python:3.11-slim", cmd: str = "pytest -q", timeout: int = 300) -> str:
    # Mount repo_dir into container at workdir and run the command
    # Use --network=none to isolate network; remove if network needed
    safe_cmd = cmd.replace("\"", "\\\"")
    docker_cmd = (
        f"docker run --rm -v {repo_dir}:{workdir} -w {workdir} --network=none {image} bash -lc \"{safe_cmd}\""
    )
    return docker_cmd


def run_tests_in_sandbox(repo_dir: str, image: str = "python:3.11-slim", cmd: str = "pytest -q", timeout: int = 300) -> Tuple[int, str]:
    """
    Run the given command inside a disposable Docker container mounting repo_dir.
    Returns (rc, output).

    Note: This function requires the Docker daemon to be available and the user
    running the process to have permission to run docker. We deliberately use
    --network=none to limit network access; remove if network access is required.
    """
    docker_cmd = _make_docker_cmd(repo_dir, image=image, cmd=cmd, timeout=timeout)
    try:
        # Use shell execution to avoid complex arg quoting in Python on various platforms
        proc = subprocess.run(docker_cmd, shell=True, check=False, capture_output=True, text=True, timeout=timeout)
        output = (proc.stdout or "") + "\n" + (proc.stderr or "")
        return proc.returncode, output
    except subprocess.TimeoutExpired as e:
        log.warning("Sandbox test run timed out: %s", e)
        return 2, f"sandbox timeout: {e}"
    except Exception as e:
        log.exception("Sandbox runner failed: %s", e)
        return 2, f"sandbox exception: {e}"
