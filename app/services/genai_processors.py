from typing import Any, Dict, List
import importlib
import logging
from app.services.pubsub import publish_task_message
from app.config import settings

log = logging.getLogger("genai_processors")


class Processor:
    async def process(self, task_uuid: str, repo_dir: str, task_name: str, result: Any, prompt: str) -> Dict:
        return {"result": result}


class ExtractPRProcessor(Processor):
    import re
    PR_RE = re.compile(r"https?://[^\s)"'\]]+/pull/\d+")

    async def process(self, task_uuid: str, repo_dir: str, task_name: str, result: Any, prompt: str) -> Dict:
        await publish_task_message(task_uuid, "[processor] ExtractPRProcessor: scanning for PR URLs...")
        pr_url = None
        if isinstance(result, dict):
            for k in ("pr_url", "pr", "pull_request", "pull_request_url", "pr_link", "prUrl"):
                if k in result and isinstance(result[k], str) and result[k].startswith("http"):
                    pr_url = result[k]
                    break
            if not pr_url:
                for v in result.values():
                    if isinstance(v, str):
                        m = self.PR_RE.search(v)
                        if m:
                            pr_url = m.group(0)
                            break
        if not pr_url and isinstance(result, str):
            m = self.PR_RE.search(result)
            if m:
                pr_url = m.group(0)
        if pr_url:
            await publish_task_message(task_uuid, f"[processor] Found PR URL: {pr_url}")
            return {"result": result, "pr_url": pr_url}
        return {"result": result}


class RunTestsProcessor(Processor):
    async def process(self, task_uuid: str, repo_dir: str, task_name: str, result: Any, prompt: str) -> Dict:
        await publish_task_message(task_uuid, "[processor] RunTestsProcessor: checking for tests...")

        def _run_pytest():
            import subprocess
            try:
                proc = subprocess.run(["pytest", "-q"], cwd=repo_dir, check=False, capture_output=True, text=True, timeout=300)
                return proc.returncode, proc.stdout + "\n" + proc.stderr
            except Exception as e:
                return 2, f"pytest execution error: {e}"

        rc, output = await __import__("asyncio").to_thread(_run_pytest)
        if rc == 0:
            await publish_task_message(task_uuid, "[processor] Tests passed")
        else:
            await publish_task_message(task_uuid, f"[processor] Tests returned code {rc}\n{output[:1000]}")
        return {"result": result, "tests": {"rc": rc, "output_snippet": output[:200]}}


_DEFAULT_PROCESSOR_CLASSES = [ExtractPRProcessor, RunTestsProcessor]


def _import_class(path: str):
    mod_name, cls_name = path.rsplit(".", 1)
    mod = importlib.import_module(mod_name)
    return getattr(mod, cls_name)


def load_processors() -> List[Processor]:
    ps: List[Processor] = []
    if getattr(settings, "PROCESSORS", None):
        for item in settings.PROCESSORS.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                cls = _import_class(item)
                ps.append(cls())
            except Exception as e:
                log.warning("Failed to load processor %s: %s", item, e)
    else:
        for cls in _DEFAULT_PROCESSOR_CLASSES:
            ps.append(cls())
    return ps
