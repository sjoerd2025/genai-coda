"""
ADK2Processor

A pluggable GenAI processor that calls into an ADK2-style SDK to run repo
analysis or model-driven checks after the agent returns. This file is defensive:
it detects common ADK2 client shapes, supports async and sync methods, and
publishes progress via the existing publish_task_message channel.

Enable by adding its import path to the PROCESSORS env var, for example:
PROCESSORS=app.processors.adk2_processor.ADK2Processor

Environment:
- ADK2_API_KEY  (optional) -- if the ADK2 client requires a key
"""
import os
import asyncio
import logging
from typing import Any, Dict, Optional

from app.services.pubsub import publish_task_message
from app.services.genai_processors import Processor

log = logging.getLogger("adk2_processor")


class ADK2Processor(Processor):
    """
    Attempts to use an installed `adk2` package to perform repository analysis
    or model-based checks. Behavior:

    - Tries to import `adk2`.
    - Instantiates a client using common names (Client, ADK2Client).
    - Looks for useful methods in order:
        - analyze_repo(repo_path)
        - analyze(path=repo_path)
        - run_model(prompt=..., repo_path=...)
        - check_repository(repo_path)
    - If the chosen method returns a dict containing a PR URL (keys like pr_url,
      pull_request, pr, etc.), we attach that as 'pr_url' in the returned dict.
    - All calls are run in an async-friendly way (await if coroutine; otherwise run in thread).
    - Publishes progress messages to the task channel.
    """

    async def process(self, task_uuid: str, repo_dir: str, task_name: str, result: Any, prompt: str) -> Dict:
        await publish_task_message(task_uuid, "[adk2] ADK2Processor: starting ADK2 analysis...")

        try:
            import adk2  # type: ignore
        except Exception as e:
            await publish_task_message(task_uuid, f"[adk2] ADK2 SDK not available: {e}")
            return {"result": result}

        # Instantiate client in a robust way
        api_key = os.getenv("ADK2_API_KEY", None)
        client = None
        for cls_name in ("Client", "ADK2Client", "ADKClient"):
            if hasattr(adk2, cls_name):
                try:
                    ctor = getattr(adk2, cls_name)
                    # try to instantiate with api_key if supported
                    try:
                        client = ctor(api_key=api_key) if api_key is not None else ctor()
                    except TypeError:
                        # fallback to no-arg constructor
                        client = ctor()
                    break
                except Exception as e:
                    log.debug("Failed to instantiate %s: %s", cls_name, e)
                    client = None

        # If no client class found, perhaps adk2 exposes helper functions directly
        if client is None and hasattr(adk2, "from_env"):
            try:
                client = adk2.from_env()
            except Exception:
                client = None

        if client is None:
            await publish_task_message(task_uuid, "[adk2] Could not instantiate ADK2 client; skipping processor.")
            return {"result": result}

        # Candidate method names to call on the client
        candidate_methods = [
            ("analyze_repo", ("repo_path", "path")),  # analyze_repo(repo_path) or analyze_repo(path=...)
            ("analyze", ("repo_path", "path")),
            ("check_repository", ("repo_path", "path")),
            ("run_model", ("prompt", "repo_path", "path")),
            ("scan", ("path",)),
        ]

        chosen_method = None
        chosen_call = None
        method_result = None

        for mname, params in candidate_methods:
            if hasattr(client, mname):
                chosen_method = getattr(client, mname)
                # prepare kwargs we will try
                kwargs = {}
                if "repo_path" in params:
                    kwargs["repo_path"] = repo_dir
                elif "path" in params:
                    kwargs["path"] = repo_dir
                if "prompt" in params:
                    kwargs["prompt"] = prompt
                chosen_call = (chosen_method, kwargs)
                break

        if chosen_call is None:
            # fallback: try a top-level function on adk2 module
            for func_name in ("analyze_repo", "analyze", "check_repository", "run_model"):
                if hasattr(adk2, func_name):
                    chosen_method = getattr(adk2, func_name)
                    kwargs = {}
                    try:
                        varnames = chosen_method.__code__.co_varnames
                    except Exception:
                        varnames = ()
                    if "repo_path" in varnames:
                        kwargs["repo_path"] = repo_dir
                    else:
                        kwargs = {"path": repo_dir}
                    chosen_call = (chosen_method, kwargs)
                    break

        if chosen_call is None:
            await publish_task_message(task_uuid, "[adk2] No suitable ADK2 method found; skipping.")
            return {"result": result}

        func, kwargs = chosen_call
        await publish_task_message(task_uuid, f"[adk2] Calling ADK2.{func.__name__} with args={list(kwargs.keys())}")

        try:
            # If coroutine, await directly
            if asyncio.iscoroutinefunction(func):
                method_result = await func(**kwargs)
            else:
                maybe = func(**kwargs)
                if asyncio.iscoroutine(maybe):
                    method_result = await maybe
                else:
                    # run blocking call in thread
                    method_result = await asyncio.to_thread(lambda: maybe)
        except Exception as e:
            await publish_task_message(task_uuid, f"[adk2] ADK2 call failed: {e}")
            log.exception("ADK2 call failed")
            return {"result": result}

        # Publish a brief summary
        try:
            summary = str(method_result)
            short = summary[:1000]
            await publish_task_message(task_uuid, f"[adk2] ADK2 returned: {short}")
        except Exception:
            pass

        # If result contains pr url, adopt it
        pr_url = None
        if isinstance(method_result, dict):
            for k in ("pr_url", "pr", "pull_request", "pull_request_url", "pr_link", "prUrl"):
                if k in method_result and isinstance(method_result[k], str) and method_result[k].startswith("http"):
                    pr_url = method_result[k]
                    break
            # also scan textual values
            if not pr_url:
                for v in method_result.values():
                    if isinstance(v, str) and "http" in v:
                        import re
                        m = re.search(r"https?://[^\s)'"]+", v)
                        if m:
                            pr_url = m.group(0)
                            break
        elif isinstance(method_result, str):
            import re
            m = re.search(r"https?://[^\s)'"]+/pull/\d+", method_result)
            if m:
                pr_url = m.group(0)

        out = {"result": result}
        if pr_url:
            out["pr_url"] = pr_url
            await publish_task_message(task_uuid, f"[adk2] Extracted PR URL: {pr_url}")

        # Allow ADK2 to supply structured signals: if method_result is dict, attach under 'adk2'
        out.setdefault("adk2", {})
        out["adk2"]["raw"] = method_result

        return out
