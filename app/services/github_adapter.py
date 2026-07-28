import subprocess
import re
import os
import asyncio
import logging
from typing import Optional, Callable, Any

import aiohttp
from prometheus_client import Counter, Histogram

from app.config import settings

log = logging.getLogger("github_adapter")

# Prometheus metrics
PR_CREATE_ATTEMPTS = Counter("coda_pr_create_attempts_total", "PR creation attempts")
PR_CREATE_SUCCESSES = Counter("coda_pr_create_success_total", "PR creation successes")
PR_CREATE_FAILURES = Counter("coda_pr_create_failure_total", "PR creation failures")
GIT_PUSH_ATTEMPTS = Counter("coda_git_push_attempts_total", "Git push attempts")
GIT_PUSH_SUCCESSES = Counter("coda_git_push_success_total", "Git push successes")
GIT_PUSH_FAILURES = Counter("coda_git_push_failure_total", "Git push failures")
PR_CREATE_LATENCY = Histogram("coda_pr_create_latency_seconds", "PR create latency seconds")
GIT_PUSH_LATENCY = Histogram("coda_git_push_latency_seconds", "Git push latency seconds")


def _get_remote_url_sync(repo_dir: str) -> Optional[str]:
    try:
        proc = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=repo_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        return proc.stdout.strip()
    except Exception as e:
        log.warning("Failed to get remote URL for %s: %s", repo_dir, e)
        return None


def _parse_owner_repo(remote_url: str) -> Optional[tuple]:
    m = re.search(r'[:/]([^/]+)/([^/]+?)(?:\.git)?$', remote_url)
    if not m:
        return None
    return m.group(1), m.group(2)


async def _retry_async(fn: Callable[..., Any], *args, retries: int = None, base_delay: float = None, retry_exceptions: tuple = (Exception,), **kwargs):
    """
    Generic async retry with exponential backoff. Reads defaults from settings if not provided.
    """
    if retries is None:
        retries = getattr(settings, "GH_RETRIES", 3)
    if base_delay is None:
        base_delay = getattr(settings, "GH_BACKOFF_BASE", 1.0)
    attempt = 0
    while True:
        try:
            return await fn(*args, **kwargs)
        except retry_exceptions as e:
            attempt += 1
            if attempt > retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            log.debug("Retry attempt %d/%d after exception: %s. Sleeping %.1fs", attempt, retries, e, delay)
            await asyncio.sleep(delay)


async def _async_push_branch(repo_dir: str, head_branch: str) -> bool:
    """
    Attempt to push the branch to origin. Return True on success, False on error.
    Runs 'git rev-parse --verify' then 'git push origin <head_branch>' in a thread.
    Retries on failure with exponential backoff.
    """
    async def _do_push():
        def _push():
            try:
                subprocess.run(["git", "rev-parse", "--verify", head_branch], cwd=repo_dir, check=True,
                               capture_output=True, text=True)
            except subprocess.CalledProcessError:
                log.warning("Branch %s not found locally in %s; cannot push", head_branch, repo_dir)
                raise RuntimeError("branch-not-found")
            try:
                proc = subprocess.run(["git", "push", "origin", head_branch], cwd=repo_dir, check=True,
                                      capture_output=True, text=True)
                log.info("git push output: %s", proc.stdout)
                return True
            except subprocess.CalledProcessError as e:
                log.warning("git push failed: %s", e.stderr)
                raise

        # run blocking push in thread
        return await asyncio.to_thread(_push)

    GIT_PUSH_ATTEMPTS.inc()
    with GIT_PUSH_LATENCY.time():
        try:
            result = await _retry_async(_do_push)
            GIT_PUSH_SUCCESSES.inc()
            return bool(result)
        except Exception as e:
            GIT_PUSH_FAILURES.inc()
            log.warning("All git push attempts failed for %s: %s", head_branch, e)
            return False


async def _github_rest_create_pr_async(owner: str, repo: str, head: str, base: str, title: str, body: str, repo_dir: str) -> Optional[str]:
    token = getattr(settings, "GITHUB_TOKEN", None)
    if not token:
        log.warning("No GITHUB_TOKEN configured; cannot create PR via REST")
        return None

    # Attempt to push branch before creating PR to ensure remote head exists
    pushed = await _async_push_branch(repo_dir, head)
    if not pushed:
        log.warning("Failed to push branch %s before creating PR", head)
        # proceed anyway; maybe branch already exists remotely; we'll still attempt PR creation

    PR_CREATE_ATTEMPTS.inc()
    with PR_CREATE_LATENCY.time():
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
        payload = {"title": title, "head": head, "base": base, "body": body}
        headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=30) as resp:
                    text = await resp.text()
                    if resp.status in (200, 201):
                        data = await resp.json()
                        PR_CREATE_SUCCESSES.inc()
                        return data.get("html_url")
                    else:
                        PR_CREATE_FAILURES.inc()
                        log.warning("GitHub PR create failed: %s %s", resp.status, text)
                        return None
        except Exception as e:
            PR_CREATE_FAILURES.inc()
            log.warning("GitHub REST call exception: %s", e)
            return None


async def create_pr_using_agent_tools_if_possible(repo_name: str, head_branch: str, base_branch: str, title: str, body: str, repo_dir: str) -> Optional[str]:
    """
    Async function that tries multiple strategies, in order:
      1) Use agent's GithubTools.create_pull_request if available.
      2) Use agent's GitTools to push branch (if available) then use GithubTools.
      3) Fallback to REST API (aiohttp), pushing branch first (safe push).
    Returns the PR URL or None.
    """
    # Try to import the coder and inspect tools
    coder = None
    try:
        from coda.agents.coder import coder as coder_obj
        coder = coder_obj
    except Exception as e:
        log.debug("Couldn't import coder: %s", e)
        coder = None

    # Candidate tools collected from coder
    tools_candidates = []
    if coder is not None:
        for attr in ("tools", "toolbelt", "toolset", "tools_list"):
            if hasattr(coder, attr):
                try:
                    tv = getattr(coder, attr)
                    if isinstance(tv, (list, tuple, set)):
                        tools_candidates.extend(list(tv))
                    else:
                        tools_candidates.append(tv)
                except Exception:
                    pass
        # also look for attributes with github or git in their name
        for name in dir(coder):
            if "github" in name.lower() or "git" in name.lower():
                try:
                    v = getattr(coder, name)
                    tools_candidates.append(v)
                except Exception:
                    pass

    # 1) Try GithubTools on agent first
    for t in tools_candidates:
        if hasattr(t, "create_pull_request"):
            try:
                maybe = t.create_pull_request(repo=repo_name, title=title, body=body, head=head_branch, base=base_branch)
                # await if coroutine
                if hasattr(maybe, "__await__"):
                    result = await maybe
                else:
                    result = maybe
                # If result is dict-like, extract url
                if isinstance(result, dict):
                    return result.get("html_url") or result.get("url") or result.get("htmlUrl")
                if isinstance(result, str) and result.startswith("http"):
                    return result
            except Exception as e:
                log.warning("Agent GithubTools.create_pull_request raised: %s", e)
                # continue to next candidate

    # 2) If GitTools exists, try to push using it (duck-typing)
    for t in tools_candidates:
        if hasattr(t, "push") or hasattr(t, "git_push") or hasattr(t, "git"):
            try:
                pushed = False
                if hasattr(t, "push"):
                    maybe = t.push(repo=repo_name, branch=head_branch)
                    if hasattr(maybe, "__await__"):
                        await maybe
                    pushed = True
                elif hasattr(t, "git_push"):
                    maybe = t.git_push(repo=repo_name, branch=head_branch)
                    if hasattr(maybe, "__await__"):
                        await maybe
                    pushed = True
                elif hasattr(t, "git"):
                    git_wrapper = getattr(t, "git")
                    if callable(git_wrapper):
                        maybe = git_wrapper("push", "origin", head_branch)
                        if hasattr(maybe, "__await__"):
                            await maybe
                        pushed = True

                if pushed:
                    # try GithubTools again after push
                    for t2 in tools_candidates:
                        if hasattr(t2, "create_pull_request"):
                            try:
                                maybe2 = t2.create_pull_request(repo=repo_name, title=title, body=body, head=head_branch, base=base_branch)
                                if hasattr(maybe2, "__await__"):
                                    res2 = await maybe2
                                else:
                                    res2 = maybe2
                                if isinstance(res2, dict):
                                    return res2.get("html_url") or res2.get("url")
                                if isinstance(res2, str) and res2.startswith("http"):
                                    return res2
                            except Exception as e:
                                log.warning("Retry GithubTools.create_pull_request failed after push: %s", e)
            except Exception as e:
                log.debug("GitTools push attempt failed: %s", e)

    # 3) REST fallback: determine owner/repo from git remote and use aiohttp
    remote = await asyncio.to_thread(_get_remote_url_sync, repo_dir)
    if not remote:
        log.warning("Cannot determine remote url for repo_dir=%s", repo_dir)
        return None
    parsed = _parse_owner_repo(remote)
    if not parsed:
        log.warning("Cannot parse owner/repo from remote %s", remote)
        return None
    owner, repo = parsed
    return await _github_rest_create_pr_async(owner, repo, head_branch, base_branch, title, body, repo_dir)
