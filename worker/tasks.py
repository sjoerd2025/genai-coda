from worker.celery_app import celery_app
from app.services.agent_service import run_task_with_agent
from app.services.pubsub import publish_task_message
from sqlmodel import Session, select
from app.models import Task
import traceback
import asyncio

@celery_app.task(name="worker.tasks.run_task")
def run_task(task_uuid: str, repo: str, task_name: str, prompt: str):
    try:
        # mark task running
        with Session(celery_app.db_engine) as session:
            t = session.exec(select(Task).where(Task.task_uuid == task_uuid)).first()
            if t:
                t.status = "running"
                session.add(t)
                session.commit()

        # run agent (wrap async runner)
        result_struct = asyncio.run(run_task_with_agent(task_uuid, prompt, repo, task_name))

        pr_url = None
        if isinstance(result_struct, dict):
            pr_url = result_struct.get("pr_url")
        # persist result
        with Session(celery_app.db_engine) as session:
            t = session.exec(select(Task).where(Task.task_uuid == task_uuid)).first()
            if t:
                t.status = "finished"
                if pr_url:
                    t.pr_url = pr_url
                session.add(t)
                session.commit()

        if pr_url:
            publish_task_message(task_uuid, f"[info] PR available: {pr_url}")
        publish_task_message(task_uuid, "[info] task finished")
        return {"result": str(result_struct)}
    except Exception as e:
        tb = traceback.format_exc()
        publish_task_message(task_uuid, f"[error] task failed: {e}\n{tb}")
        with Session(celery_app.db_engine) as session:
            t = session.exec(select(Task).where(Task.task_uuid == task_uuid)).first()
            if t:
                t.status = "failed"
                session.add(t)
                session.commit()
        raise
