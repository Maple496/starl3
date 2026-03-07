import sys
import os
import json
import time
import subprocess
import logging
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    stream=sys.stdout
)
log = logging.getLogger("scheduler")

if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent

STATE_FILE = BASE_DIR / ".scheduler_state.json"


def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_schedule(path=None):
    path = Path(path) if path else BASE_DIR / "schedule.json"
    if not path.exists():
        log.error(f"配置不存在: {path}")
        sys.exit(2)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def build_command(task):
    program = task["program"]
    args = task.get("args", [])
    ext = Path(program).suffix.lower()
    if ext == ".py":
        python = task.get("python", sys.executable)
        return [python, str(BASE_DIR / program)] + [str(a) for a in args]
    return [str(BASE_DIR / program)] + [str(a) for a in args]


def check_file_deps(task):
    for f in task.get("depends_on_files", []):
        if not (BASE_DIR / f).exists():
            return False, str(f)
    return True, None


def run_task(task):
    task_id = task["id"]
    cmd = build_command(task)
    timeout = task.get("timeout", 3600)
    retry = task.get("retry", 0)
    retry_delay = task.get("retry_delay", 5)

    for attempt in range(1 + retry):
        if attempt > 0:
            log.info(f"[{task_id}] 第{attempt}次重试")
            time.sleep(retry_delay)

        log.info(f"[{task_id}] 执行: {' '.join(cmd)}")
        start = time.time()

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(BASE_DIR))
        elapsed = round(time.time() - start, 2)

        if result.stdout.strip():
            for line in result.stdout.strip().split('\n'):
                log.info(f"[{task_id}:out] {line}")
        if result.stderr.strip():
            for line in result.stderr.strip().split('\n'):
                log.warning(f"[{task_id}:err] {line}")

        if result.returncode == 0:
            log.info(f"[{task_id}] 成功 elapsed={elapsed}s")
            return {"status": "success", "code": 0, "elapsed": elapsed}

        log.error(f"[{task_id}] 失败(code={result.returncode}) elapsed={elapsed}s")

    return {"status": "failed", "code": result.returncode, "elapsed": elapsed}


def run_pipeline(tasks, on_failure="continue"):
    results = {}
    state = load_state()
    from datetime import datetime
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log.info(f"调度开始 run_id={run_id} tasks={len(tasks)}")

    for task in tasks:
        task_id = task["id"]

        if not task.get("enabled", True):
            log.info(f"[{task_id}] 禁用，跳过")
            results[task_id] = {"status": "skipped"}
            continue

        deps = task.get("depends_on", [])
        dep_failed = next((d for d in deps if results.get(d, {}).get("status") != "success"), None)
        if dep_failed:
            log.warning(f"[{task_id}] 依赖 {dep_failed} 未成功，跳过")
            results[task_id] = {"status": "dep_failed"}
            continue

        ok, missing = check_file_deps(task)
        if not ok:
            log.warning(f"[{task_id}] 文件缺失: {missing}，跳过")
            results[task_id] = {"status": "file_missing"}
            continue

        result = run_task(task)
        results[task_id] = result
        state[task_id] = {"run_id": run_id, "time": datetime.now().isoformat(), **result}
        save_state(state)

        if result["status"] == "failed" and on_failure == "stop":
            log.error("策略为stop，终止")
            break

    failed = sum(1 for r in results.values() if r.get("status") == "failed")
    log.info(f"调度完成 run_id={run_id} failed={failed}")
    return failed


def build_trigger(schedule_cfg):
    stype = schedule_cfg.get("type", "once")

    if stype == "interval":
        return IntervalTrigger(seconds=schedule_cfg.get("interval_seconds", 3600))

    if stype == "cron":
        return CronTrigger(**schedule_cfg.get("cron", {"hour": 8, "minute": 0}))

    if stype == "daily":
        at = schedule_cfg.get("at", "08:00")
        h, m = at.split(":")
        return CronTrigger(hour=int(h), minute=int(m))

    if stype == "weekday":
        at = schedule_cfg.get("at", "08:00")
        h, m = at.split(":")
        return CronTrigger(day_of_week="mon-fri", hour=int(h), minute=int(m))

    return None


def on_job_event(event):
    if event.exception:
        log.error(f"调度任务异常: {event.exception}")
    else:
        log.info(f"调度任务完成 retval={event.retval}")


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    mode = sys.argv[2] if len(sys.argv) > 2 else "once"

    config = load_schedule(config_path)
    tasks = config["tasks"]
    on_failure = config.get("on_failure", "continue")

    if mode == "once":
        failed = run_pipeline(tasks, on_failure)
        sys.exit(1 if failed else 0)

    schedule_cfg = config.get("schedule", {})
    trigger = build_trigger(schedule_cfg)

    if not trigger:
        log.info("无定时配置，执行一次")
        failed = run_pipeline(tasks, on_failure)
        sys.exit(1 if failed else 0)

    scheduler = BlockingScheduler()
    scheduler.add_listener(on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    if schedule_cfg.get("run_on_start", True):
        run_pipeline(tasks, on_failure)

    scheduler.add_job(run_pipeline, trigger, args=[tasks, on_failure], id="main_pipeline", max_instances=1)

    log.info(f"守护模式启动 trigger={trigger}")
    scheduler.start()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.info("手动中止")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        log.error(f"调度器异常: {e}")
        sys.exit(1)