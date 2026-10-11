#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_phase2.py — Phase 2 编排器：按固定顺序、固定配置跑完 RQ1–RQ5 的 Spark 分析。

为什么要编排而不是手动一个个跑：
  1. 每个脚本都要重新启动 JVM，资源必须在启动前确定；顺序执行可避免并发抢内存。
  2. Phase 5 要求「完整执行顺序 + 成功/失败日志」，因此每次运行的配置、
     退出码、耗时、日志路径都必须落盘，而不是靠人回忆。
  3. 每个脚本前打印一次系统可用内存/磁盘，避免在资源不足时硬上。

顺序（后者依赖前者的产物）：
  1. analysis_evo.py         RQ1 + RQ2  -> gold/analysis/*.csv|json
  2. analysis_graph.py       RQ3 Spark 原生图统计 -> graph_stats.json / node_metrics
  3. analysis_ml.py          RQ4 MLlib（先划分后拟合）-> ml_summary.json
  4. analysis_rq5.py         RQ5 任务生态对比 -> rq5_task_ecosystem.csv

（analysis_graph_gf.py 单独跑：它是长任务，带超时看门狗，见 docs/graph_analysis_report.md）

用法：
    python run_phase2.py
    python run_phase2.py --only evo,graph
"""
import argparse
import json
import os
import subprocess
import sys
import time

import psutil

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "data_hubstats", "_smoke")
ANALYSIS = os.path.join(PROJECT_ROOT, "data_hubstats", "gold", "analysis")
PY = sys.executable

CONFIG = {
    "HF_SPARK_MASTER": os.environ.get("HF_SPARK_MASTER", "local[6]"),
    "HF_SPARK_DRIVER_MEMORY": os.environ.get("HF_SPARK_DRIVER_MEMORY", "4g"),
    "HF_SHUFFLE_PARTITIONS": os.environ.get("HF_SHUFFLE_PARTITIONS", "64"),
    "HF_ENGINE": "spark",
    "PYTHONUNBUFFERED": "1",
}

STEPS = [
    ("evo", "analysis_evo.py", "RQ1 生态演化 + RQ2 注意力集中与长尾"),
    ("graph", "analysis_graph.py", "RQ3 Spark 原生图统计（出度/入度/可达/外部父模型）"),
    ("ml", "analysis_ml.py", "RQ4 MLlib 独立测试集训练与评估"),
    ("rq5", "analysis_rq5.py", "RQ5 任务生态对比"),
]


def snapshot():
    vm = psutil.virtual_memory()
    du = psutil.disk_usage(PROJECT_ROOT)
    return (f"可用内存 {vm.available / 2**30:.2f} GiB（占用 {vm.percent:.1f}%）；"
            f"磁盘剩余 {du.free / 2**30:.1f} GiB")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None, help="逗号分隔的步骤 key：evo,graph,ml,rq5")
    args = ap.parse_args()
    wanted = [s.strip() for s in args.only.split(",")] if args.only else None
    os.makedirs(LOG_DIR, exist_ok=True)

    env = dict(os.environ)
    env.update(CONFIG)
    records = []
    for key, script, desc in STEPS:
        if wanted and key not in wanted:
            continue
        log_name = f"phase2_{key}.log"
        log_path = os.path.join(LOG_DIR, log_name)
        print(f"\n===== Phase2 {key}: {desc} =====", flush=True)
        print(f"[phase2] 配置 {CONFIG}", flush=True)
        print(f"[phase2] {snapshot()}", flush=True)
        t0 = time.time()
        with open(log_path, "w", encoding="utf-8") as log:
            proc = subprocess.Popen([PY, script], cwd=PROJECT_ROOT, env=env,
                                    stdout=log, stderr=subprocess.STDOUT)
            code = proc.wait()
        dt = time.time() - t0
        ok = code == 0
        print(f"[phase2] {script} 退出码={code} 用时={dt:.1f}s 日志={log_name} "
              f"{'OK' if ok else 'FAILED'}", flush=True)
        records.append({"step": key, "script": script, "desc": desc,
                        "exit_code": code, "seconds": round(dt, 1), "log": log_name,
                        "ok": ok})
        if not ok:
            print(f"[phase2] 中止：{script} 失败，后续步骤依赖其产物。", flush=True)
            break

    out = os.path.join(ANALYSIS, "phase2_runs.json")
    os.makedirs(ANALYSIS, exist_ok=True)
    # 合并而不是覆盖：用 --only 只跑部分步骤时，若直接覆盖，之前跑过的步骤
    # （evo/graph/ml）的运行记录会整段丢失，Phase 5 就无法再证明「完整执行顺序」。
    # 以 step 为键合并：本次跑过的步骤替换同名旧记录，未跑的步骤原样保留。
    history = {}
    if os.path.exists(out):
        try:
            with open(out, encoding="utf-8") as f:
                for r in json.load(f).get("steps", []):
                    history[r["step"]] = r
        except (OSError, ValueError):
            history = {}
    for r in records:
        history[r["step"]] = r
    step_order = [k for k, _, _ in STEPS]
    merged = [history[k] for k in step_order if k in history]
    merged += [r for k, r in history.items() if k not in step_order]
    payload = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
               "config": CONFIG,
               "ran_this_invocation": [r["step"] for r in records],
               "steps": merged,
               "all_ok": all(r["ok"] for r in merged) and len(merged) > 0}
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\n[phase2] 运行记录（已与历史记录合并）-> {out}")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not payload["all_ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
