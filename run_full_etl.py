#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_full_etl.py — 全量 Spark ETL 运行器（Phase 1B）。

职责（只做"跑 + 量"，不改 ETL 逻辑）：
  1. 以固定环境变量启动 spark_etl.py（master / driver memory / shuffle partitions）。
  2. 后台线程采样**系统内存**与**子进程 RSS**，得到内存峰值。
  3. 记录墙钟耗时、退出码、stdout/stderr 日志路径。
  4. 把配置与指标写成 data_hubstats/_smoke/full_etl_run_report.json。

为什么单独一个脚本：Phase 1B 要求记录「Spark Master / Driver Memory / Shuffle
Partitions / 运行时间 / 内存峰值 / 输入输出文件大小」，这些指标不在 ETL 进程内部，
必须由外部采样，且不能靠事后估算。

用法：
    python run_full_etl.py                     # 用默认配置跑全量
    HF_SPARK_MASTER=local[6] python run_full_etl.py
"""
import json
import os
import subprocess
import sys
import threading
import time

import psutil

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "data_hubstats", "_smoke")
LOG_PATH = os.path.join(LOG_DIR, "spark_etl_full.log")
REPORT = os.path.join(LOG_DIR, "full_etl_run_report.json")

CONFIG = {
    "HF_ENGINE": os.environ.get("HF_ENGINE", "spark"),
    "HF_SPARK_MASTER": os.environ.get("HF_SPARK_MASTER", "local[6]"),
    "HF_SPARK_DRIVER_MEMORY": os.environ.get("HF_SPARK_DRIVER_MEMORY", "3g"),
    "HF_SHUFFLE_PARTITIONS": os.environ.get("HF_SHUFFLE_PARTITIONS", "64"),
    "JAVA_HOME": os.environ.get("JAVA_HOME", "E:/Software/jdk-17"),
    "PYSPARK_PYTHON": sys.executable,
    # 重定向到文件时 Python 默认块缓冲，日志会长时间不可见；强制行缓冲便于实时监控
    "PYTHONUNBUFFERED": "1",
}


def dir_size(path):
    """目录下所有文件的字节数之和（Spark 输出是目录，不是单文件）。"""
    total = 0
    for base, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(base, name))
            except OSError:
                pass
    return total


def sample_memory(stop, samples):
    """后台采样线程体：系统可用/已用内存 + 所有 java.exe 的 RSS 之和。

    注意：必须遍历 psutil.process_iter，而不是用 subprocess.Popen 对象——
    Popen 没有 memory_info()（早期版本因此在线程里抛 AttributeError 后静默退出，
    peak_jvm_rss_gib 一直是 0.0）。
    """
    while not stop.is_set():
        vm = psutil.virtual_memory()
        if samples["sys_available_min"] is None or vm.available < samples["sys_available_min"]:
            samples["sys_available_min"] = vm.available
        samples["sys_used_max"] = max(samples["sys_used_max"], vm.used)
        rss = 0
        for p in psutil.process_iter(["name", "memory_info"]):
            try:
                name = (p.info["name"] or "").lower()
                if name in ("java.exe", "java") and p.info["memory_info"]:
                    rss += p.info["memory_info"].rss
            except (psutil.Error, AttributeError, KeyError):
                pass
        samples["jvm_rss_max"] = max(samples["jvm_rss_max"], rss)
        time.sleep(0.5)


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    env = dict(os.environ)
    env.update(CONFIG)

    samples = {"sys_available_min": None, "sys_used_max": 0, "jvm_rss_max": 0}
    stop = threading.Event()
    th = threading.Thread(target=sample_memory, args=(stop, samples), daemon=True)
    th.start()

    t0 = time.time()
    with open(LOG_PATH, "w", encoding="utf-8") as log:
        proc = subprocess.Popen([sys.executable, "spark_etl.py"],
                                cwd=PROJECT_ROOT, env=env,
                                stdout=log, stderr=subprocess.STDOUT)
        code = proc.wait()
    elapsed = time.time() - t0
    stop.set()
    th.join(timeout=3)

    with open(LOG_PATH, encoding="utf-8", errors="replace") as f:
        lines = f.read().splitlines()
    steps = [ln for ln in lines if ln.startswith("[time]")]
    timings = [ln for ln in lines if ln.startswith(("[silver]", "[gold]", "[bronze]"))]

    bronze = os.path.join(PROJECT_ROOT, "data_hubstats", "bronze", "models.parquet")
    report = {
        "config": CONFIG,
        "exit_code": code,
        "elapsed_seconds": round(elapsed, 1),
        "peak_system_used_gib": round(samples["sys_used_max"] / 2 ** 30, 2),
        "min_system_available_gib": round((samples["sys_available_min"] or 0) / 2 ** 30, 2),
        "peak_jvm_rss_gib": round(samples["jvm_rss_max"] / 2 ** 30, 2),
        "bronze_bytes": os.path.getsize(bronze) if os.path.exists(bronze) else None,
        "silver_bytes": dir_size(os.path.join(PROJECT_ROOT, "data_hubstats", "silver")),
        "gold_bytes": dir_size(os.path.join(PROJECT_ROOT, "data_hubstats", "gold")),
        "step_timings": steps,
        "etl_messages": timings,
        "log_path": os.path.relpath(LOG_PATH, PROJECT_ROOT).replace("\\", "/"),
    }
    with open(REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    sys.exit(code)


if __name__ == "__main__":
    main()
