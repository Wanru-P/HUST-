#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_benchmark.py — Phase 3 编排器：依次跑完三组实验并合并结果 + 生成报告。

三组实验（**结论严格分开**）：
  P1 真实数据规模扫描   hub-stats models.parquet，10/25/50/100% 嵌套子集，10 个算子
  P2 并行度扫描         local[2] / local[4] / local[6]，同一规模、同一算子，其余条件固定
  P3 合成数据压力测试   逐行变化的合成 parquet，仅用于受控压力测试

设计要点：
  - 每组都用独立子进程 + 独立 SparkSession，因为 master / driver memory 必须在
    JVM 启动前确定，无法在同一个进程里切换。
  - 每阶段前打印可用内存/磁盘，避免在资源不足时硬上。
  - 合并后的 benchmark_all.csv / benchmark_all_summary.csv 是**唯一**被报告引用的数据源。

用法：
    python run_benchmark.py                 # 跑全部三组
    python run_benchmark.py --phases p1     # 只跑真实数据规模扫描
"""
import argparse
import csv
import glob
import json
import os
import subprocess
import sys
import time

import psutil

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
ANALYSIS = os.path.join(PROJECT_ROOT, "data_hubstats", "gold", "analysis")
LOG_DIR = os.path.join(PROJECT_ROOT, "data_hubstats", "_smoke")
PY = sys.executable

PHASES = {
    "p1": {
        "desc": "真实数据规模扫描（10/25/50/100%，10 个算子）",
        # driver 内存取 3g（不是常规的 8g 起步值，也不是更省的 2g）：
        #   本机物理 31.43 GiB，但实测**可用**内存只有约 6～8.5 GiB
        #   （用户自己的 qemu 虚拟机约 3.1 GiB + Gradle/Kotlin 守护进程约 4.7 GiB 常驻占用）。
        #   * -Xmx2g：local[6] 下 6 个并发任务解码全部 20 列时直接
        #     java.lang.OutOfMemoryError: Java heap space，实验无法完成；
        #   * -Xmx3g：可以完成（首轮实测 350s 跑完 187 次测量），
        #     但若启动时可用内存只有 6 GiB 左右，JVM 的 RSS 峰值会达到 6.18 GiB，
        #     把系统可用内存压到 0.004 GiB，100% 档的重算子发生换页、耗时不可复现。
        # 因此 3g 是"能跑完"与"不压垮系统"之间唯一可用的取值，并依赖两重保护：
        #   1) run_one 启动前打印可用内存；
        #   2) benchmark.py 对每次测量采样可用内存，低于阈值即置 mem_pressure=true
        #      并在汇总中单独成组，绝不与正常测量混算中位数。
        "env": {"HF_SPARK_MASTER": "local[6]", "HF_SPARK_DRIVER_MEMORY": "3g"},
        "args": ["--mode", "real", "--scales", "10,25,50,100",
                 "--partitions", "16,32,64", "--partition-sweep-scales", "100",
                 "--repeats", "3", "--warmup", "1",
                 "--tag", "real_local6_dm3g"],
        "out": "benchmark_real_detail.csv", "out_summary": "benchmark_real_summary.csv",
    },
    "p2": {
        "desc": "并行度扫描（local[2] / local[4] / local[6]，50% 规模）",
        "multi": [
            {"HF_SPARK_MASTER": "local[2]", "HF_SPARK_DRIVER_MEMORY": "3g",
             "tag": "cores_local2"},
            {"HF_SPARK_MASTER": "local[4]", "HF_SPARK_DRIVER_MEMORY": "3g",
             "tag": "cores_local4"},
            {"HF_SPARK_MASTER": "local[6]", "HF_SPARK_DRIVER_MEMORY": "3g",
             "tag": "cores_local6"},
        ],
        "args": ["--mode", "real", "--scales", "50",
                 "--partitions", "16,32,64", "--partition-sweep-scales", "50",
                 "--repeats", "3", "--warmup", "1"],
        "out": "benchmark_{tag}_detail.csv",
        "out_summary": "benchmark_{tag}_summary.csv",
    },
    "p3": {
        "desc": "合成数据压力测试（仅用于受控压力测试）",
        "env": {"HF_SPARK_MASTER": "local[6]", "HF_SPARK_DRIVER_MEMORY": "3g"},
        "args": ["--mode", "synth", "--scales", "200000,1000000,3000000",
                 "--partitions", "16,32,64", "--partition-sweep-scales", "100",
                 "--repeats", "3", "--warmup", "1",
                 "--tag", "synth_local6_dm3g"],
        "out": "benchmark_synth_detail.csv", "out_summary": "benchmark_synth_summary.csv",
    },
}


def resource_snapshot():
    vm = psutil.virtual_memory()
    du = psutil.disk_usage(PROJECT_ROOT)
    return (f"可用内存 {vm.available / 2**30:.2f} GiB / 总 {vm.total / 2**30:.2f} GiB "
            f"（占用 {vm.percent:.1f}%）；工程盘剩余 {du.free / 2**30:.1f} GiB")


# 启动一个阶段前，可用内存低于该值就告警（GiB）。
# 依据：实测 -Xmx3g 的 Spark JVM 在 local[6] 下 RSS 峰值约 6.2 GiB，
# 若启动时可用内存不足，运行中会被压到接近 0 并触发换页，测量结果不可复现。
MIN_AVAIL_GIB = 7.5


def preflight_warning():
    """返回启动前的资源告警文本；资源充足时返回空串。"""
    vm = psutil.virtual_memory()
    avail = vm.available / 2**30
    if avail >= MIN_AVAIL_GIB:
        return ""
    return (f"⚠️ 启动前可用内存仅 {avail:.2f} GiB（低于建议的 {MIN_AVAIL_GIB} GiB）。"
            f"Spark JVM 峰值约需 6 GiB，运行中可能发生换页，"
            f"受影响的测量会在 mem_pressure 列被标为 true 并单独成组。"
            f"建议先关闭不用的程序（如 qemu 虚拟机、Gradle/Kotlin 守护进程）再运行。")


def run_one(tag, env_extra, args, out, out_summary, log_name):
    env = dict(os.environ)
    env.update(env_extra)
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [PY, "benchmark.py"] + args + ["--out", os.path.join(ANALYSIS, out),
                                        "--out-summary", os.path.join(ANALYSIS, out_summary)]
    log_path = os.path.join(LOG_DIR, log_name)
    # 先删掉本次要写的两个 CSV：否则若本阶段失败，上一轮的同名文件会留在原地，
    # 后面的 merge 会把它当成本次结果合并进去 —— 那等于用旧数据冒充新实验，
    # 而且因为退出码只在 meta 里体现，很容易被忽略。
    for stale in (out, out_summary):
        sp = os.path.join(ANALYSIS, stale)
        if os.path.exists(sp):
            os.remove(sp)
            print(f"[bench-run] 已清除上一轮遗留文件 {stale}", flush=True)
    print(f"\n[bench-run] {tag}  {resource_snapshot()}", flush=True)
    warn = preflight_warning()
    if warn:
        print(f"[bench-run] {warn}", flush=True)
    print(f"[bench-run] {' '.join(f'{k}={v}' for k, v in env_extra.items())}", flush=True)
    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.Popen(cmd, cwd=PROJECT_ROOT, env=env,
                                stdout=log, stderr=subprocess.STDOUT)
        code = proc.wait()
    dt = time.time() - t0
    print(f"[bench-run] {tag} 退出码={code} 用时={dt:.1f}s 日志={log_name}", flush=True)
    if code != 0:
        print(f"[bench-run] ❌ {tag} 失败！该阶段结果缺失，详见 {log_name}。"
              "后续 merge 不会包含这一阶段。", flush=True)
    return {"tag": tag, "exit_code": code, "seconds": round(dt, 1), "log": log_name,
            "env": env_extra, "detail": out, "summary": out_summary}


def merge(pattern, out_name, key_order=None):
    """把多个明细/汇总 CSV 合并成一个，去重列顺序。

    必须排除**本次合并的输出文件本身**：`benchmark_all_summary.csv` 也匹配
    glob `benchmark_*_summary.csv`，若不过滤，第二次运行会把上一次的合并结果
    再当成一份输入读进来，造成行数翻倍、统计口径被污染。
    """
    out_path = os.path.join(ANALYSIS, out_name)
    files = sorted(fp for fp in glob.glob(os.path.join(ANALYSIS, pattern))
                   if os.path.abspath(fp) != os.path.abspath(out_path))
    rows, cols = [], []
    for fp in files:
        with open(fp, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for c in r.fieldnames:
                if c not in cols:
                    cols.append(c)
            rows.extend(list(r))
    if not rows:
        return None, 0
    if key_order:
        cols = [c for c in key_order if c in cols] + [c for c in cols if c not in key_order]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[bench-merge] {out_name}: {len(rows)} 行（来自 {len(files)} 个文件）")
    return out_path, len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phases", default="p1,p2,p3")
    ap.add_argument("--skip-merge", action="store_true")
    args = ap.parse_args()
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(ANALYSIS, exist_ok=True)

    wanted = [p.strip() for p in args.phases.split(",") if p.strip()]
    runs = []
    for key in wanted:
        cfg = PHASES[key]
        print(f"\n===== {key}: {cfg['desc']} =====", flush=True)
        if "multi" in cfg:
            for m in cfg["multi"]:
                env_extra = {k: v for k, v in m.items() if k != "tag"}
                runs.append(run_one(m["tag"], env_extra,
                                    cfg["args"] + ["--tag", m["tag"]],
                                    cfg["out"].format(tag=m["tag"]),
                                    cfg["out_summary"].format(tag=m["tag"]),
                                    f"benchmark_{key}_{m['tag']}.log"))
        else:
            tag = cfg["env"].get("HF_SPARK_MASTER", "spark").replace("[", "").replace("]", "")
            runs.append(run_one(f"{key}_{tag}", cfg["env"], cfg["args"],
                                cfg["out"], cfg["out_summary"], f"benchmark_{key}.log"))

    if not args.skip_merge:
        merge("benchmark_*_detail.csv", "benchmark_all.csv")
        merge("benchmark_*_summary.csv", "benchmark_all_summary.csv")

    meta = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "runs": runs, "phases": {k: PHASES[k]["desc"] for k in wanted},
            "note": ("本机为 local[N] 单机多核，实验只能说明本地并行度与资源对耗时的影响，"
                     "不得表述为多节点横向扩展；合成数据结果只用于受控压力测试。")}
    with open(os.path.join(ANALYSIS, "benchmark_runs.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print("\n[bench-run] 完成:", json.dumps(runs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
