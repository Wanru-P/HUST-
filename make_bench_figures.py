#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_bench_figures.py — Benchmark 三张图（Figure 10–12）的**独立入口**。

历史沿革
--------
本文件原先是旧 Demo 时代独立绘制 benchmark 的脚本，数据源是
``data/gold/analysis/benchmark_scale.csv``（旧 Demo 结果，已废弃）。
Phase 3 起 benchmark 的真实结果统一落在
``data_hubstats/gold/analysis/benchmark_all_summary.csv``，绘图逻辑并入
``make_figures.py`` 的 ``fig10_scale_runtime`` / ``fig11_partitions`` /
``fig12_operator_comparison``。

为遵守「保留脚本入口与输出格式、不重写项目」的工程约束，本文件保留原有
入口名与调用方式（``python make_bench_figures.py``），但内部**委托**给
``make_figures.py`` 中同名模块级函数，保证：

  * 只有一处绘图实现，不会出现两份口径不一致的 Fig10–12；
  * 输出目录与正式图一致（``figures_final/``，PNG 300dpi + PDF + SVG）；
  * 数据源只读正式 benchmark 结果，**不再读旧 Demo 的 data/ 目录**。

依赖前置产物：``data_hubstats/gold/analysis/benchmark_all_summary.csv``
（由 ``run_benchmark.py`` 生成）。若该文件不存在，本脚本会明确报错并给出
应先执行的命令，而不是回退到旧 Demo 数据。

用法：python make_bench_figures.py
"""
import os
import sys

import pandas as pd

import make_figures as MF

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BENCH_CSV = os.path.join(MF.ANALYSIS, "benchmark_all_summary.csv")


def main():
    if not os.path.exists(BENCH_CSV):
        print("[bench-fig] 缺少正式 benchmark 结果：")
        print(f"           {BENCH_CSV}")
        print("[bench-fig] 请先运行： python run_benchmark.py")
        print("[bench-fig] （本脚本不会回退到旧 Demo 的 data/gold/analysis/benchmark_scale.csv）")
        return 2

    summary = pd.read_csv(BENCH_CSV)
    if summary.empty:
        print("[bench-fig] benchmark_all_summary.csv 为空，无可绘制内容。")
        return 2

    n_before = len(MF.FIG_INDEX)
    MF.fig10_scale_runtime(summary)
    MF.fig11_partitions(summary)
    MF.fig12_operator_comparison(summary)
    n_drawn = len(MF.FIG_INDEX) - n_before

    if n_drawn == 0:
        print("[bench-fig] 未生成任何图（可能缺少真实数据 benchmark 行）。")
        print("            本机真实数据 benchmark 未完成时，Figure 10–12 会如实留空，")
        print("            不使用合成数据结果冒充真实数据结果。")
        return 1

    print(f"[bench-fig] 共生成 {n_drawn} 张 -> {MF.FIG_DIR}")
    for item in MF.FIG_INDEX[n_before:]:
        print(f"           {item['file']}  {item['title']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
