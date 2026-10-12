# -*- coding: utf-8 -*-
"""生成补充验证图 Figure S1 / S2（PNG + PDF + SVG，风格对齐 make_figures.py）。

数据来源：supplementary/rq2_gini_result.json（S1）、supplementary/rq4_time_split_result.json（S2）。
用法：python supplementary/make_supp_figures.py
"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["figure.dpi"] = 120
plt.rcParams["savefig.dpi"] = 300

C_MAIN, C_ACCENT, C_ALT, C_GRID = "#2563eb", "#ef4444", "#0f9d58", "#e5e7eb"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R2_JSON = os.path.join(PROJECT_ROOT, "supplementary", "rq2_gini_result.json")
RQ4_JSON = os.path.join(PROJECT_ROOT, "supplementary", "rq4_time_split_result.json")
FIG_DIR = os.path.join(PROJECT_ROOT, "figures_supp")
os.makedirs(FIG_DIR, exist_ok=True)


def _save(fig, name):
    for ext in ("png", "pdf", "svg"):
        fig.savefig(os.path.join(FIG_DIR, f"{name}.{ext}"), bbox_inches="tight")
    plt.close(fig)


def fig_s1():
    d = json.load(open(R2_JSON, encoding="utf-8"))
    labels = ["全部", "≥1", "≥10", "≥100", "≥1k", "≥10k", "≥100k"]
    d30 = [p["gini"] for p in d["downloads"]["decay"]]
    dall = [p["gini"] for p in d["downloadsAllTime"]["decay"]]
    g_pos = d["downloads"]["g_pos"]
    g_top = d["downloads"]["g_top"]
    x = range(len(labels))
    fig, ax = plt.subplots(figsize=(8, 4.6))
    ax.plot(x, d30, "o-", color=C_MAIN, lw=2, ms=6, label="近 30 天下载量")
    ax.plot(x, dall, "s--", color=C_ACCENT, lw=2, ms=6, label="累计下载量")
    ax.set_xticks(list(x)); ax.set_xticklabels(labels)
    ax.set_ylim(0.68, 1.01)
    ax.set_ylabel("Gini 系数")
    ax.set_xlabel("只保留「下载量 ≥ 阈值」的模型（阈值）")
    ax.set_title("RQ2 稳健性：Gini 随剔除长尾阈值的衰减")
    ax.annotate(f"G_pos={g_pos:.4f}（剔零）", xy=(1, d30[1]), xytext=(2.1, 1.002),
                color=C_MAIN, fontsize=8, arrowprops=dict(arrowstyle="->", color=C_MAIN, lw=0.8))
    ax.annotate(f"G_top={g_top:.4f}\n(Top-1% 内部)", xy=(6, d30[6]), xytext=(4.0, 0.72),
                color=C_MAIN, fontsize=8, arrowprops=dict(arrowstyle="->", color=C_MAIN, lw=0.8))
    ax.text(0.02, 0.03, "剔除 43.5% 零下载后 Gini 仍≈0.992（Top-1% 内部 0.937）：集中度在剔除零值后依然很高",
            transform=ax.transAxes, fontsize=8.5, color="#374151")
    ax.grid(axis="y", color=C_GRID, lw=0.5)
    ax.legend(fontsize=8, loc="upper right")
    fig.tight_layout()
    _save(fig, "figS1_rq2_gini_robustness")


def fig_s2():
    r = json.load(open(RQ4_JSON, encoding="utf-8"))
    keys = ["Baseline_mean", "LinearRegression", "RandomForest", "GBT"]
    labels = ["Baseline_mean", "线性回归", "随机森林", "GBT"]
    random = [r["models"][k]["random"] for k in keys]
    temporal = [r["models"][k]["temporal"] for k in keys]
    calendar = [r["models"][k]["calendar"] for k in keys]
    import numpy as np
    x = np.arange(len(labels))
    w = 0.26
    fig, ax = plt.subplots(figsize=(8.6, 4.4))
    b1 = ax.bar(x - w, random, w, color=C_MAIN, label="随机切分（原报告口径）")
    b2 = ax.bar(x, temporal, w, color=C_ACCENT, label="时间切分（最老 80% 训练）")
    b3 = ax.bar(x + w, calendar, w, color=C_ALT, label="日历切分（2026 前训练）")
    for bars in (b1, b2, b3):
        for b in bars:
            h = b.get_height()
            if abs(h) < 0.01:
                continue
            ax.text(b.get_x() + b.get_width()/2, h + (0.008 if h >= 0 else -0.03),
                    f"{h:.2f}", ha="center", va="bottom" if h >= 0 else "top", fontsize=7.5)
    ax.axhline(0, color="#9ca3af", lw=0.8)
    ax.set_xticks(x); ax.set_xticklabels(labels)
    ax.set_ylim(-0.08, 0.88)
    ax.set_ylabel("R²（测试集）")
    ax.set_title("RQ4 时间切分对照：随机切分 vs 按创建时间切分")
    ax.text(0.02, 0.95, "随机森林 R² 0.68 → 0.50（−26%）：说明性能对创建时间队列分布较敏感；\n特征与目标同快照，不能据此量化时间泄漏，也不代表未来预测",
            transform=ax.transAxes, fontsize=8.5, color="#374151", va="top")
    ax.grid(axis="y", color=C_GRID, lw=0.5)
    ax.legend(fontsize=8, loc="upper right")
    fig.tight_layout()
    _save(fig, "figS2_rq4_time_split")


if __name__ == "__main__":
    fig_s1()
    fig_s2()
    print("figures written ->", FIG_DIR)
