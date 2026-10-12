#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_figures.py — Phase 4：生成 12 张**正式**图（Figure 1–12）。

数据来源（全部为正式结果，**不读旧 Demo 目录 data/，不读 gold_local/**）：
    data_hubstats/gold/analysis/     Spark 侧 RQ1–RQ5 结果
    data_hubstats/gold/*.parquet     Gold 表（models_enriched / model_family_stats 等）
    data_hubstats/gold/analysis/benchmark_*.csv   Phase 3 真实数据 benchmark

输出：
    figures_final/fig01..fig12_*.png    300 dpi
    figures_final/fig01..fig12_*.pdf    矢量（LaTeX 用）
    figures_final/fig01..fig12_*.svg    矢量
    dashboard_final.html                离线自包含看板（**不运行 Spark**）
    data_hubstats/gold/analysis/figure_index.json   图目录（编号/标题/数据源/口径）

⚠️ 规范
  - 轴必须带单位；图例完整；不使用截断纵轴或未说明的归一化；
  - 标题/脚注写明「数据快照」与「分析样本范围」；
  - 大图网络不画全图：先在全量上算指标，再取可解释子图绘制（family_graph.json 已是子图）；
  - 2026-10 为快照未走完的月份，图中显式标注，不与其他完整月份直接比较。

用法：python make_figures.py
"""
import base64
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
from matplotlib.patches import FancyBboxPatch

import schema_adapter as SA

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
GOLD = DIRS["gold"]
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(GOLD, "analysis")

SUPP_PATH = os.path.join(ANALYSIS, "supplementary_results.json")


def load_supplementary():
    """读取补充验证结果（Part A–E）；缺失时返回 {}。"""
    if not os.path.exists(SUPP_PATH):
        return {}
    with open(SUPP_PATH, encoding="utf-8") as f:
        return json.load(f)


def _sn(v, nd=0):
    try:
        return f"{float(v):,.{nd}f}"
    except (TypeError, ValueError):
        return "—"


def _sp(v, nd=1):
    try:
        return f"{float(v) * 100:.{nd}f}%"
    except (TypeError, ValueError):
        return "—"


def supp_dashboard():
    """Dashboard 的补充结论（Part A–E），数字只取自 supplementary_results.json。

    ⚠️ 两份 Dashboard 由本脚本生成；补充结论必须在这里产出，
    否则重新运行本脚本会把手工加进去的内容冲掉。
    """
    s = load_supplementary()
    if not s:
        return {"rq2": "", "rq3": "", "rq4": "", "limit_rows": []}
    d2 = (s.get("rq2_robustness") or {}).get("downloads", {})
    d2a = (s.get("rq2_robustness") or {}).get("downloadsAllTime", {})
    z = s.get("zero_download_age") or {}
    dd = s.get("derivation_vs_downloads") or {}
    trf = ((s.get("rq4_time_split") or {}).get("r2_by_split") or {}).get("RandomForest", {})
    ms = s.get("maintenance_span_sanity") or {}
    rq2 = (f"稳健性（补充验证）：剔除 {_sp(d2.get('zero_share'), 2)} 零下载模型后 Gini 仍为 "
           f"{_sn(d2.get('g_pos_excl_zero'), 6)}（累计 {_sn(d2a.get('g_pos_excl_zero'), 6)}）、"
           f"Top-1% 内部 {_sn(d2.get('g_top1pct_internal'), 6)}，下载量高度集中"
           f"<b>并不只是由零下载记录造成的</b>；零下载中 {_sp(z.get('share_zero_gt30'), 1)} 创建超 30 天，"
           f"剔除新模型后零下载率 {_sp(z.get('refined_zero_rate_excl_le30'), 2)}"
           f"（见 <code>docs/supplementary_analysis.md</code> Part A）。")
    rq3 = (f"衍生热度 ≠ 下载热度（补充验证）：底座衍生数与其自身下载量的秩相关仅 "
           f"<b>+{_sn(dd.get('spearman_children_vs_downloads30d'), 2)}</b>"
           f"（累计 +{_sn(dd.get('spearman_children_vs_downloadsAllTime'), 2)}，"
           f"n={_sn(dd.get('n_bases_compared'))}）；两榜前十交集为 "
           f"<b>{_sn(dd.get('top10_overlap_with_downloads_ranking'))}</b>"
           f"（同一总体内亦为 {_sn(dd.get('top10_overlap_within_bases'))}），且类别相反——"
           f"被大量微调的是大型生成式底座，被大量下载的是小型句子嵌入/编码模型；"
           f"<b>不能</b>把下载量读作被依赖程度（见 <code>docs/supplementary_analysis.md</code> Part D）。")
    rq4 = (f"时间切分对照（补充验证；与 Spark MLlib 口径分别列出、<b>不混算</b>）：同一模型按 "
           f"<code>createdAt</code> 切分后，RandomForest R² 由随机切分 {_sn(trf.get('random'), 2)} "
           f"降至 <b>{_sn(trf.get('temporal'), 2)}</b>；该差值说明模型对"
           f"<b>创建时间队列的分布差异</b>较为敏感，特征与目标同快照，<b>不能</b>把该差值归因为"
           f"「时间泄漏的贡献」，也<b>不能</b>解释为前瞻性预测能力。"
           f"特征质量（补充验证）：<code>maintenance_span_days</code> 含 "
           f"<b>{_sn(ms.get('n_negative_span'))} 个负值（{_sp(ms.get('negative_share'), 3)}）</b>"
           f"（最小 {_sn(ms.get('min_span_days'))} 天，物理上不可能）且 "
           f"<b>{_sp(ms.get('zero_share'), 3)} 整日法取值为 0</b>"
           f"（0 ≤ Δ < 1 天，<b>不是</b>「同日」；精确 Δ==0 仅 {_sn(ms.get('n_exact_zero_span'))} 个），"
           f"属时间戳/口径问题、不臆断根因（见 <code>docs/supplementary_analysis.md</code> Part C/E）。")
    limit_rows = [
        ["衍生数（children_count）/ 下载量",
         "中等正相关（ρ≈0.37–0.39），头部由不同类别模型占据，<b>不能互相替代读</b>"],
        ["maintenance_span_days 负值 / 有效维护跨度",
         f"<b>{_sp(ms.get('negative_share'), 3)}</b> 为负（物理上不可能），属时间戳/口径问题"],
    ]
    return {"rq2": rq2, "rq3": rq3, "rq4": rq4, "limit_rows": limit_rows}
FIG_DIR = os.path.join(PROJECT_ROOT, "figures_final")
SUPP_DIR = os.path.join(PROJECT_ROOT, "figures_supplementary")
FIG_REPORT_DIR = os.path.join(PROJECT_ROOT, "figures_report")
os.makedirs(FIG_DIR, exist_ok=True)
os.makedirs(SUPP_DIR, exist_ok=True)
os.makedirs(FIG_REPORT_DIR, exist_ok=True)

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["figure.dpi"] = 120
plt.rcParams["savefig.dpi"] = 300

C_MAIN, C_ACCENT, C_ALT, C_GRID = "#2563eb", "#ef4444", "#0f9d58", "#e5e7eb"
SNAPSHOT = SA.HUBSTATS_COMMIT[:8]
def build_footer():
    """图注里的样本范围从 Phase 1B 的正式结果自动读取，避免硬编码数字与真实结果漂移。

    去重模型数来自 `concentration_stats.json`（Phase 1B 产出），
    它比本脚本更早生成，不存在循环依赖；读不到时退化为不含数字的措辞，
    **绝不写一个可能过期的数字**。
    """
    cs = load_gold_json("concentration_stats.json") or {}
    n = cs.get("n_models")
    # Bronze 行数同样从结果清单读取：早先这里写死「Bronze 3,138,959 行」，
    # 与同一个函数里「绝不写一个可能过期的数字」的承诺自相矛盾（由 E5 检查发现）。
    bronze = manifest_value(load_manifest(), "data.bronze_rows")
    if isinstance(n, int) and isinstance(bronze, int):
        scope = f"快照内全部 {n:,} 个去重模型（Bronze {bronze:,} 行去重后）"
    elif isinstance(n, int):
        scope = f"快照内全部 {n:,} 个去重模型"
    else:
        scope = "快照内全部去重模型"
    return (f"数据快照 cfahlgren1/hub-stats models.parquet @ {SNAPSHOT}"
            f"（截至 2026-10-10）；样本：{scope}")

FIG_INDEX = []


def save(fig, num, slug, title, sources, note=""):
    base = f"fig{num:02d}_{slug}"
    for ext in ("png", "pdf", "svg"):
        fig.savefig(os.path.join(FIG_DIR, f"{base}.{ext}"), bbox_inches="tight")
    plt.close(fig)
    FIG_INDEX.append({"figure": f"Figure {num}", "file": f"{base}.png",
                      "title": title, "data_sources": sources, "note": note})
    print(f"[fig] {base}  {title}")


def save_supp(fig, base, title):
    """把**补充材料**图写入 figures_supplementary/。

    补充图不计入 figure_index.json，也不参与「12 张正式图」完整性守卫：
    它是被正式图替换掉的旧版/参考版本，保留是为了可追溯，而不是交付内容。
    """
    for ext in ("png", "pdf", "svg"):
        fig.savefig(os.path.join(SUPP_DIR, f"{base}.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print(f"[fig-supp] {base}  {title}")


def save_report(fig, base, title):
    """把 **A4 单栏报告版**图写入 figures_report/。

    报告版图不计入 figure_index.json、不参与 12 张正式图完整性守卫：
    它们是正式 Figure 7 的拆分版本，专为 LaTeX A4 单栏插图（约 15–16 cm 宽）设计，
    与 PPT/Dashboard 使用的 `figures_final/fig07_family_network.*` 并行存在、互不覆盖。
    """
    for ext in ("png", "pdf", "svg"):
        fig.savefig(os.path.join(FIG_REPORT_DIR, f"{base}.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print(f"[fig-report] {base}  {title}")


def load_json(name):
    path = os.path.join(ANALYSIS, name)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_gold_json(name):
    """读取 **gold/ 根目录**下的结果 JSON（注意：不是 gold/analysis/）。

    `concentration_stats.json` / `data_quality_report.json` 由 Phase 1B 的 ETL
    直接写在 gold/ 根下，而 RQ 分析产物写在 gold/analysis/ 下。两者极容易混淆：
    若用 `load_json("concentration_stats.json")`（它指向 analysis/）会**静默**拿到
    None，后果是 Figure 5、Figure 6 被整段跳过，Dashboard 的 Gini / Top-1% /
    零下载占比全部显示成 "-"，而进程退出码仍然是 0 —— 表面成功、实际缺件。
    """
    path = os.path.join(DIRS["gold"], name)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def read_csv(name):
    path = os.path.join(ANALYSIS, name)
    return pd.read_csv(path) if os.path.exists(path) else None


def read_spark_csv(name):
    """读取 Spark 写出的**目录式** CSV（`df.write.csv(path)` 的产物）。

    Spark 落盘的是一个**目录**：里面是若干 `part-*.csv` 分片加一个 `_SUCCESS`
    标记，而不是单个 `.csv` 文件。因此 `pd.read_csv(目录路径)` 必然失败，
    调用方若只写 `if imp is not None` 就会**静默**跳过整块内容 ——
    Figure 9 的 (a) 特征重要性面板就是这样丢掉的。
    这里显式合并全部分片，跳过 `_SUCCESS` 与 `.` 开头的元数据文件。
    """
    path = os.path.join(ANALYSIS, name)
    if not os.path.isdir(path):
        return None
    parts = sorted(f for f in os.listdir(path)
                   if f.endswith(".csv") and not f.startswith("_") and not f.startswith("."))
    if not parts:
        return None
    frames = [pd.read_csv(os.path.join(path, f)) for f in parts]
    return pd.concat(frames, ignore_index=True)


def read_parquet(name):
    path = os.path.join(GOLD, name)
    return pd.read_parquet(path) if os.path.exists(path) else None


def read_parquet_cols(name, columns):
    """按**列投影**读取 Gold parquet。

    `node_metrics` 有 313.9 万行，全列读入会白白吃掉内存；只取需要的列可显著
    降低峰值内存，且不改变任何数值（列裁剪与全读的取值完全一致）。
    """
    path = os.path.join(GOLD, name)
    if not os.path.exists(path):
        return None
    return pd.read_parquet(path, columns=columns)


def load_manifest():
    """读取 results_manifest.json 的 metrics 字典（全工程**唯一权威指标源**）。

    图表里的关键统计值必须经此获取，不允许在绘图代码里写死数字：
    一旦上游重算，硬编码的图会与 results_manifest.json 悄悄不一致，
    而这类漂移在人工浏览时几乎不可能发现。
    """
    m = load_json("results_manifest.json")
    return (m or {}).get("metrics") or {}


def manifest_value(metrics, key, default=None):
    """从 manifest 的 metrics 中取出某个指标的 value 字段。

    metrics 的结构是 {key: {"value":..., "source":..., "note":...}}，
    因此不能直接把 entry 当数字用。缺失时返回 default，绝不猜一个值。
    """
    entry = metrics.get(key)
    if isinstance(entry, dict):
        return entry.get("value", default)
    return default


def add_footer(fig):
    fig.text(0.5, -0.02, build_footer(), ha="center", fontsize=7, color="#6b7280")


def month_ticks(ax, labels, rotate=45):
    step = max(1, len(labels) // 14)
    ax.set_xticks(range(0, len(labels), step))
    ax.set_xticklabels(list(labels)[::step], rotation=rotate, ha="right", fontsize=8)


# ===================== Figure 1 =====================
def fig01_monthly(monthly, rq1):
    fig, ax = plt.subplots(figsize=(9, 4.2))
    # 首月要单独着色：该月绝大多数模型挤在数据源的最早一天（时间戳异常聚集），
    # 不是真实发布高峰，若与其它月份同色会被误读。
    # 注：只描述「观测到的聚集」，不宣称成因（根因未确认，见 summary_rq1.json）。
    c_floor = "#f59e0b"
    is_floor = monthly["month"] == rq1.get("first_month")
    colors = [C_ACCENT if p else (c_floor if f else C_MAIN)
              for p, f in zip(monthly["is_partial"], is_floor)]
    ax.bar(monthly["month"], monthly["new_models"], color=colors)
    ax.set_title("Figure 1  模型生态时间演化：月度新增模型数（按创建时间归月，UTC）")
    ax.set_xlabel("创建月份")
    ax.set_ylabel("新增模型数（个）")
    month_ticks(ax, monthly["month"])
    ax.grid(axis="y", color=C_GRID)
    if is_floor.any():
        # 标注必须放在图例**下方**：早期版本把它放在 0.80×max 处，正好压在左上角
        # 图例框上，文字被遮掉一半（实测由 check_figure_quality.py 检出 76% 重叠）。
        ax.annotate(
            f"{rq1.get('earliest_day', '首日')} 单日 "
            f"{rq1.get('earliest_day_count', 0):,} 个\n"
            f"（占首月 {rq1.get('date_floor_share', 0):.1%}，时间戳异常聚集）",
            xy=(0, monthly["new_models"].iloc[0]),
            xytext=(len(monthly) * 0.02, monthly["new_models"].max() * 0.42),
            fontsize=8, color="#b45309",
            arrowprops=dict(arrowstyle="->", color="#b45309", lw=0.8))
    if monthly["is_partial"].any():
        pm = monthly.loc[monthly["is_partial"], "month"].iloc[0]
        ax.axvline(len(monthly) - 0.5, color=C_ACCENT, ls="--", lw=1)
        ax.annotate(f"{pm} 快照未走完\n（不可与完整月份直接比较）",
                    xy=(len(monthly) - 1, monthly["new_models"].max() * 0.75),
                    xytext=(len(monthly) * 0.45, monthly["new_models"].max() * 0.92),
                    fontsize=8, color=C_ACCENT,
                    arrowprops=dict(arrowstyle="->", color=C_ACCENT, lw=0.8))
    ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=C_MAIN),
                       plt.Rectangle((0, 0), 1, 1, color=c_floor),
                       plt.Rectangle((0, 0), 1, 1, color=C_ACCENT)],
              labels=["完整月份", "首月（含时间戳异常聚集）", "快照截止月（不完整）"],
              fontsize=8, loc="upper left")
    add_footer(fig)
    fig.tight_layout()
    save(fig, 1, "monthly_evolution",
         "月度新增模型数（快照可见模型的创建时间分布）",
         ["gold/analysis/monthly_new_models.csv", "gold/analysis/summary_rq1.json"],
         f"峰值月 {rq1['peak_month']}（{rq1['peak_count']:,} 个）；"
         f"pipeline_tag 缺失率 {rq1['unknown_task_share']:.1%} 记为 unknown 单独报告；"
         f"首月 {rq1.get('first_month')} 有 {rq1.get('date_floor_share', 0):.1%} 集中在最早一天"
         f"（{rq1.get('earliest_day')}，该日仅 {rq1.get('earliest_day_distinct_timestamps')} 个"
         f"不同时间戳）：这是已观测到的时间戳异常聚集，**根因未确认**，"
         "不作真实发布高峰解读，该月不与其它月份直接比较")


# ===================== Figure 2 =====================
def fig02_task_growth(pipeline_trend, rq1):
    totals = (pipeline_trend.groupby("task")["new_models"].sum()
              .sort_values(ascending=False))
    top = [t for t in totals.head(8).index if t != "unknown"]
    pt = pipeline_trend.copy()
    pt["grp"] = np.where(pt["task"].isin(top), pt["task"],
                         np.where(pt["task"] == "unknown", "unknown（任务标签缺失）", "其他任务"))
    pv = (pt.pivot_table(index="month", columns="grp", values="new_models",
                         aggfunc="sum", fill_value=0).sort_index())
    order = [c for c in top if c in pv.columns] + \
            [c for c in pv.columns if c not in top]
    pv = pv[order]
    fig, ax = plt.subplots(figsize=(9.5, 4.6))
    ax.stackplot(pv.index, [pv[c] for c in pv.columns], labels=pv.columns, alpha=0.9)
    ax.set_title("Figure 2  任务构成随时间的演化（Top-8 任务 + unknown + 其他，堆叠面积）")
    ax.set_xlabel("创建月份")
    ax.set_ylabel("新增模型数（个）")
    month_ticks(ax, pv.index)
    ax.legend(loc="upper left", fontsize=7, ncol=3)
    ax.grid(axis="y", color=C_GRID, alpha=0.6)
    add_footer(fig)
    fig.tight_layout()
    save(fig, 2, "task_growth",
         "任务构成演化（堆叠面积图）",
         ["gold/analysis/pipeline_trend.csv", "gold/analysis/summary_rq1.json"],
         f"unknown（pipeline_tag 缺失）占全部模型 {rq1['unknown_task_share']:.1%}，"
         "缺失未被推断成任何具体任务；按任务的结论只覆盖已标注部分")


# ===================== Figure 3 =====================
def fig03_task_time_heatmap(pipeline_trend):
    pt = pipeline_trend.copy()
    pt["year"] = pt["month"].str[:4]
    totals = pt.groupby("task")["new_models"].sum().sort_values(ascending=False)
    top = [t for t in totals.index if t != "unknown"][:12]
    pv = (pt[pt["task"].isin(top)]
          .pivot_table(index="task", columns="year", values="new_models",
                       aggfunc="sum", fill_value=0).reindex(top))
    fig, ax = plt.subplots(figsize=(9, 5))
    im = ax.imshow(np.log1p(pv.values), aspect="auto", cmap="Blues")
    ax.set_xticks(range(len(pv.columns)), pv.columns, fontsize=9)
    ax.set_yticks(range(len(pv.index)), pv.index, fontsize=8)
    vmax = np.log1p(pv.values).max()
    for i in range(pv.shape[0]):
        for j in range(pv.shape[1]):
            v = pv.values[i, j]
            if v:
                ax.text(j, i, f"{v:,}", ha="center", va="center", fontsize=7,
                        color="white" if np.log1p(v) > vmax * 0.6 else "#1f2937")
    ax.set_title("Figure 3  Task × 年份 新增模型热力图（色深 = log(1+数量)，格内为实际数量）")
    ax.set_xlabel("创建年份")
    ax.set_ylabel("pipeline_tag（任务）")
    fig.colorbar(im, ax=ax, shrink=0.85, label="log(1 + 新增模型数)")
    add_footer(fig)
    fig.tight_layout()
    save(fig, 3, "task_time_heatmap", "Task × 年份热力图",
         ["gold/analysis/pipeline_trend.csv"],
         "2026 为快照截止年，只含 1—10 月，年际比较需注意覆盖不全")


# ===================== Figure 4 =====================
def fig04_library_task_structure(rq5, library_trend):
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 5))
    lib = (library_trend[library_trend["library"] != "unknown"]
           .groupby("library")["new_models"].sum().sort_values(ascending=False).head(15)
           .iloc[::-1])
    axes[0].barh(lib.index, lib.values, color=C_MAIN)
    axes[0].set_title("(a) Top-15 库（library_name）模型数")
    axes[0].set_xlabel("模型数（个）")
    axes[0].grid(axis="x", color=C_GRID)
    for i, v in enumerate(lib.values):
        axes[0].text(v, i, f" {v:,}", va="center", fontsize=7)

    lab = rq5[(~rq5["is_unknown"]) & rq5["comparable"]].head(12).iloc[::-1]
    y = np.arange(len(lab))
    axes[1].barh(y - 0.2, lab["base_relation_coverage"] * 100, height=0.38,
                 color=C_ALT, label="显式底座关系覆盖率")
    axes[1].barh(y + 0.2, lab["share_models_with_children"] * 100, height=0.38,
                 color=C_ACCENT, label="有衍生模型的占比")
    axes[1].set_yticks(y, lab["task"], fontsize=8)
    axes[1].set_xlabel("占该任务模型数的百分比（%）")
    axes[1].set_title("(b) 任务生态结构：显式血缘覆盖率 vs 被衍生的比例")
    axes[1].legend(fontsize=8, loc="lower right")
    axes[1].grid(axis="x", color=C_GRID)
    fig.suptitle("Figure 4  库 / 任务生态结构（仅已标注任务，且样本量 ≥ 200）", y=1.0)
    add_footer(fig)
    fig.tight_layout()
    save(fig, 4, "library_task_structure", "库与任务生态结构对比",
         ["gold/analysis/library_trend.csv", "gold/analysis/rq5_task_ecosystem.csv"],
         "unknown 任务已剔除；小样本任务组（n<200）不参与比较")


# ===================== Figure 5 =====================
def fig05_lorenz(cs):
    lc = read_csv("lorenz_curve.csv")
    fig, ax = plt.subplots(figsize=(6, 6))
    styles = {"downloads": (C_MAIN, "-", "downloads（近 30 天下载）"),
              "downloadsAllTime": (C_ACCENT, "-", "downloadsAllTime（历史累计）")}
    for metric, (color, ls, label) in styles.items():
        sub = lc[lc["metric"] == metric]
        g = cs[metric]["gini"]
        ax.plot(sub["cum_model_pct"], sub["cum_download_pct"], color=color, ls=ls, lw=2,
                label=f"{label}  Gini={g:.4f}")
        ax.fill_between(sub["cum_model_pct"], sub["cum_download_pct"],
                        sub["cum_model_pct"], alpha=0.08, color=color)
    ax.plot([0, 1], [0, 1], "--", color="gray", lw=1, label="绝对平均线（Gini=0）")
    ax.set_title("Figure 5  下载量 Lorenz 曲线（两种口径独立绘制，不混合）")
    ax.set_xlabel("累计模型占比（0→1，模型按下载量升序）")
    ax.set_ylabel("累计下载量占比（0→1）")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(color=C_GRID)
    add_footer(fig)
    fig.tight_layout()
    save(fig, 5, "lorenz", "下载量 Lorenz 曲线（双口径）",
         ["gold/analysis/lorenz_curve.csv", "gold/concentration_stats.json"],
         "曲线由频数聚合后的真实累计点构成，未抽样、未插值；"
         "Gini 用频数加权秩公式精确计算")


# ===================== Figure 6 =====================
def fig06_ccdf(ccdf, cs):
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.4))
    for metric, color, name in (("downloads", C_MAIN, "downloads（近 30 天）"),
                                ("downloadsAllTime", C_ACCENT, "downloadsAllTime（累计）")):
        sub = ccdf[ccdf["metric"] == metric]
        axes[0].plot(sub["value"], sub["ccdf"], ".", ms=2.5, color=color, label=name)
    axes[0].set_xscale("log")
    axes[0].set_yscale("log")
    axes[0].set_xlabel("下载量 x（对数轴）")
    axes[0].set_ylabel("P(X ≥ x)（对数轴）")
    axes[0].set_title("(a) CCDF 双对数（仅正取值）")
    axes[0].legend(fontsize=8)
    axes[0].grid(color=C_GRID, alpha=0.6, which="both")

    metrics = ["downloads", "downloadsAllTime"]
    labels = ["downloads", "downloadsAllTime"]
    zero = [cs[m]["zero_value_share"] * 100 for m in metrics]
    top10 = [cs[m]["top_10pct_share"] * 100 for m in metrics]
    x = np.arange(len(metrics))
    axes[1].bar(x - 0.2, zero, width=0.38, color="#9ca3af", label="零下载模型占比")
    axes[1].bar(x + 0.2, top10, width=0.38, color=C_MAIN, label="Top-10% 模型下载占比")
    axes[1].set_xticks(x, labels)
    axes[1].set_ylabel("百分比（%）")
    axes[1].set_ylim(0, 105)
    axes[1].set_title("(b) 零值占比 vs 头部集中度")
    axes[1].legend(fontsize=8, loc="lower right")
    axes[1].grid(axis="y", color=C_GRID)
    for i, (z, t) in enumerate(zip(zero, top10)):
        axes[1].text(i - 0.2, z + 1.5, f"{z:.1f}%", ha="center", fontsize=8)
        axes[1].text(i + 0.2, t + 1.5, f"{t:.1f}%", ha="center", fontsize=8)
    fig.suptitle("Figure 6  下载量长尾形态（只描述形态，未做幂律拟合，不宣称幂律分布）", y=1.0)
    add_footer(fig)
    fig.tight_layout()
    save(fig, 6, "ccdf_longtail", "下载量长尾与头部集中度",
         ["gold/analysis/ccdf.csv", "gold/concentration_stats.json"],
         "Top-p 份额按值降序取前 max(1,int(n·p)) 个模型，并列值整体同权，结果可复现")


# ===================== Figure 7 =====================
# relation → 颜色：**全局统一语义**。任何图里出现 relation 都必须复用这张表，
# 保证「同一颜色 = 同一语义」跨图成立。relation 信息不足时**不得**按模型名猜类别。
RELATION_ORDER = ["finetune", "adapter", "quantized", "merge"]
RELATION_COLORS = {
    "finetune": "#2563eb",
    "adapter": "#0f9d58",
    "quantized": "#f59e0b",
    "merge": "#8b5cf6",
}
RELATION_CN = {
    "finetune": "微调 finetune",
    "adapter": "适配器 adapter",
    "quantized": "量化 quantized",
    "merge": "合并 merge",
}
# 代表性子图规模：每个 relation 取下载累计最高的 k 个 → 4 类 × k 个，落在 20–50 区间内。
FAMILY_PER_RELATION_K = 10
# 代表性基础模型：按「top-10 底座中 relation 构成最均衡」挑选（见 docs/visual_quality_review.md）。
# 不使用 FLUX.1-dev / Qwen1.5-0.5B 这类子模型最多的底座：它们 98%+ 是单一 relation，
# 4 色图例会退化成单色图，等于给出误导性的分类视觉。
FAMILY_PANEL_BASES = ["meta-llama/Llama-3.1-8B-Instruct", "Qwen/Qwen3-4B-Instruct-2507"]


def load_lineage_edges():
    """读取 Silver 的**显式底座关系边表**（3 列 / 919,839 行 / 26 MB）。

    这是全工程唯一带 `relation` 字段的边表，也是 Figure 7 全部绘制关系的**唯一来源**。
    它足够小（26 MB），pandas 直接读即可，**不需要启动 Spark**，属于「小规模可视化子图」，
    不会触发完整分析流水线。
    """
    path = os.path.join(SILVER, "model_base_edges")
    if not os.path.isdir(path):
        return None
    return pd.read_parquet(path, columns=["src", "dst", "relation"])


def select_family_children(edges, downloads, base, per_relation_k):
    """确定性选取「代表性子模型」：每种 relation 各取 downloadsAllTime 前 k 个。

    规则完全确定、可复现，**不含任何随机抽样**：
      1) 按 relation 分组，组内按 downloadsAllTime 降序；
      2) 并列值以 dst 字典序升序稳定决胜（同一输入永远得到同一输出）；
      3) 某 relation 不足 k 个时**全部保留**，不跨类型补齐、不随机凑数。
    这样每种关系类型都有代表，颜色图例才真实可信，也避免「为好看而挑结构」。

    返回 (选中子集 DataFrame[src,dst,relation,downloadsAllTime], {relation: 该类总数})。
    """
    sub = edges[edges["src"] == base].copy()
    if len(sub) == 0:
        return sub.assign(downloadsAllTime=pd.Series(dtype="int64")), {}
    sub["downloadsAllTime"] = sub["dst"].map(downloads).fillna(0).astype("int64")
    totals = {str(r): int(c) for r, c in sub["relation"].value_counts().items()}
    picked = []
    for rel in RELATION_ORDER:
        g = sub[sub["relation"] == rel]
        if len(g) == 0:
            continue
        g = g.sort_values(["downloadsAllTime", "dst"], ascending=[False, True])
        picked.append(g.head(per_relation_k))
    out = pd.concat(picked, ignore_index=True) if picked else sub.iloc[0:0]
    return out, totals


def label_names(ids, width=18):
    """把模型 ID 压成可读标签；若不同 ID 会压成同名，则加作者前缀消歧。

    必须消歧：把两个不同模型标成同一个名字，等于在图里制造「模型 ID 混淆」，
    属于明确的正确性错误，不能为了排版好看而容忍。
    """
    tails = [str(i).split("/")[-1] for i in ids]
    clipped = [t if len(t) <= width else t[: width - 1] + "…" for t in tails]
    if len(set(clipped)) == len(clipped):
        return dict(zip(ids, clipped))
    out = {}
    for mid, tail in zip(ids, tails):
        author = str(mid).split("/")[0][:10]
        s = f"{author}/{tail}"
        out[mid] = s if len(s) <= width + 8 else s[: width + 7] + "…"
    return out


def draw_stat_cards(ax, cards):
    """Panel A：把 (数值, 标签, 口径说明) 画成纵向统计卡片。

    刻意**不用条形长度**编码：四项指标量纲互不相同（边数 / 节点数 / 分量规模 /
    单模型衍生数），用长度对比会诱导读者比较「谁更大」，属于误导性视觉。
    """
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    n = max(1, len(cards))
    h = 1.0 / n
    for i, (value, label, note) in enumerate(cards):
        top = 1.0 - i * h
        bottom = top - h
        pad = h * 0.09
        ax.add_patch(FancyBboxPatch((0.02, bottom + pad), 0.96, h - 2 * pad,
                                    boxstyle="round,pad=0.006,rounding_size=0.02",
                                    linewidth=1.0, edgecolor="#e5e7eb",
                                    facecolor="#f8fafc"))
        ax.text(0.07, bottom + h * 0.68, value, fontsize=18, fontweight="bold",
                color="#111827", va="center")
        ax.text(0.07, bottom + h * 0.42, label, fontsize=9.6, color="#374151", va="center")
        ax.text(0.07, bottom + h * 0.21, note, fontsize=7.0, color="#6b7280", va="center")


def draw_family_subgraph(ax, base, children, total_children, label_width=22):
    """Panel B：单个代表基础模型的 1 跳「枢纽—辐条」子图。

    只画**边表里真实存在**的 base→derived 边，绝不凭可达性补一条并不存在的边。
    子图只覆盖部分子模型，因此标题显式标注 Representative Subgraph / 代表性局部子图。
    节点大小与深浅编码的是**角色**（底座 vs 衍生），不是 children_count——
    每个面板只有一个底座，用它编码家族规模没有信息量，硬编码会误导读者。
    """
    kids = children.copy()
    kids["_rank"] = kids["relation"].map({r: i for i, r in enumerate(RELATION_ORDER)})
    kids = (kids.sort_values(["_rank", "downloadsAllTime", "dst"],
                             ascending=[True, False, True]).reset_index(drop=True))
    n = len(kids)
    ang = np.linspace(np.pi / 2, np.pi / 2 + 2 * np.pi, n, endpoint=False)
    cx, cy = np.cos(ang), np.sin(ang)
    for x, y in zip(cx, cy):
        ax.annotate("", xy=(x, y), xytext=(0.0, 0.0),
                    arrowprops=dict(arrowstyle="-|>", color="#9ca3af",
                                    lw=0.7, alpha=0.9, shrinkA=0, shrinkB=5))
    ax.scatter(cx, cy, s=30, c=[RELATION_COLORS[r] for r in kids["relation"]],
               edgecolors="white", linewidths=0.5, zorder=3)
    ax.scatter([0.0], [0.0], s=360, c="#111827", edgecolors="white",
               linewidths=1.4, zorder=4)
    # 只标注「每类 relation 中下载累计最高的 1 个」，且放在圆周**内部**（半径 0.55）：
    #   ① 这 4 个点分属 4 个扇区、方向互相垂直，标签天然不重叠；
    #   ② 向外延伸时，左面板的右侧标签与右面板的左侧标签会互相压字（实测确实重叠），
    #      内部放置使标签始终落在本面板坐标系内，跨面板不可能再撞车；
    #   ③ 白色底衬遮挡穿过的辐条，保证文字可读。
    # 消歧只在**实际标注的这 4 个**之间做，避免被未标注节点的重名牵连而全体加前缀。
    picks = []
    for rel in RELATION_ORDER:
        grp = kids[kids["relation"] == rel]
        if len(grp) == 0:
            continue
        picks.append(int(grp.sort_values(["downloadsAllTime", "dst"],
                                         ascending=[False, True]).index[0]))
    names = label_names([kids.at[i, "dst"] for i in picks], width=label_width)
    for pick in picks:
        ax.text(cx[pick] * 0.55, cy[pick] * 0.55, names[kids.at[pick, "dst"]],
                fontsize=7.2, color="#374151", ha="center", va="center", zorder=5,
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.85, pad=1.2))
    ax.set_xlim(-1.40, 1.40)
    ax.set_ylim(-1.62, 1.48)
    ax.set_aspect("equal")
    ax.axis("off")
    # 标题拆成 3 行：单行「代表性局部子图 Representative Subgraph · 展示 N / M 个…」
    # 在 9.5pt 下宽约 3.7 in，两个面板的副标题会在中间互相压字（实测已重叠）。
    ax.set_title(f"{base}\n代表性局部子图 Representative Subgraph\n"
                 f"展示 {n} / {total_children:,} 个直接衍生模型", fontsize=9.5, pad=6)
    ax.text(-1.36, -1.58, "仅含边表中真实存在的 base→derived 边，未补任何可达性边",
            fontsize=6.6, color="#6b7280")


def fig07_family_network(graph_stats):
    """Figure 7：完整谱系统计 + 代表性模型家族（左右双面板）。

    旧版把最大弱连通分量的 Top-1500 节点画成力导向网络：节点过密、只有一种颜色、
    边过浅、标签重叠、缩到 A4 单栏后完全不可读，也无法比较不同底座的衍生生态。
    现改为：
      Panel A  全量图统计（边数 / 参与节点 / 最大弱连通分量 / 最大直接衍生数）
      Panel B  两个代表性基础模型的 1 跳子图，按 relation 着色并共享图例
    Panel A 的数字全部读自 results_manifest.json；Panel B 的关系全部读自 Silver 边表。
    **不硬编码、不随机抽样、不把子图统计写成全量统计。**
    """
    metrics = load_manifest()
    edges = load_lineage_edges()
    nm = read_parquet_cols("analysis/node_metrics",
                           ["model_id", "downloadsAllTime", "children_count"])
    if not metrics or edges is None or nm is None:
        print("[fig] 跳过 Figure 7：缺 results_manifest.json / 边表 / node_metrics")
        return
    viz = manifest_value(metrics, "rq3_gf.viz_subgraph") or {}
    n_edges = manifest_value(metrics, "rq3.n_edges")
    n_nodes = manifest_value(metrics, "rq3.n_nodes_involved")
    comp_size = viz.get("component_size")
    max_children = manifest_value(metrics, "rq3.max_children_count")
    if None in (n_edges, n_nodes, comp_size, max_children):
        print("[fig] 跳过 Figure 7：Panel A 的全量图统计缺件（manifest 未提供）")
        return
    cards = [
        (f"{n_edges:,}", "显式模型继承关系数（边）",
         "Silver 边表 model_base_edges 全量计数，base→derived，方向由 baseModels 决定"),
        (f"{n_nodes:,}", "参与谱系网络的模型数（节点）",
         "(src ∪ dst) 去重后节点数，非「父 + 子」直接相加（那样会重复计数）"),
        (f"{comp_size:,}", "最大弱连通分量节点数",
         "GraphFrames connectedComponents 在「全量图」上的结果，非子图"),
        (f"{max_children:,}", "最大直接衍生模型数（children_count）",
         "单个底座模型的 1 跳直接子模型数上界，不等于家族总规模"),
    ]

    dl = nm.set_index("model_id")["downloadsAllTime"].fillna(0).astype("int64")
    panels = []
    for base in FAMILY_PANEL_BASES:
        sel, totals = select_family_children(edges, dl, base, FAMILY_PER_RELATION_K)
        if len(sel) == 0:
            print(f"[fig] Figure 7 Panel B 跳过 {base}：边表中无该底座的子模型")
            continue
        panels.append((base, sel, int(sum(totals.values()))))
    if len(panels) == 0:
        print("[fig] 跳过 Figure 7：没有任何可绘制的代表性子图")
        return

    fig = plt.figure(figsize=(13.2, 7.4))
    grid = fig.add_gridspec(1, 1 + len(panels), width_ratios=[1.05] + [1.0] * len(panels),
                            wspace=0.22, left=0.025, right=0.985, top=0.81, bottom=0.15)
    ax_a = fig.add_subplot(grid[0, 0])
    draw_stat_cards(ax_a, cards)
    ax_a.set_title("Panel A  全量谱系网络统计\nGlobal Lineage Statistics (full graph)",
                   fontsize=11, color="#111827", pad=10)
    for j, (base, sel, total) in enumerate(panels):
        ax_b = fig.add_subplot(grid[0, 1 + j])
        draw_family_subgraph(ax_b, base, sel, total)

    rel_handles = [plt.Line2D([], [], marker="o", ls="", ms=6, color=RELATION_COLORS[r],
                              label=RELATION_CN[r]) for r in RELATION_ORDER]
    role_handles = [
        plt.Line2D([], [], marker="o", ls="", ms=10, color="#111827",
                   label="底座模型 base（深色大节点）"),
        plt.Line2D([], [], marker="o", ls="", ms=5, color="#94a3b8",
                   label="衍生模型 derived（浅色小节点，颜色 = relation）"),
    ]
    fig.legend(handles=rel_handles, loc="lower left", bbox_to_anchor=(0.02, 0.008),
               ncol=4, fontsize=8.5, frameon=False,
               title="边 / 节点颜色 = 关系类型 relation（全图统一语义）", title_fontsize=8.5)
    fig.legend(handles=role_handles, loc="lower right", bbox_to_anchor=(0.985, 0.008),
               ncol=1, fontsize=7.6, frameon=False)
    fig.suptitle("Figure 7  模型谱系结构：全量图统计 + 代表性模型家族"
                 "（右侧为代表性局部子图，非完整继承关系）", fontsize=12.5, y=0.955)
    add_footer(fig)
    save(fig, 7, "family_network", "模型谱系结构：全量统计 + 代表性家族子图",
         ["gold/analysis/results_manifest.json", "silver/model_base_edges",
          "gold/analysis/node_metrics", "gold/analysis/graphframes_result.json"],
         f"Panel A 为**全量图**统计（{n_edges:,} 边 / {n_nodes:,} 节点 / "
         f"最大弱连通分量 {comp_size:,} 节点 / 最大直接衍生 {max_children:,}）；"
         f"Panel B 为代表性局部子图：每个底座按 relation 各取 downloadsAllTime 前 "
         f"{FAMILY_PER_RELATION_K} 个（不足则全取），非完整继承关系，"
         f"不可解释为完整弱连通分量。节点大小/深浅编码角色而非 children_count。")


def fig07_legacy_network(graph_stats):
    """旧版 Figure 7（最大弱连通分量 Top-1500 力导向网络）——保留为**补充材料**。

    保留原因：用户要求不删除旧图。它仍能反映最大弱连通分量的整体形态，
    但节点密度过高、无分类着色、标签重叠，缩到 A4 单栏不可读，
    因此移到 figures_supplementary/ 并明确标注抽样方法与局限，不再作为正式图。
    """
    path = os.path.join(ANALYSIS, "family_graph.json")
    if not os.path.exists(path):
        print("[fig] 跳过旧版 Figure 7：缺 family_graph.json")
        return
    with open(path, encoding="utf-8") as f:
        gj = json.load(f)
    node_ids = list(gj.get("nodes") or [])
    G = nx.DiGraph()
    G.add_nodes_from(node_ids)
    for e in gj.get("links") or []:
        G.add_edge(e["src"], e["dst"])
    if len(G) == 0:
        print("[fig] 跳过旧版 Figure 7：子图为空")
        return
    nm = read_parquet_cols("analysis/node_metrics", ["model_id", "children_count"])
    if nm is None or "children_count" not in nm.columns:
        print("[fig] 跳过旧版 Figure 7：缺 node_metrics")
        return
    sub = nm[nm["model_id"].isin(set(G.nodes))]
    true_children = dict(zip(sub["model_id"], sub["children_count"]))
    mx = max(1, max(int(v) for v in true_children.values()) if true_children else 1)
    sizes = [30 + 420 * (int(true_children.get(n, 0)) / mx) ** 0.5 for n in G.nodes]
    pos = nx.spring_layout(G, seed=42, k=0.5, iterations=60)
    fig, ax = plt.subplots(figsize=(10.5, 7.5))
    nx.draw_networkx_edges(G, pos, ax=ax, alpha=0.3, arrows=True, arrowsize=7,
                           edge_color="#9ca3af", width=0.6)
    nx.draw_networkx_nodes(G, pos, ax=ax, node_size=sizes, node_color=C_MAIN,
                           alpha=0.85, edgecolors="white", linewidths=0.5)
    hubs = sorted(G.nodes, key=lambda n: -int(true_children.get(n, 0)))[:8]
    nx.draw_networkx_labels(G, pos, labels={h: h.split("/")[-1][:16] for h in hubs},
                            font_size=7, ax=ax)
    ax.set_title(
        f"旧版（补充材料）模型家族网络子图\n最大弱连通分量 {gj.get('component_size', 0):,} 个节点，"
        f"此处只画其中度数最高的 {len(G):,} 个节点（已截断）；节点过密、无分类着色，"
        f"缩小后不可读，仅供追溯", fontsize=9)
    ax.axis("off")
    add_footer(fig)
    fig.tight_layout()
    save_supp(fig, "fig07_global_network_legacy",
              "旧版模型家族网络子图（补充材料，非正式图）")


# ===================== Figure 7 报告版（A4 单栏） =====================
# 以下两个函数生成 Figure 7 的 A4 单栏拆分版，专为 LaTeX 插图设计。
# 不覆盖 figures_final/fig07_family_network.*（PPT/Dashboard 用）。

# 7b 报告版只选一个底座：Llama-3.1-8B-Instruct 在现有正式 Figure 7 中
# 4 类 relation 分布最均衡、标签最短可读，适合单栏空间。
FIG07B_REPORT_BASE = "meta-llama/Llama-3.1-8B-Instruct"
# 7b 报告版每类 relation 取前 5 个 → 总计约 20 个节点，减少标签拥挤
FIG07B_REPORT_PER_RELATION_K = 5


def fig07a_report_global_lineage():
    """Figure 7a 报告版：完整模型谱系统计（2×2 紧凑统计卡片）。

    面向 A4 单栏正文（约 15–16 cm 插图宽度），只展示四项已验证指标：
      1. 显式模型继承边数（Silver 边表全量计数）
      2. 参与谱系网络的模型节点数（src∪dst 去重）
      3. 最大弱连通分量节点数（GraphFrames connectedComponents）
      4. 最大直接衍生模型数（children_count 上界）

    全部数值从 results_manifest.json 读取，不硬编码。
    与正式 Figure 7 Panel A 的数据完全一致，但采用 2×2 布局而非纵向 1×4，
    以适应单栏宽高比。
    """
    metrics = load_manifest()
    if not metrics:
        print("[fig-report] 跳过 Figure 7a：缺 results_manifest.json")
        return
    viz = manifest_value(metrics, "rq3_gf.viz_subgraph") or {}
    n_edges = manifest_value(metrics, "rq3.n_edges")
    n_nodes = manifest_value(metrics, "rq3.n_nodes_involved")
    comp_size = viz.get("component_size")
    max_children = manifest_value(metrics, "rq3.max_children_count")
    if None in (n_edges, n_nodes, comp_size, max_children):
        print("[fig-report] 跳过 Figure 7a：Panel A 全量图统计缺件（manifest 未提供）")
        return

    cards = [
        (f"{n_edges:,}", "显式模型继承关系数（边）",
         "Silver 边表 model_base_edges\n全量计数，base→derived"),
        (f"{n_nodes:,}", "参与谱系网络的模型数（节点）",
         "(src ∪ dst) 去重后节点数"),
        (f"{comp_size:,}", "最大弱连通分量节点数",
         "GraphFrames connectedComponents\n在全量图上的结果"),
        (f"{max_children:,}", "最大直接衍生模型数",
         "单个底座的 1 跳直接子模型数上界\n不等于家族总规模"),
    ]

    # A4 单栏宽度约 15–16 cm → 约 6.0–6.3 inch；
    # 2×2 布局，高度约 4.5 inch，宽高比适合单栏插图。
    fig, axes = plt.subplots(2, 2, figsize=(6.2, 4.6))
    for ax, (value, label, note) in zip(axes.flat, cards):
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")
        ax.add_patch(FancyBboxPatch((0.03, 0.06), 0.94, 0.88,
                                    boxstyle="round,pad=0.008,rounding_size=0.03",
                                    linewidth=0.8, edgecolor="#e5e7eb",
                                    facecolor="#f8fafc"))
        ax.text(0.50, 0.72, value, fontsize=15, fontweight="bold",
                color="#111827", ha="center", va="center")
        ax.text(0.50, 0.44, label, fontsize=8.2, color="#374151",
                ha="center", va="center")
        ax.text(0.50, 0.18, note, fontsize=6.2, color="#6b7280",
                ha="center", va="center", linespacing=1.3)

    fig.suptitle("Figure 7a  全量模型谱系网络统计\n"
                 "Global Lineage Statistics (full graph)",
                 fontsize=10, y=0.98, color="#111827")
    add_footer(fig)
    fig.tight_layout(rect=[0, 0.03, 1, 0.92])
    save_report(fig, "fig07a_global_lineage",
                "Figure 7a 报告版：全量谱系网络统计（2×2 卡片，A4 单栏）")


def fig07b_report_representative_family():
    """Figure 7b 报告版：单个底座模型的代表性家族 hub-and-spoke 子图（A4 单栏）。

    面向 A4 单栏正文，只展示一个底座模型（Llama-3.1-8B-Instruct）及部分真实衍生模型。
    每类 relation 取 downloadsAllTime 前 5 个 → 总计约 20 个节点。

    与正式 Figure 7 Panel B 的区别：
      - 图幅更窄（6.2 in 宽），适合 A4 单栏；
      - 标签外置在圆周之外并用引导线连接，不穿过中心节点区域；
      - 底座名称完整显示在中心节点下方；
      - 图例说明修正为「节点颜色 = 关系类型」（边本身是灰色，不编码 relation）。

    所有绘制边确实存在于 Silver 边表；边方向为 Base → Derived；
    子节点筛选规则确定、可复现（与正式 Figure 7 Panel B 规则一致，仅 k 更小）。
    """
    metrics = load_manifest()
    edges = load_lineage_edges()
    nm = read_parquet_cols("analysis/node_metrics",
                           ["model_id", "downloadsAllTime"])
    if not metrics or edges is None or nm is None:
        print("[fig-report] 跳过 Figure 7b：缺 results_manifest.json / 边表 / node_metrics")
        return

    dl = nm.set_index("model_id")["downloadsAllTime"].fillna(0).astype("int64")
    base = FIG07B_REPORT_BASE
    sel, totals = select_family_children(edges, dl, base, FIG07B_REPORT_PER_RELATION_K)
    if len(sel) == 0:
        print(f"[fig-report] 跳过 Figure 7b：边表中无 {base} 的子模型")
        return

    n_kids = len(sel)
    total_children = int(sum(totals.values()))

    # ---- 独立布局（不复用 draw_family_subgraph，避免标题冲突和标签位置不适合单栏）----
    kids = sel.copy()
    kids["_rank"] = kids["relation"].map({r: i for i, r in enumerate(RELATION_ORDER)})
    kids = kids.sort_values(["_rank", "downloadsAllTime", "dst"],
                            ascending=[True, False, True]).reset_index(drop=True)
    n = len(kids)
    ang = np.linspace(np.pi / 2, np.pi / 2 + 2 * np.pi, n, endpoint=False)
    cx, cy = np.cos(ang), np.sin(ang)

    fig, ax = plt.subplots(figsize=(6.2, 4.7))
    # 边（灰色，不编码 relation）
    for x, y in zip(cx, cy):
        ax.annotate("", xy=(x, y), xytext=(0.0, 0.0),
                    arrowprops=dict(arrowstyle="-|>", color="#9ca3af",
                                    lw=0.7, alpha=0.85, shrinkA=0, shrinkB=5))
    # 衍生节点（颜色 = relation）
    ax.scatter(cx, cy, s=36, c=[RELATION_COLORS[r] for r in kids["relation"]],
               edgecolors="white", linewidths=0.5, zorder=3)
    # 底座节点（深色大圆）
    ax.scatter([0.0], [0.0], s=400, c="#111827", edgecolors="white",
               linewidths=1.5, zorder=4)

    # ---- 外置标签 + 引导线 ----
    # 每类 relation 只标注 downloadsAllTime 最高的 1 个节点（共 ≤ 4 个标签）。
    # 标签放在圆周外侧（半径 1.25），用细引导线从节点连到标签。
    picks = []
    for rel in RELATION_ORDER:
        grp = kids[kids["relation"] == rel]
        if len(grp) == 0:
            continue
        picks.append(int(grp.sort_values(["downloadsAllTime", "dst"],
                                         ascending=[False, True]).index[0]))
    names = label_names([kids.at[i, "dst"] for i in picks], width=16)
    for pick in picks:
        nx_pos, ny_pos = cx[pick], cy[pick]
        # 标签位置：沿节点方向外推到半径 1.25
        label_r = 1.25
        lx, ly = nx_pos * label_r, ny_pos * label_r
        # 引导线：从节点边缘到标签框边缘
        ax.annotate("",
                    xy=(lx, ly), xytext=(nx_pos, ny_pos),
                    arrowprops=dict(arrowstyle="-", color="#9ca3af", lw=0.5, alpha=0.7),
                    zorder=2)
        # 标签文本（白色底衬）
        ha = "left" if lx >= 0 else "right"
        va = "bottom" if ly >= 0 else "top"
        # 微调：让标签稍微偏离引导线末端，避免与节点重叠
        offset_x = 0.08 if lx >= 0 else -0.08
        offset_y = 0.06 if ly >= 0 else -0.06
        ax.text(lx + offset_x, ly + offset_y,
                names[kids.at[pick, "dst"]],
                fontsize=6.8, color="#374151", ha=ha, va=va, zorder=6,
                bbox=dict(facecolor="white", edgecolor="#d1d5db",
                          alpha=0.92, pad=1.5, boxstyle="round,pad=0.15"))

    # ---- 底座名称：中心节点下方，完整可读，下移避免与下方节点簇重叠 ----
    ax.text(0.0, -0.38, base, fontsize=7.5, color="#111827",
            ha="center", va="top", zorder=10,
            bbox=dict(facecolor="white", edgecolor="none", alpha=0.92, pad=1.5))
    ax.text(0.0, -0.50, "（底座模型 base）", fontsize=6.2, color="#6b7280",
            ha="center", va="top", zorder=10)

    # ---- 坐标范围：留出外置标签空间 ----
    ax.set_xlim(-1.55, 1.55)
    ax.set_ylim(-1.45, 1.45)
    ax.set_aspect("equal")
    ax.axis("off")

    # ---- 单一标题 ----
    fig.suptitle(
        f"Figure 7b  代表性模型家族子图\n"
        f"{base}  ·  展示 {n_kids} / {total_children:,} 个直接衍生模型"
        f"（每类 relation 取 downloadsAllTime 前 {FIG07B_REPORT_PER_RELATION_K} 个）"
        f"\n代表性局部子图，不展示完整继承关系",
        fontsize=8.0, y=0.96, color="#111827", linespacing=1.35)

    # ---- 图例：修正说明为「节点颜色 = 关系类型」----
    rel_handles = [plt.Line2D([], [], marker="o", ls="", ms=6, color=RELATION_COLORS[r],
                              label=RELATION_CN[r]) for r in RELATION_ORDER]
    role_handles = [
        plt.Line2D([], [], marker="o", ls="", ms=9, color="#111827",
                   label="底座模型 base（深色大节点）"),
        plt.Line2D([], [], marker="o", ls="", ms=5, color="#94a3b8",
                   label="衍生模型 derived（小节点，颜色 = relation）"),
    ]
    fig.legend(handles=role_handles + rel_handles, loc="lower center",
               bbox_to_anchor=(0.5, 0.01), ncol=3, fontsize=7.0, frameon=False,
               title="节点颜色 = 关系类型 relation（边为灰色，不编码 relation）",
               title_fontsize=7.0)

    add_footer(fig)
    fig.tight_layout(rect=[0, 0.11, 1, 0.86])
    save_report(fig, "fig07b_representative_family",
                "Figure 7b 报告版：代表性家族子图（A4 单栏，外置标签）")


# ===================== Figure 8 =====================
def fig08_family_size(graph_stats):
    fs = read_parquet("model_family_stats")
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.6))
    fs = fs.sort_values("family_size")
    axes[0].bar(fs["family_size"].astype(str), fs["num_models"], color=C_MAIN)
    axes[0].set_yscale("log")
    axes[0].set_title("(a) direct_family_size 分布（= children_count + 1，1 跳直系）")
    axes[0].set_xlabel("direct_family_size（含自身）")
    axes[0].set_ylabel("模型数（对数轴）")
    axes[0].grid(axis="y", color=C_GRID)
    nz = fs[fs["family_size"] > 1]
    if len(nz):
        axes[0].text(0.98, 0.92, f"family_size=1（无衍生）: "
                                 f"{int(fs.loc[fs['family_size'] == 1, 'num_models'].sum()):,}",
                     transform=axes[0].transAxes, ha="right", fontsize=8, color=C_ACCENT)

    cs = read_csv("component_size_dist.csv")
    if cs is not None and len(cs):
        axes[1].loglog(cs["component_size"], cs["num_components"], "o", ms=4, color=C_ALT)
        axes[1].set_title("(b) 弱连通分量规模分布（GraphFrames connectedComponents）")
        axes[1].set_xlabel("连通分量规模（节点数，对数轴）")
        axes[1].set_ylabel("分量个数（对数轴）")
        axes[1].grid(color=C_GRID, alpha=0.6, which="both")
    else:
        axes[1].text(0.5, 0.5, "缺少 component_size_dist.csv\n（GraphFrames 未完成）",
                     ha="center", va="center", fontsize=10, color="#6b7280")
        axes[1].set_axis_off()
    fig.suptitle("Figure 8  家族规模 vs 连通分量规模（两者含义不同，不可互相替代）", y=1.0)
    add_footer(fig)
    fig.tight_layout()
    save(fig, 8, "family_size", "家族规模与连通分量规模分布",
         ["gold/model_family_stats", "gold/analysis/component_size_dist.csv",
          "gold/analysis/graph_stats.json"],
         f"最大 children_count = {graph_stats['max_children_count']:,}；"
         "direct_family_size 只是 1 跳直系规模，不等于连通分量规模，也不等于可达后代数")


# ===================== Figure 9 =====================
def fig09_ml(ml):
    # 特征重要性是 Spark 写出的**目录式** CSV（ml_feature_importance_<model>/），
    # 不是单个 .csv 文件；必须走 read_spark_csv 合并 part 分片。
    imp = read_spark_csv("ml_feature_importance_randomforest")
    if imp is None:
        imp = read_spark_csv("ml_feature_importance_gbt")
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.6))
    if imp is not None:
        imp = imp.sort_values("importance")
        axes[0].barh(imp["feature"], imp["importance"], color=C_MAIN)
        axes[0].set_title("(a) 特征重要性（RandomForest，目标 log(1+downloads)）")
        axes[0].set_xlabel("重要性（one-hot 分列按源列汇总）")
        axes[0].grid(axis="x", color=C_GRID)
        axes[0].tick_params(axis="y", labelsize=8)
    else:
        axes[0].text(0.5, 0.5, "缺少 ml_feature_importance_*/（Spark 目录式 CSV）",
                     ha="center", va="center", fontsize=10, color="#6b7280")
        axes[0].set_axis_off()

    rows = list(ml["metrics_on_test"].items())
    names = [r[0] for r in rows]
    r2 = [r[1]["r2"] for r in rows]
    rmse = [r[1]["rmse"] for r in rows]
    x = np.arange(len(names))
    axes[1].bar(x - 0.2, rmse, width=0.38, color=C_MAIN, label="RMSE")
    axes[1].bar(x + 0.2, r2, width=0.38, color=C_ALT, label="R²")
    axes[1].set_xticks(x, names, fontsize=8, rotation=15)
    axes[1].set_ylabel("指标值（RMSE 与 R² 同轴仅便于并列展示）")
    axes[1].set_title("(b) 独立测试集上的模型对比")
    axes[1].legend(fontsize=8)
    axes[1].grid(axis="y", color=C_GRID)
    for i, (a, b) in enumerate(zip(rmse, r2)):
        axes[1].text(i - 0.2, a, f"{a:.2f}", ha="center", fontsize=7)
        axes[1].text(i + 0.2, b, f"{b:.2f}", ha="center", fontsize=7)
    fig.suptitle("Figure 9  MLlib 特征重要性与模型对比（先划分后拟合，指标只在测试集上计算）",
                 y=1.0)
    add_footer(fig)
    fig.tight_layout()
    save(fig, 9, "ml_importance", "特征重要性与模型对比",
         ["gold/analysis/ml_feature_importance_randomforest.csv",
          "gold/analysis/ml_summary.json"],
         f"训练 {ml['split']['n_train']:,} / 测试 {ml['split']['n_test']:,} 行，"
         f"seed={ml['split']['seed']}，train∩test={ml['split']['train_test_overlap']}；"
         "likes 与 downloads 同取自一个快照，存在同期相关，不代表前瞻预测能力")


# ===================== Figure 10–12（benchmark）=====================
def mem_pressure_mask(df):
    """布尔掩码：该行是否被标记为受内存压力影响。

    换页会让耗时偏大且不可复现，画进图里就是给出错误的性能结论，
    因此所有 benchmark 图都必须先把这些行剔掉（它们只在报告 §3.2 里作为资源约束证据列出）。
    """
    if "mem_pressure" not in df.columns:
        return pd.Series(False, index=df.index)
    return df["mem_pressure"].astype(str).str.strip().str.lower().isin(["true", "1", "yes"])


def real_scale_sweep(summary):
    """挑出"真实数据规模扫描"那一组（P1）的汇总行，并剔除内存压力行。

    `benchmark_all_summary.csv` 里混着三组实验：P1 规模扫描（10/25/50/100%）、
    P2 并行度扫描（local[2]/[4]/[6]，规模档也叫 real_50pct）、P3 合成数据。
    若不过滤，Figure 10 会在同一个 n_rows 上出现多个 master 的点、曲线来回折，
    Figure 11/12 的 50% 档也会混入不同核数的结果。
    这里用 `run_tag` 隔离：**拥有最多不同 n_rows 档位**的那一组就是规模扫描
    （P1 有 5 档：10/25/50/100% + 冷读哨兵 real_cold_touch；P2 只有 2 档），
    无需硬编码 tag 名称。
    """
    real = summary[summary["data_kind"] == "hubstats_models_parquet"]
    real = real[~mem_pressure_mask(real)]
    if real.empty or "run_tag" not in real.columns:
        return real
    counts = real.groupby("run_tag")["n_rows"].nunique()
    return real[real["run_tag"] == counts.idxmax()]


def fig10_scale_runtime(summary):
    sub = real_scale_sweep(summary)
    if sub.empty:
        print("[fig] 跳过 Figure 10：缺真实数据 benchmark")
        return
    parts = sorted(sub["shuffle_partitions"].unique())
    sub = sub[sub["shuffle_partitions"] == parts[-1]]
    # 只画覆盖了 ≥2 个规模档的算子，否则单点"曲线"没有意义
    keep = [op for op, g in sub.groupby("op") if g["n_rows"].nunique() >= 2]
    sub = sub[sub["op"].isin(keep)]
    fig, ax = plt.subplots(figsize=(9, 5))
    for op, grp in sub.groupby("op"):
        grp = grp.sort_values("n_rows")
        ax.plot(grp["n_rows"], grp["seconds_median"], "o-", ms=4, lw=1.4, label=op)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("输入行数（对数轴）")
    ax.set_ylabel("中位执行时间（秒，对数轴）")
    ax.set_title(f"Figure 10  真实数据：执行时间 vs 数据规模"
                 f"（shuffle.partitions={parts[-1]} 固定，{int(sub['repeats'].max())} 次取中位）")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(color=C_GRID, alpha=0.6, which="both")
    add_footer(fig)
    fig.tight_layout()
    save(fig, 10, "scale_runtime", "执行时间 vs 数据规模",
         ["gold/analysis/benchmark_all_summary.csv"],
         "各档为同一 crc32 哈希阈值的嵌套子集（10%⊂25%⊂50%⊂100%）且**已物化成独立 Parquet 文件**，"
         "差异只来自数据量；Windows 无法清 OS page cache，除首次接触外均为缓存热态")


def top_scale_label(sub):
    """挑出「最大规模、且**做过分区扫描**」的规模档标签。

    不能简单取 `n_rows` 最大的 scale_label：P1 里还有一行哨兵档
    `real_cold_touch`（物化子集之前的首次冷读，n_rows = Bronze 全量 3,138,959），
    它比去重后的 `real_100pct`（3,138,954）**还大 5 行**，且只有 1 个分区档。
    直接按 n_rows 取最大就会选中它，后果是：
      * Figure 11 的分区对比变成空集，被静默跳过（少一张图）；
      * Figure 12 只画得出 `parquet_scan_first_touch` 一个算子。
    这里改为「在至少有两个分区档的规模标签里取 n_rows 最大者」。
    """
    swept = [lb for lb, g in sub.groupby("scale_label")
             if g["shuffle_partitions"].nunique() >= 2]
    if not swept:
        return None
    best, best_rows = None, -1
    for lb in swept:
        m = int(sub.loc[sub["scale_label"] == lb, "n_rows"].max())
        if m > best_rows:
            best, best_rows = lb, m
    return best


def fig11_partitions(summary):
    sub = real_scale_sweep(summary)
    if sub.empty:
        print("[fig] 跳过 Figure 11：缺真实数据 benchmark")
        return
    top_scale = top_scale_label(sub)
    if top_scale is None:
        print("[fig] 跳过 Figure 11：没有任何规模档做过 ≥2 个分区数")
        return
    sub = sub[sub["scale_label"] == top_scale]
    # 只保留真正做过多个分区档的算子：其余算子在该规模下只测过一个分区数
    keep = [op for op, g in sub.groupby("op") if g["shuffle_partitions"].nunique() >= 2]
    if not keep:
        print("[fig] 跳过 Figure 11：分区扫描结果不足")
        return
    sub = sub[sub["op"].isin(keep)]
    fig, ax = plt.subplots(figsize=(9, 5))
    ops = sorted(keep)
    width = 0.8 / max(1, len(ops))
    parts = sorted(sub["shuffle_partitions"].unique())
    x = np.arange(len(parts))
    for i, op in enumerate(ops):
        g = sub[sub["op"] == op].set_index("shuffle_partitions").reindex(parts)
        ax.bar(x + i * width, g["seconds_median"], width=width, label=op)
    ax.set_xticks(x + 0.4 - width / 2, [str(p) for p in parts])
    ax.set_xlabel("spark.sql.shuffle.partitions")
    ax.set_ylabel("中位执行时间（秒）")
    ax.set_title(f"Figure 11  分区数对性能的影响（{top_scale}，其他条件固定）")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(axis="y", color=C_GRID)
    add_footer(fig)
    fig.tight_layout()
    save(fig, 11, "partitions_perf", "shuffle 分区数 vs 性能",
         ["gold/analysis/benchmark_all_summary.csv"],
         f"仅在 {top_scale} 上扫描 16/32/64，其余条件（数据、算子、master、driver 内存）全部固定；"
         "本机为 local[N] 单机多核，不得表述为多节点横向扩展")


def fig12_operator_comparison(summary):
    sub = real_scale_sweep(summary)
    if sub.empty:
        print("[fig] 跳过 Figure 12：缺真实数据 benchmark")
        return
    # 与 Figure 11 同一口径挑最大规模档，避免选中只有 1 行的
    # real_cold_touch 哨兵档（那会让本图退化成只画 1 个算子的柱状图）。
    top_scale = top_scale_label(sub)
    if top_scale is None:
        print("[fig] 跳过 Figure 12：没有可用的规模档")
        return
    sub = sub[sub["scale_label"] == top_scale]
    parts = sorted(sub["shuffle_partitions"].unique())
    sub = sub[sub["shuffle_partitions"] == parts[-1]].sort_values("seconds_median")
    fig, ax = plt.subplots(figsize=(9.5, 5))
    y = np.arange(len(sub))
    ax.barh(y, sub["seconds_median"], color=C_MAIN, label="中位执行时间（秒）")
    ax.set_yticks(y, sub["op"], fontsize=8)
    ax.set_xlabel("中位执行时间（秒）")
    ax.set_title(f"Figure 12  算子耗时对比（{top_scale}，shuffle.partitions={parts[-1]}）")
    ax.grid(axis="x", color=C_GRID)
    ax2 = ax.twiny()
    sr = sub["shuffle_read_bytes_median"]
    ax2.plot(sr / 2 ** 20, y, "s", ms=5, color=C_ACCENT, label="shuffle 读（MiB）")
    ax2.set_xlabel("shuffle 读取量（MiB）", color=C_ACCENT)
    ax2.tick_params(axis="x", colors=C_ACCENT)
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, fontsize=8, loc="lower right")
    add_footer(fig)
    fig.tight_layout()
    n_na = int(sr.isna().sum())
    save(fig, 12, "operator_comparison", "算子耗时与 shuffle 量对比",
         ["gold/analysis/benchmark_all_summary.csv"],
         "shuffle 量为 Spark UI REST 的 stage 级 shuffleRead/WriteBytes 之和；"
         f"确实取不到时记为**空值**并在汇总中排除（本图 {n_na} 个算子不可用），"
         "绝不写 0 —— 写 0 等于谎称该算子没有发生 shuffle")


# ===================== Dashboard =====================
def pct(v):
    """百分比格式化；非数值返回 '-'。

    统一保留 **2 位小数**，与 write_reports.py 的 pct(...,2) 一致，
    否则同一指标会在 Dashboard 与报告里出现不同写法（49.4% vs 49.36%）。
    """
    return f"{v:.2%}" if isinstance(v, (int, float)) else "-"


def fmt_opt(v, spec, scale=1.0):
    """可选数值格式化：不可用（None / NaN）显示为 "NA"，绝不编造一个数字填上。

    pandas 读 CSV 时会把空字段变成 NaN 而不是 None，所以两者都要判。
    """
    if v is None or (isinstance(v, float) and v != v):
        return "NA"
    try:
        return format(v / scale, spec)
    except (TypeError, ValueError):
        return "NA"


def html_table(rows, headers):
    """把二维列表渲染成 HTML 表格。"""
    head = "".join(f"<th>{h}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><tr>{head}</tr>{body}</table>"


def img_data_uri(path):
    """把图片读成 data URI（base64），供**单文件**看板使用。

    为什么需要：原 `dashboard_final.html` 用 `src="figures_final/xxx.png"` 相对路径
    引用图片目录，只把 HTML 单独发出去时 12 张图会全部失效。单文件版把图片内嵌，
    拷到任何位置都能打开。代价是文件变大：本工程 12 张 PNG 合计约 3.6 MB，
    base64 后约 4.8 MB，本地打开仍然很快，换来的是「一个文件即可分享」。
    """
    with open(path, "rb") as f:
        return "data:image/png;base64," + base64.b64encode(f.read()).decode("ascii")


# 图表按研究问题分组展示：读者按 RQ 顺序看，而不是按文件名排序看。
FIG_SECTIONS = [
    ("RQ1 · 生态演化", [1, 2, 3]),
    ("RQ2 · 注意力集中与长尾", [5, 6]),
    ("RQ3 · 模型谱系网络", [7, 8]),
    ("RQ4 · 属性与下载热度的统计关联", [9]),
    ("RQ5 · 任务生态对比", [4]),
    ("Spark 扩展性实验（真实数据）", [10, 11, 12]),
]


def figure_cards(embed):
    """按研究问题分组生成图卡片 HTML。

    embed=True 内嵌 base64 图片（单文件分享版）；embed=False 用 `figures_final/`
    相对路径（本地可运行版）。图注取自 `figure_index.json` 的**标题与口径说明**，
    而不是文件名 —— 文件名对读者没有信息量，且无法说明统计范围。

    `loading="lazy"` 只用于**相对路径版**：那一版的图片是外部文件，懒加载能省 IO。
    内嵌版的图片字节本来就在 HTML 里，懒加载省不了任何下载，反而会让
    「打印/导出 PDF」「按锚点跳转后截图」这类场景出现空白（浏览器尚未触发解码，
    布局高度按 0 计算）。因此内嵌版一律立即加载。
    """
    by_num = {}
    for item in FIG_INDEX:
        try:
            by_num[int(str(item.get("figure", "")).split()[1])] = item
        except (IndexError, ValueError):
            continue
    lazy = "" if embed else ' loading="lazy"'
    blocks = []
    for title, nums in FIG_SECTIONS:
        cards = []
        for n in nums:
            item = by_num.get(n)
            if not item:
                continue
            fname = item.get("file", "")
            path = os.path.join(FIG_DIR, fname)
            if not fname or not os.path.exists(path):
                continue
            src = img_data_uri(path) if embed else f"figures_final/{fname}"
            caption = item.get("title", fname)
            note = item.get("note", "")
            note_html = f'<span class="note">{note}</span>' if note else ""
            cards.append(f'<figure><img src="{src}" alt="Figure {n}：{caption}"{lazy}>'
                         f'<figcaption><b>Figure {n}</b> · {caption}{note_html}</figcaption>'
                         f"</figure>")
        if cards:
            blocks.append(f'<h3>{title}</h3><div class="grid">{"".join(cards)}</div>')
    return "".join(blocks)


def kpi_groups_html(groups):
    """把「分组标题 + KPI 卡片」渲染成 HTML，替代原来一长排无分组的 KPI。"""
    out = []
    for title, items in groups:
        cards = "".join(f'<div class="kpi"><b>{v}</b><span>{k}</span></div>' for k, v in items)
        out.append(f'<div class="kpigroup"><h3>{title}</h3><div class="kpis">{cards}</div></div>')
    return "".join(out)


DASH_CSS = """
*{box-sizing:border-box}
body{font-family:"Microsoft YaHei",system-ui,-apple-system,sans-serif;margin:0;
background:#f8fafc;color:#111827;line-height:1.6}
.wrap{max-width:1400px;margin:0 auto;padding:20px 22px 60px}
header.top{background:#fff;border-bottom:1px solid #e5e7eb;padding:18px 22px}
h1{font-size:22px;margin:0 0 4px} h2{font-size:17px;margin:30px 0 10px;
padding-bottom:6px;border-bottom:2px solid #eff6ff}
h3{font-size:14px;margin:20px 0 8px;color:#374151}
.sub{color:#6b7280;font-size:13px}
nav{position:sticky;top:0;z-index:10;background:rgba(255,255,255,.95);
border-bottom:1px solid #e5e7eb;padding:8px 22px;display:flex;flex-wrap:wrap;
gap:6px 14px;font-size:12.5px}
nav a{color:#2563eb;text-decoration:none} nav a:hover{text-decoration:underline}
.kpigroup{margin:14px 0}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(165px,1fr));gap:12px}
.kpi{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:12px 14px}
.kpi b{display:block;font-size:19px;color:#2563eb;line-height:1.25}
.kpi span{font-size:12px;color:#6b7280}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(430px,1fr));gap:16px}
figure{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:10px;margin:0}
figure img{width:100%;height:auto;border-radius:6px;display:block}
figcaption{font-size:12px;color:#374151;padding-top:8px}
figcaption .note{display:block;color:#6b7280;font-size:11.5px;margin-top:3px}
.tablewrap{overflow-x:auto}
table{border-collapse:collapse;width:100%;background:#fff;font-size:12.5px;
margin-bottom:8px;min-width:520px}
th,td{border:1px solid #e5e7eb;padding:6px 9px;text-align:left;white-space:nowrap}
th{background:#eff6ff}
.note{font-size:12.5px;color:#6b7280;line-height:1.75}
.warn{background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;
padding:10px 14px;font-size:12.5px;color:#9a3412;margin:12px 0}
code{background:#f1f5f9;padding:1px 5px;border-radius:4px;font-size:12px}
@media (max-width:760px){
 body{font-size:13px} h1{font-size:18px} h2{font-size:15px} .wrap{padding:14px}
 .grid{grid-template-columns:1fr;gap:12px}
 .kpis{grid-template-columns:repeat(auto-fit,minmax(132px,1fr))}
 .kpi b{font-size:16px} nav{padding:8px 14px}
}
@media print{nav{display:none} .grid{grid-template-columns:1fr 1fr}}
"""


def dashboard():
    """离线看板：只读**已生成的**结果文件，不运行 Spark。

    同时产出两个版本，内容一致、只有图片来源不同：
      - `dashboard_final.html`      —— 相对路径引用 `figures_final/`（**保留的本地可运行版**）；
      - `dashboard_standalone.html` —— 图片 base64 内嵌，**单文件即可独立分享**。
    选择「内嵌单文件」而非「目录 + ZIP」：本工程 12 张 PNG 合计约 3.6 MB，
    内嵌后约 4.8 MB，单文件分享最不易出错（不会漏带图片目录）。
    """
    cs = load_gold_json("concentration_stats.json") or {}
    gs = load_json("graph_stats.json") or {}
    rq1 = load_json("summary_rq1.json") or {}
    rq5 = read_csv("rq5_task_ecosystem.csv")
    ml = load_json("ml_summary.json") or {}
    gfr = load_json("graphframes_result.json") or {}
    bench = read_csv("benchmark_all_summary.csv")
    met = load_manifest()
    table = html_table

    n_models = rq1.get("n_models", 0)
    unknown_n = rq1.get("unknown_task_models", 0)
    bronze_rows = manifest_value(met, "data.bronze_rows")
    viz = manifest_value(met, "rq3_gf.viz_subgraph") or {}
    comp_size = viz.get("component_size")

    kpi_groups = [
        ("数据集与快照", [
            ("去重模型总数", f"{n_models:,}"),
            ("原始快照行数", f"{bronze_rows:,}" if isinstance(bronze_rows, int) else "-"),
            ("快照 commit", SA.HUBSTATS_COMMIT[:8]),
            ("创建时间范围", f"{rq1.get('first_month', '-')} → {rq1.get('last_month', '-')}"),
        ]),
        ("下载量集中度（downloads = 近 30 天口径）", [
            ("Gini", f"{cs.get('downloads', {}).get('gini', '-')}"),
            ("Top-1% 下载占比", pct(cs.get("downloads", {}).get("top_1pct_share"))),
            ("Top-10% 下载占比", pct(cs.get("downloads", {}).get("top_10pct_share"))),
            ("零下载模型占比", pct(cs.get("downloads", {}).get("zero_value_share"))),
        ]),
        ("模型谱系结构", [
            ("显式血缘边", f"{gs.get('n_edges', 0):,}"),
            ("参与血缘的模型", f"{gs.get('n_nodes_involved', 0):,}"),
            ("最大直接衍生数", f"{gs.get('max_children_count', 0):,}"),
            ("最大弱连通分量", f"{comp_size:,}" if isinstance(comp_size, int) else "-"),
        ]),
        ("任务标签与实验环境", [
            ("有效任务分类模型", f"{n_models - unknown_n:,}"),
            ("任务标签缺失", pct(rq1.get("unknown_task_share"))),
            ("Spark Master / Driver",
             f"{manifest_value(met, 'env.HF_SPARK_MASTER', '-')} / "
             f"{manifest_value(met, 'env.HF_SPARK_DRIVER_MEMORY', '-')}"),
            ("全量 ETL 端到端墙钟",
             f"{manifest_value(met, 'env.etl_elapsed_seconds', '-')} 秒"),
        ]),
    ]

    rq5_rows = []
    if rq5 is not None:
        # 表头写的是「仅已标注任务，样本量 ≥ 200」，因此这里必须真的过滤掉
        # unknown（任务标签缺失的占位组，205.8 万条，按 n_models 排序会排第 1）
        # 与 comparable=False 的小组，否则表头与内容自相矛盾、把未标注模型
        # 当成一个任务展示。过滤口径与 fig04_library_task_structure 完全一致。
        labeled = rq5[(~rq5["is_unknown"]) & rq5["comparable"]]
        for r in labeled.head(10).to_dict(orient="records"):
            rq5_rows.append([r["task"], f"{r['n_models']:,}",
                             f"{r['downloads_total']:,}", f"{r['downloads_median']:.0f}",
                             f"{r['gini_downloads']:.4f}",
                             pct(r["top_10pct_share_downloads"]),
                             pct(r["base_relation_coverage"])])

    ml_rows = [[k, f"{v['rmse']:.4f}", f"{v['mae']:.4f}", f"{v['r2']:.4f}"]
               for k, v in (ml.get("metrics_on_test") or {}).items()]

    bench_rows = []
    if bench is not None:
        # 只展示 P1 规模扫描组，避免 P2 并行度扫描的同名规模档混进来
        b = real_scale_sweep(bench)
        for r in b.sort_values(["n_rows", "op"]).to_dict(orient="records"):
            bench_rows.append([r["scale_label"], f"{r['n_rows']:,}", r["op"],
                               r["shuffle_partitions"], f"{r['seconds_median']:.3f}",
                               fmt_opt(r["executor_run_ms_median"], ".0f"),
                               fmt_opt(r["shuffle_read_bytes_median"], ".1f", 2 ** 20)])

    rq2_table = table(
        [[m, f"{cs.get(m, {}).get('gini', '-')}", pct(cs.get(m, {}).get("top_1pct_share")),
          pct(cs.get(m, {}).get("top_5pct_share")), pct(cs.get(m, {}).get("top_10pct_share")),
          pct(cs.get(m, {}).get("zero_value_share"))]
         for m in ("downloads", "downloadsAllTime")],
        ["口径", "Gini", "Top-1%", "Top-5%", "Top-10%", "零值占比"])

    reach = gs.get("reach_hops", "-")
    rq3_table = table(
        [["血缘边数", f"{gs.get('n_edges', 0):,}"],
         ["不同底座（父）模型", f"{gs.get('n_distinct_base_models', 0):,}"],
         ["不同衍生（子）模型", f"{gs.get('n_distinct_derived_models', 0):,}"],
         ["参与血缘的模型（并集去重）", f"{gs.get('n_nodes_involved', 0):,}"],
         ["既是底座又是衍生的模型", f"{gs.get('n_nodes_both_base_and_derived', 0):,}"],
         ["外部父模型边（快照外引用，已保留）",
          f"{gs.get('n_edges_with_external_parent', 0):,}"
          f"（{pct(gs.get('external_edge_share'))}）"],
         ["最大直接衍生数（children_count）", f"{gs.get('max_children_count', 0):,}"],
         [f"最大 ≤{reach} 跳可达后代数", f"{gs.get('max_descendant_count_khop', 0):,}"],
         ["最大弱连通分量规模（GraphFrames，全量图）",
          f"{comp_size:,}" if isinstance(comp_size, int) else "-"],
         ["GraphFrames 状态", gfr.get("status", "未运行")]],
        ["指标", "数值"])

    rq5_table = table(
        rq5_rows, ["任务", "模型数", "总下载(downloads)", "中位下载", "Gini(downloads)",
                   "Top-10% 份额", "底座关系覆盖率"])
    ml_table = table(ml_rows, ["模型", "RMSE", "MAE", "R²"])
    if bench_rows:
        bench_table = table(bench_rows, ["规模", "行数", "算子", "分区数", "中位耗时(s)",
                                         "CPU/run 时间(ms)", "shuffle 读(MiB)"])
    else:
        bench_table = "<p class='note'>尚未生成 benchmark 结果。</p>"

    kpi_html = kpi_groups_html(kpi_groups)
    peak_month = rq1.get("peak_month", "-")
    peak_count = rq1.get("peak_count", 0)
    partial_month = rq1.get("partial_month", "-")
    # 快照信息一律从结果文件读取，不手写数字（手写值会与真实结果漂移）
    dl_total = cs.get("downloads", {}).get("total_downloads")
    at_total = cs.get("downloadsAllTime", {}).get("total_downloads")
    caliber = (f"{at_total / dl_total:.1f}" if dl_total and at_total else "—")
    floor_share = pct(rq1.get("date_floor_share"))
    first_month = rq1.get("first_month", "-")
    earliest_day = rq1.get("earliest_day", "-")
    # 两种图片来源各算一次：本地版用相对路径，单文件版用 base64 内嵌。
    cards_local = figure_cards(False)
    cards_embed = figure_cards(True)

    body = f"""<header class="top">
<h1>Hugging Face 开源模型生态分析 · Dashboard（正式结果）</h1>
<div class="sub">数据快照 <code>cfahlgren1/hub-stats / models.parquet</code>
@ commit <code>{SA.HUBSTATS_COMMIT}</code>；样本 = 快照内全部 {n_models:,} 个去重模型。
本页为静态离线页面，<b>打开时不会运行 Spark</b>，所有数字均读取自预先落盘的正式结果文件。</div>
</header>
<nav>
<a href="#overview">概览</a><a href="#rq1">RQ1 演化</a><a href="#rq2">RQ2 集中度</a>
<a href="#rq3">RQ3 谱系</a><a href="#rq4">RQ4 关联</a><a href="#rq5">RQ5 任务生态</a>
<a href="#spark">Spark 扩展性</a><a href="#figures">全部图表</a><a href="#limits">口径与限制</a>
</nav>
<div class="wrap">
<h2 id="overview">概览</h2>
{kpi_html}
<div class="warn"><b>口径提示（结论成立的前提）</b>：<b>downloads</b> 为近 30 天下载计数，
<b>downloadsAllTime</b> 为历史累计，两者合计相差约 {caliber} 倍，<b>不可混用、不可相加</b>。
下载量只反映传播热度，<b>不代表模型质量</b>；PageRank 同理不等于质量。
<code>pipeline_tag</code> 缺失率 {pct(rq1.get('unknown_task_share'))}，
缺失记为 unknown 单独报告，<b>不被当作一个任务类别</b>，所有按任务的结论只覆盖已标注部分。</div>

<h2 id="rq1">RQ1 · 生态演化</h2>
<p class="note">研究对象是「快照中仍可见模型的创建时间分布」，<b>不是</b>历史上所有曾发布模型的完整记录。
峰值月 {peak_month}（{peak_count:,} 个）；{partial_month} 为快照截止月，尚未走完，不与完整月份直接比较。</p>
<div class="warn">首月 <code>{first_month}</code> 存在<b>时间戳异常聚集</b>：约 {floor_share} 的模型挤在
<code>{earliest_day}</code> 一天，而该日只有 {rq1.get('earliest_day_distinct_timestamps', '-')} 个不同时间戳。
这是<b>已观测到的现象</b>；<b>根因未确认</b>（平台元数据迁移 / 时间戳记录下限等解释均缺可核验证据），
故不作因果推断，也不当作自然发布高峰。</div>

<h2 id="rq2">RQ2 · 注意力集中与长尾</h2>
<div class="tablewrap">{rq2_table}</div>
<p class="note">Gini 由频数加权秩公式精确计算（非采样近似）；Top-p 份额按值降序取前
<code>max(1, int(n·p))</code> 个，并列值整体同权。只描述长尾形态，
<b>未做严格幂律拟合与模型比较，因此不宣称幂律分布</b>。{supp_dashboard()['rq2']}</p>

<h2 id="rq3">RQ3 · 模型谱系网络</h2>
<div class="tablewrap">{rq3_table}</div>
<p class="note">边语义固定为 <code>src = 底座</code> → <code>dst = 衍生模型</code>，方向不可反转。
四类规模指标含义不同、禁止互相替代：<code>children_count</code>（出度）/ 
<code>direct_family_size</code>（= children_count + 1，1 跳直系）/
<code>descendant_count_khop</code>（≤{reach} 跳有界可达，是下界）/
<code>component_size</code>（弱连通分量，跨多跳）。<b>PageRank 不等于模型质量。</b>
Figure 7 右侧为<b>代表性局部子图</b>，不可解释为完整弱连通分量。{supp_dashboard()['rq3']}</p>

<h2 id="rq4">RQ4 · 属性与下载热度的统计关联</h2>
<div class="tablewrap">{ml_table}</div>
<p class="note">目标 y = log(1+downloads)；<b>先划分后拟合</b>，预处理只在训练集上 fit，
指标只在<b>独立测试集</b>上计算。likes 与 downloads 同取自一个快照，存在同期相关，
<b>不能</b>解释为前瞻性预测能力；特征重要性也不代表因果或模型质量。{supp_dashboard()['rq4']}</p>

<h2 id="rq5">RQ5 · 任务生态对比（仅已标注任务，样本量 ≥ 200）</h2>
<div class="tablewrap">{rq5_table}</div>
<p class="note">下载量口径为 <b>downloads（近 30 天）</b>；unknown 与样本不足的任务组已剔除，
不参与比较。表中「底座关系覆盖率」指该任务下显式声明血缘的模型占比。</p>

<h2 id="spark">Spark 扩展性实验（真实数据）</h2>
<div class="tablewrap">{bench_table}</div>
<p class="note">全部实验在 <code>local[N]</code> <b>单机多核</b>上完成，
<b>不构成多节点横向扩展结论</b>。Windows 无法清 OS 文件缓存，除首次接触外扫描计时均为<b>缓存热态</b>；
列裁剪实验使用<b>不可下推</b>的 <code>count(每列)</code>，以避开 Parquet 统计下推造成的测量偏差。
内存压力行已单独成组，不与正常测量混算中位数。</p>

<h2 id="figures">全部正式图表（Figure 1–12，按研究问题分组）</h2>
{cards_local}
<p class="note">每张图同时提供 PNG（300 dpi）/ PDF / SVG，矢量版适合 LaTeX 插入。
图目录（编号 / 标题 / 数据源 / 口径）见 <code>data_hubstats/gold/analysis/figure_index.json</code>。
旧版 Figure 7（最大弱连通分量 Top-1500 力导向网络）作为<b>补充材料</b>保留在
<code>figures_supplementary/</code>：节点过密、无分类着色，缩小后不可读，故不作为正式图。</p>

<h2 id="limits">口径与限制（易混淆项）</h2>
<div class="tablewrap">{table(
        [["下载热度 / 模型质量", "下载量只反映传播热度，<b>不等于质量</b>"],
         ["PageRank / 模型质量", "只反映图结构中的被引用强度，<b>不等于质量</b>"],
         ["同期统计关联 / 前瞻性预测", "RQ4 是同一快照内的同期关联，<b>不是</b>预测"],
         ["快照创建时间分布 / 历史完整发布记录", "只含当前仍可见模型，<b>不是</b>全部历史"],
         ["任务标签缺失 / 已标注任务", "unknown <b>不是</b>一个任务类别，不计入按任务结论"],
         ["children_count / direct_family_size", "后者 = 前者 + 1（含自身），均为 1 跳直系"],
         ["component_size / 家族规模", "弱连通分量跨多跳，<b>不等于</b> 1 跳家族规模"],
         ["descendant_count_khop / 全量后代", f"只统计 ≤{reach} 跳，<b>是下界</b>"],
         ["maintenance_span_days / 更新频率", "lastModified − createdAt 是<b>维护跨度</b>，不是更新频率"],
         ["本地单机 / 多节点集群", "local[N] 单机多核，<b>不构成</b>横向扩展结论"]]
        + supp_dashboard()['limit_rows'],
        ["易混淆的两侧", "正确表述"])}</div>
</div>"""

    head = ('<!DOCTYPE html><html lang="zh"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<title>HF 开源模型生态分析 · Dashboard（正式结果）</title>'
            f"<style>{DASH_CSS}</style></head><body>")

    out_local = os.path.join(PROJECT_ROOT, "dashboard_final.html")
    with open(out_local, "w", encoding="utf-8") as f:
        f.write(head + body + "</body></html>")
    print(f"[dashboard] 写出 {out_local}（相对路径版，本地可运行）")

    body_embedded = body.replace(cards_local, cards_embed)
    out_single = os.path.join(PROJECT_ROOT, "dashboard_standalone.html")
    with open(out_single, "w", encoding="utf-8") as f:
        f.write(head + body_embedded + "</body></html>")
    size_mb = os.path.getsize(out_single) / 2 ** 20
    print(f"[dashboard] 写出 {out_single}（单文件内嵌版，{size_mb:.1f} MB，可独立分享）")


def main():
    monthly = read_csv("monthly_new_models.csv")
    pipeline_trend = read_csv("pipeline_trend.csv")
    library_trend = read_csv("library_trend.csv")
    ccdf = read_csv("ccdf.csv")
    rq5 = read_csv("rq5_task_ecosystem.csv")
    rq1 = load_json("summary_rq1.json") or {}
    gs = load_json("graph_stats.json") or {}
    cs = load_gold_json("concentration_stats.json") or {}
    ml = load_json("ml_summary.json")
    # 唯一数据源：run_benchmark.py 合并后的汇总（含 p1 真实数据 / p2 并行度 / p3 合成三组）。
    # Figure 10–12 只取 data_kind == "hubstats_models_parquet" 的行，合成数据不会混进来。
    bench = read_csv("benchmark_all_summary.csv")

    if monthly is not None and rq1:
        fig01_monthly(monthly, rq1)
    if pipeline_trend is not None and rq1:
        fig02_task_growth(pipeline_trend, rq1)
        fig03_task_time_heatmap(pipeline_trend)
    if rq5 is not None and library_trend is not None:
        fig04_library_task_structure(rq5, library_trend)
    if cs:
        fig05_lorenz(cs)
    if ccdf is not None and cs:
        fig06_ccdf(ccdf, cs)
    if gs:
        fig07_family_network(gs)
        fig07_legacy_network(gs)
        fig08_family_size(gs)
    if ml:
        fig09_ml(ml)
    if bench is not None:
        fig10_scale_runtime(bench)
        fig11_partitions(bench)
        fig12_operator_comparison(bench)

    # A4 单栏报告版 Figure 7a/7b（不覆盖正式 figures_final/fig07_family_network.*）
    fig07a_report_global_lineage()
    fig07b_report_representative_family()

    dashboard()
    idx_path = os.path.join(ANALYSIS, "figure_index.json")
    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump({"snapshot_sha": SA.HUBSTATS_COMMIT, "footer": build_footer(),
                   "figures": FIG_INDEX}, f, ensure_ascii=False, indent=2)
    print(f"[figures] 共 {len(FIG_INDEX)} 张 -> {FIG_DIR}")
    print(f"[figures] 图目录 -> {idx_path}")

    # 缺件必须让进程**失败**，不能只是少画一张图还返回 0。
    # 上面每个 `if 数据存在` 分支都会静默跳过：一旦某个结果文件路径写错
    # （例如 concentration_stats.json 实际在 gold/ 而不是 gold/analysis/），
    # 就会「退出码 0 但少了 Figure 5/6」，Phase 4 验收会误判为通过。
    have = {int(item["figure"].split()[1]) for item in FIG_INDEX}
    missing = sorted(set(range(1, 13)) - have)
    if missing:
        print(f"[figures] ❌ 缺少 Figure {missing}（共应有 12 张，实际 {len(FIG_INDEX)} 张）。"
              f"请检查对应的上游结果文件是否存在。", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
