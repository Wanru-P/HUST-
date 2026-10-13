#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_ext_figures.py — 增量完善 Task E：新增正式图表（fig13–fig15）。

输出到 `figures_ext/`（独立目录，**不进入** figures_final 的 12 图完整性守卫），
每张图输出 PNG(300dpi)/PDF/SVG 三格式，视觉风格与 figures_final 保持一致
（复用 make_figures 的配色、页脚与字体设置）。

  Figure 13  三种典型家族结构对比（星型 / 链型 / 合并型）
  Figure 14  家族深度分布 + 关系类型构成 + 结构分类
  Figure 15  下载热度 vs 衍生热度（底座规模 vs 下载量）

全部使用真实计算结果（family_base_metrics.csv / family_ext_stats.json /
family_cases.json / Silver 边表），并在图注标注数据来源与口径。

用法：python make_ext_figures.py
"""
import json
import os
from collections import Counter

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import make_figures as MF
import schema_adapter as SA

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
EXT_DIR = os.path.join(PROJECT_ROOT, "figures_ext")
os.makedirs(EXT_DIR, exist_ok=True)

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["savefig.dpi"] = 300

C_MAIN, C_ACCENT, C_ALT, C_GRID = "#2563eb", "#ef4444", "#0f9d58", "#e5e7eb"
REL_COLORS = {"finetune": "#2563eb", "adapter": "#7c3aed",
              "quantized": "#d97706", "merge": "#059669"}
REL_LABELS = {"finetune": "Finetune", "adapter": "Adapter",
              "quantized": "Quantized", "merge": "Merge"}
STRUCT_COLORS = {"star": "#2563eb", "chain": "#7c3aed", "merge": "#059669", "mixed": "#9ca3af"}

EXT_INDEX = []


def save_ext(fig, num, slug, title, sources, note=""):
    base = f"fig{num}_{slug}"
    for ext in ("png", "pdf", "svg"):
        fig.savefig(os.path.join(EXT_DIR, f"{base}.{ext}"), bbox_inches="tight")
    plt.close(fig)
    EXT_INDEX.append({"figure": f"Figure {num}", "file": f"{base}.png", "title": title,
                      "data_sources": sources, "note": note})
    print(f"[ext] {base}  {title}")


def load_json(name):
    with open(os.path.join(ANALYSIS, name), encoding="utf-8") as f:
        return json.load(f)


def load_edges():
    return pq.read_table(os.path.join(SILVER, "model_base_edges"),
                         columns=["src", "dst", "relation"]).to_pandas()


def load_dl_subset(ids):
    tbl = pq.read_table(os.path.join(SILVER, "models"),
                        columns=["model_id", "downloadsAllTime"])
    mask = pc.is_in(tbl["model_id"], value_set=pa.array(list(ids)))
    sub = tbl.filter(mask).to_pandas()
    return dict(zip(sub["model_id"], sub["downloadsAllTime"]))


def sample_deep_chain(children_map, base, levels=5, fanout=1):
    """沿真实边采样一条最深分支：每层取子节点数最多的那个继续往下。"""
    chain = [base]
    cur = base
    for _ in range(levels):
        kids = children_map.get(cur, [])
        if not kids:
            break
        nxt = max(kids, key=lambda k: len(children_map.get(k, [])))
        chain.append(nxt)
        cur = nxt
    return chain


# ---------------------------------------------------------------- Figure 13
def fig13_structure_compare(cases, edges, short):
    children_map = {}
    for s, d in zip(edges["src"], edges["dst"]):
        children_map.setdefault(s, []).append(d)
    parents_map = {}
    for s, d in zip(edges["src"], edges["dst"]):
        parents_map.setdefault(d, []).append(s)

    fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.8))
    roles = [("star", "星型 Star", C_MAIN), ("chain", "链型 Chain", "#7c3aed"),
             ("merge", "合并型 Merge", C_ALT)]
    case_by_role = {c["role"]: c for c in cases}

    for ax, (role, label, color) in zip(axes, roles):
        c = case_by_role[role]
        base = c["base_model"]
        ax.set_title(f"{label}\n{short(base)}", fontsize=11)
        ax.axis("off")

        if role == "star":
            kids = children_map.get(base, [])[:48]
            n = len(kids)
            ax.scatter([0], [0], s=420, c="#111827", zorder=3)
            for i, k in enumerate(kids):
                a = -np.pi / 2 + 2 * np.pi * i / max(n, 1)
                x, y = np.cos(a), np.sin(a)
                ax.plot([0, x], [0, y], color="#9ca3af", lw=0.6, zorder=1)
                ax.scatter([x], [y], s=26, c=color, zorder=2)
            ax.set_xlim(-1.35, 1.35); ax.set_ylim(-1.35, 1.35)
            ax.text(0, -1.5, f"直接衍生 {c['children_count']:,} · 采样显示 {n}",
                    ha="center", fontsize=9, color="#374151")
        elif role == "chain":
            chain = sample_deep_chain(children_map, base, levels=5)
            for i in range(len(chain) - 1):
                ax.annotate("", xy=(i + 1, 0), xytext=(i, 0),
                            arrowprops=dict(arrowstyle="->", color="#7c3aed", lw=1.6))
            # 每一层画少量旁支
            for i, node in enumerate(chain):
                sib = [k for k in children_map.get(node, [])[:4] if k not in chain]
                for j, s in enumerate(sib):
                    ax.scatter([i + 0.28], [0.16 * (j + 1)], s=16, c="#c4b5fd", zorder=2)
                    ax.plot([i, i + 0.28], [0, 0.16 * (j + 1)], color="#ddd6fe", lw=0.6)
                ax.scatter([i], [0], s=260 if i == 0 else 90,
                           c="#111827" if i == 0 else color, zorder=3)
                lab = short(node)
                lab = lab if len(lab) <= 12 else lab[:11] + "…"
                ax.text(i, -0.30, lab, ha="center", va="top", fontsize=6.5, rotation=45)
            ax.set_xlim(-0.6, len(chain) - 0.4); ax.set_ylim(-0.7, 0.9)
            ax.text(len(chain) / 2 - 0.5, 0.72,
                    f"最长链 {c['subtree_height']} 跳 · 直接衍生 {c['children_count']:,}",
                    ha="center", fontsize=9, color="#374151")
        else:  # merge
            kids = children_map.get(base, [])[:36]
            n = len(kids)
            ax.scatter([0], [0], s=380, c="#111827", zorder=3)
            n_mp = 0
            for i, k in enumerate(kids):
                a = -np.pi / 2 + 2 * np.pi * i / max(n, 1)
                x, y = np.cos(a), np.sin(a)
                ps = parents_map.get(k, [])
                is_mp = len(ps) >= 2
                n_mp += int(is_mp)
                ax.plot([0, x], [0, y], color="#059669" if is_mp else "#9ca3af",
                        lw=1.4 if is_mp else 0.6, zorder=1)
                if is_mp:
                    a2 = a + 0.5
                    ax.plot([np.cos(a2) * 0.9, x], [np.sin(a2) * 0.9, y],
                            color="#f59e0b", lw=0.9, ls="--", zorder=1)
                    ax.scatter([np.cos(a2) * 0.9], [np.sin(a2) * 0.9], s=22,
                               c="#f59e0b", marker="s", zorder=2)
                ax.scatter([x], [y], s=26, c=color, zorder=2)
            ax.set_xlim(-1.35, 1.35); ax.set_ylim(-1.35, 1.35)
            ax.text(0, -1.5, f"直接衍生 {c['children_count']:,} · 采样中多父节点 {n_mp}",
                    ha="center", fontsize=9, color="#374151")

    fig.suptitle("Figure 13 · 三种典型模型家族结构对比（代表性局部子图，非全量网络）",
                 fontsize=13, y=1.0)
    fig.text(0.5, 0.005, MF.build_footer(), ha="center", fontsize=8, color="#6b7280")
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    save_ext(fig, 13, "family_structure_compare",
             "三种典型家族结构对比（星/链/合并）",
             ["gold/analysis/family_cases.json", "silver/model_base_edges",
              "gold/analysis/family_base_metrics.csv"],
             "各面板为代表性局部子图（按上限采样），非完整家族网络；"
             "星型=FLUX.1-dev、链型=Meta-Llama-3-8B、合并型=Laxhar/noobai-XL-1.1；"
             "橙色方块/虚线表示多父（合并）来源。")


# ---------------------------------------------------------------- Figure 14
def fig14_depth_relation(base_metrics, ext_stats):
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.4))

    h = base_metrics["subtree_height"].clip(upper=20)
    ax = axes[0]
    ax.hist(h, bins=np.arange(0, 22) - 0.5, color=C_MAIN, alpha=0.85)
    ax.set_yscale("log")
    ax.set_xlabel("最长衍生链（subtree_height，跳；≥20 合并显示）")
    ax.set_ylabel("底座模型数（log）")
    ax.set_title("(a) 家族深度分布", fontsize=11)
    ax.grid(axis="y", color=C_GRID, lw=0.6)

    ax = axes[1]
    shares = ext_stats["relation_shares"]
    order = ["finetune", "adapter", "quantized", "merge"]
    ax.bar([REL_LABELS[k] for k in order], [shares[k] * 100 for k in order],
           color=[REL_COLORS[k] for k in order])
    for i, k in enumerate(order):
        ax.text(i, shares[k] * 100 + 0.6, f"{shares[k]*100:.1f}%", ha="center", fontsize=9)
    ax.set_ylabel("占全部血缘边比例（%）")
    ax.set_title("(b) 关系类型构成", fontsize=11)
    ax.grid(axis="y", color=C_GRID, lw=0.6)

    ax = axes[2]
    tier = ext_stats["structure_class_counts_by_tier"]["ge100"]
    labels = ["star", "chain", "merge", "mixed"]
    vals = [tier.get(k, 0) for k in labels]
    ax.bar(["星型", "链型", "合并型", "混合"], vals,
           color=[STRUCT_COLORS[k] for k in labels])
    for i, v in enumerate(vals):
        ax.text(i, v + max(vals) * 0.02, str(v), ha="center", fontsize=9)
    ax.set_ylabel("底座模型数")
    ax.set_title("(c) 成规模家族结构分类\n（children_count ≥ 100）", fontsize=11)
    ax.grid(axis="y", color=C_GRID, lw=0.6)

    fig.suptitle("Figure 14 · 模型家族深度与关系类型分布", fontsize=13, y=1.02)
    fig.text(0.5, -0.02, MF.build_footer(), ha="center", fontsize=8, color="#6b7280")
    fig.tight_layout()
    save_ext(fig, 14, "family_depth_relation",
             "模型家族深度与关系类型分布",
             ["gold/analysis/family_base_metrics.csv", "gold/analysis/family_ext_stats.json"],
             "深度=凝聚 DAG 上的最长衍生链（含环图先做 SCC 折叠）；"
             "关系类型占比基于 919,839 条显式血缘边；"
             "(c) 只统计 children_count≥100 的成规模家族。")


# ---------------------------------------------------------------- Figure 15
def fig15_heat(cases, base_metrics):
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8))

    ax = axes[0]
    d = base_metrics.dropna(subset=["children_count", "subtree_height"])
    colors = [STRUCT_COLORS.get(s, "#9ca3af") for s in d["structure"]]
    ax.scatter(d["children_count"], d["subtree_height"] + 1, s=6, c=colors, alpha=0.35,
               rasterized=True)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("直接衍生数 children_count（log）")
    ax.set_ylabel("最长衍生链 subtree_height+1（log）")
    ax.set_title("(a) 底座规模 vs 衍生链深度", fontsize=11)
    for c in cases:
        ax.annotate(c["base_model"].split("/")[-1],
                    (c["children_count"], c["subtree_height"] + 1),
                    fontsize=8, color="#111827",
                    xytext=(4, 4), textcoords="offset points")
        ax.scatter([c["children_count"]], [c["subtree_height"] + 1], s=90,
                   facecolors="none", edgecolors="#111827", zorder=3)
    handles = [plt.Line2D([], [], marker="o", ls="", color=v, label=k)
               for k, v in STRUCT_COLORS.items()]
    ax.legend(handles=handles, fontsize=8, title="结构", loc="upper left")
    ax.grid(color=C_GRID, lw=0.5)

    ax = axes[1]
    names = [c["base_model"].split("/")[-1] for c in cases]
    base_dl = [c["base_downloadsAllTime"] for c in cases]
    child_dl = [c["child_sum_downloadsAllTime"] for c in cases]
    x = np.arange(len(cases)); w = 0.38
    ax.bar(x - w / 2, base_dl, w, label="底座自身 downloadsAllTime", color="#111827")
    ax.bar(x + w / 2, child_dl, w, label="全部直接衍生 downloadsAllTime 合计", color=C_MAIN)
    ax.set_yscale("log")
    ax.set_xticks(x); ax.set_xticklabels(names, fontsize=9)
    ax.set_ylabel("downloadsAllTime（log）")
    ax.set_title("(b) 底座下载热度 vs 衍生热度", fontsize=11)
    ax.legend(fontsize=8)
    ax.grid(axis="y", color=C_GRID, lw=0.6)

    fig.suptitle("Figure 15 · 下载热度与衍生热度对比（不代表质量、不代表因果）",
                 fontsize=13, y=1.02)
    fig.text(0.5, -0.02, MF.build_footer(), ha="center", fontsize=8, color="#6b7280")
    fig.tight_layout()
    save_ext(fig, 15, "download_derivative_heat",
             "下载热度与衍生热度对比",
             ["gold/analysis/family_base_metrics.csv", "gold/analysis/family_cases.json"],
             "下载量只反映传播热度，不代表模型质量；两者为同一快照下的同期统计，"
             "不构成因果。纵轴 log10 便于跨量级比较。")


def main():
    ext_stats = load_json("family_ext_stats.json")
    base_metrics = pd.read_csv(os.path.join(ANALYSIS, "family_base_metrics.csv"))
    cases = load_json("family_cases.json")["cases"]
    edges = load_edges()

    def short(x):
        return x.split("/")[-1]

    fig13_structure_compare(cases, edges, short)
    fig14_depth_relation(base_metrics, ext_stats)
    fig15_heat(cases, base_metrics)

    with open(os.path.join(ANALYSIS, "figures_ext_index.json"), "w", encoding="utf-8") as f:
        json.dump({"snapshot_sha": SA.HUBSTATS_COMMIT, "footer": MF.build_footer(),
                   "figures": EXT_INDEX}, f, ensure_ascii=False, indent=2)
    print(f"[ext] 共 {len(EXT_INDEX)} 张 -> {EXT_DIR}")


if __name__ == "__main__":
    main()
