# -*- coding: utf-8 -*-
"""补充图 S3 / S4 / S5。

S3：模型层 vs **发布账号层**的下载量集中度 + 按发布账号分组的 Theil 分解
S4：血缘衍生活跃度（按**官方发布账号前缀**归并的被引用边数 vs 单底座强度）
S5：稳健性矩阵（发布账号规模门槛 × 是否剔除零下载，正交互斥的 12 组）
    —— 图内文字明确统计对象为「发布账号」，不使用「组织」等易误解说法

风格对齐 figures_supp（微软雅黑 / 300dpi / 同配色 / PNG+PDF+SVG）。
数据来自 org_concentration_result.json、lineage_family_activity_result.json、
robustness_matrix.csv。

用法：python make_org_lineage_figures.py
"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt          # noqa: E402
import numpy as np                        # noqa: E402

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["figure.dpi"] = 120
plt.rcParams["savefig.dpi"] = 300

C_MAIN, C_ACCENT, C_ALT, C_GRID = "#2563eb", "#ef4444", "#0f9d58", "#e5e7eb"
HERE = os.path.dirname(os.path.abspath(__file__))
FIG_DIR = os.path.join(HERE, "figures_org_lineage")
os.makedirs(FIG_DIR, exist_ok=True)


def _save(fig, name):
    for ext in ("png", "pdf", "svg"):
        fig.savefig(os.path.join(FIG_DIR, f"{name}.{ext}"), bbox_inches="tight")
    plt.close(fig)


def _load(name):
    with open(os.path.join(HERE, name), encoding="utf-8") as f:
        return json.load(f)


def fig_s3():
    org = _load("org_concentration_result.json")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.0, 4.9))

    labels = ["模型层\n(近30天)", "发布账号层\n(账号累计·近30天)", "模型层\n(累计)",
              "发布账号层\n(账号累计·累计)"]
    vals = [org["gini_model_downloads"], org["gini_org_downloads"],
            org["gini_model_downloadsAllTime"], org["gini_org_downloadsAllTime"]]
    cols = [C_MAIN, C_ACCENT, C_MAIN, C_ACCENT]
    x = np.arange(len(vals))
    ax1.bar(x, vals, 0.62, color=cols)
    for xi, v in zip(x, vals):
        ax1.text(xi, v + max(vals) * 1e-6 + 0.00012, f"{v:.4f}", ha="center", fontsize=8.5)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=7.6)
    ax1.set_ylim(min(vals) - 0.0025, max(vals) + 0.0014)
    ax1.set_ylabel("Gini")
    ax1.set_title("发布账号层集中度高于模型层\n（两个 Gini 的统计单位不同，不能直接证明垄断）",
                  fontsize=10)
    ax1.grid(axis="y", color=C_GRID, lw=0.5)

    th = org["theil"]
    ax2.bar(["Theil T"], [th["between"]], 0.45, color=C_ACCENT,
            label=f"组间（发布账号之间） {th['between_share']:.2%}")
    ax2.bar(["Theil T"], [th["within"]], 0.45, bottom=[th["between"]], color=C_MAIN,
            label=f"组内（发布账号内部） {th['within_share']:.2%}")
    ax2.set_ylabel("Theil T")
    ax2.set_title("正下载模型按发布账号分组的 Theil 分解\n"
                  f"组间差异贡献 {th['between_share']:.2%}（只描述差异分布，不证明成因）",
                  fontsize=10)
    ax2.legend(fontsize=8, loc="lower right")
    ax2.grid(axis="y", color=C_GRID, lw=0.5)

    fig.suptitle(f"模型 {org['n_models']:,} 个 / 发布账号 {org['n_accounts']:,} 个 · "
                 f"零下载模型占比 {org['zero_download_share']:.2%}（单独报告）",
                 fontsize=9.5)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    _save(fig, "figS3_org_vs_model_concentration")
    print("  -> figS3_org_vs_model_concentration.{png,pdf,svg}")


def fig_s4():
    lin = _load("lineage_family_activity_result.json")
    top = lin["family_activity_top20"][:12]
    names = [t["family"] for t in top][::-1]
    edges = [t["n_edges"] for t in top][::-1]
    per = [t["edges_per_active_base"] for t in top][::-1]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.8, 5.4))
    y = np.arange(len(names))
    ax1.barh(y, edges, 0.68, color=C_MAIN)
    for i, v in enumerate(edges):
        ax1.text(v * 1.012, i, f"{v:,}", va="center", fontsize=8)
    ax1.set_yticks(y)
    ax1.set_yticklabels(names, fontsize=8.5)
    ax1.set_xlabel("被引用血缘边数（显式血缘边）")
    ax1.set_title("衍生总量：Qwen 官方发布账号前缀居首", fontsize=10)
    ax1.grid(axis="x", color=C_GRID, lw=0.5)
    ax1.set_xlim(0, max(edges) * 1.13)
    ax1.set_ylim(-0.7, len(names) - 0.3)

    ax2.barh(y, per, 0.68, color=C_ALT)
    for i, v in enumerate(per):
        ax2.text(v * 1.012, i, f"{v:,.0f}", va="center", fontsize=8)
    ax2.set_yticks(y)
    ax2.set_yticklabels([])
    ax2.set_xlabel("每个被作为底座的模型的衍生数")
    ax2.set_title("单底座强度：black-forest-labs / distilbert 最高\n"
                  "（排序随底座门槛变化）", fontsize=10)
    ax2.grid(axis="x", color=C_GRID, lw=0.5)
    ax2.set_xlim(0, max(per) * 1.15)
    ax2.set_ylim(-0.7, len(names) - 0.3)

    n_edges = lin.get("n_edges_official", lin["n_edges"])
    fig.suptitle(f"血缘衍生活跃度：总量 vs 单底座强度 —— 统计对象为「官方发布账号前缀」 · "
                 f"正式血缘边口径（按模型 id 去重 + 按 (base, derived) 去重）{n_edges:,} 条",
                 fontsize=9.5)
    fig.tight_layout(rect=(0, 0, 1, 0.945))
    _save(fig, "figS4_lineage_family_activity")
    print("  -> figS4_lineage_family_activity.{png,pdf,svg}")


def fig_s5():
    import csv as _csv
    with open(os.path.join(HERE, "robustness_matrix.csv"), encoding="utf-8-sig") as f:
        rows = [r for r in _csv.DictReader(f) if r["part"] == "A_org_concentration"]
    rows.sort(key=lambda r: (r["caliber"] != "downloads",
                             int(r["account_min_models"]),
                             r["exclude_zero"] == "True"))

    def lab(r):
        cal = "近30天" if r["caliber"] == "downloads" else "累计"
        amm = int(r["account_min_models"])
        acct = "不限" if amm == 0 else f"≥{amm}"
        ex = "剔零" if r["exclude_zero"] == "True" else "含零"
        return f"{cal}\n{acct}\n{ex}"

    labels = [lab(r) for r in rows]
    gm = [float(r["gini_model"]) for r in rows]
    go = [float(r["gini_org"]) for r in rows]
    bs = [float(r["theil_between_share"]) for r in rows]
    zs = [float(r["zero_share"]) for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14.4, 5.2))
    x = np.arange(len(rows))
    w = 0.38
    ax1.bar(x - w / 2, gm, w, color=C_MAIN, label="模型层 Gini")
    ax1.bar(x + w / 2, go, w, color=C_ACCENT, label="发布账号层 Gini")
    for xi, a, b in zip(x, gm, go):
        ax1.text(xi - w / 2, a + 0.00008, f"{a:.4f}", ha="center", va="bottom",
                 rotation=90, fontsize=6.2)
        ax1.text(xi + w / 2, b + 0.00008, f"{b:.4f}", ha="center", va="bottom",
                 rotation=90, fontsize=6.2)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=7.0)
    ax1.set_ylim(0.9905, 1.0013)
    ax1.set_ylabel("Gini")
    ax1.set_xlabel("口径 · 账号规模门槛（按账号发布的全部模型数）· 是否剔除零下载\n"
                   f"零下载占比 {min(zs):.1%} – {max(zs):.1%}（单独报告）",
                   fontsize=8.5)
    ax1.set_title("模型层 vs 发布账号层 Gini\n"
                  "「发布账号层更高」不稳健：门槛提高后部分口径反转", fontsize=10)
    ax1.legend(fontsize=8, loc="lower right")
    ax1.grid(axis="y", color=C_GRID, lw=0.5)

    ax2.bar(x, bs, 0.58, color=C_ALT, zorder=3)
    for xi, v in zip(x, bs):
        ax2.text(xi, v + 0.008, f"{v:.1%}", ha="center", fontsize=7.0)
    ax2.axhspan(min(bs), max(bs), color="#fde68a", alpha=0.35, zorder=0)
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, fontsize=7.0)
    ax2.set_ylim(0, 0.85)
    ax2.set_ylabel("Theil 组间占比")
    ax2.set_xlabel("口径 · 账号规模门槛（按账号发布的全部模型数）· 是否剔除零下载", fontsize=8.5)
    ax2.set_title(f"组间差异贡献稳定在 {min(bs):.1%} – {max(bs):.1%}\n"
                  "只能描述差异分布，不能证明成因", fontsize=10)
    ax2.grid(axis="y", color=C_GRID, lw=0.5)

    fig.tight_layout()
    _save(fig, "figS5_robustness_matrix")
    print("  -> figS5_robustness_matrix.{png,pdf,svg}")


if __name__ == "__main__":
    fig_s3()
    fig_s4()
    fig_s5()
    print("figures ->", FIG_DIR)
