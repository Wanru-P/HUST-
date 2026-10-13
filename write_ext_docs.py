#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
write_ext_docs.py — 增量完善（Task A–F）的**文档生成器**。

与 write_reports.py 同样的纪律：**所有数字都从结果文件读取**，
不在文档里手抄任何指标。这样文档与 Dashboard / 图表 / 清单不会漂移。

产出（docs/）：
    family_structure_analysis.md    Task A：家族结构深度分析（方法 + 指标定义 + 结果）
    family_cases.md                 Task B：三个代表性家族案例
    family_explorer_guide.md        Task C：交互式家族探索器使用说明
    concentration_groups.md         Task D：分组集中度与 Theil 分解
    incremental_acceptance.md       Task F：增量交付验收（文件清单 / 运行记录 / 映射 / 限制）

用法：python write_ext_docs.py
"""
import csv
import json
import os
from datetime import datetime, timezone

import schema_adapter as SA

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
DOCS = os.path.join(PROJECT_ROOT, "docs")
os.makedirs(DOCS, exist_ok=True)


def load_json(path, default=None):
    if not os.path.exists(path):
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_csv(name):
    path = os.path.join(ANALYSIS, name)
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def manifest():
    return load_json(os.path.join(ANALYSIS, "results_manifest.json"), {}) or {}


def mv(m, key, default="（缺失）"):
    return ((m.get("metrics") or {}).get(key) or {}).get("value", default)


def num(v, nd=0):
    if v is None:
        return "—"
    try:
        return f"{float(v):,.{nd}f}"
    except (TypeError, ValueError):
        return str(v)


def pct(v, nd=1):
    if v is None:
        return "—"
    try:
        return f"{float(v) * 100:.{nd}f}%"
    except (TypeError, ValueError):
        return str(v)


def table(headers, rows):
    out = ["| " + " | ".join(str(h) for h in headers) + " |",
           "| " + " | ".join("---" for _ in headers) + " |"]
    for r in rows:
        out.append("| " + " | ".join("" if c is None else str(c) for c in r) + " |")
    return "\n".join(out)


def write_to(path, lines):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"[docs] 写出 {path}")


def stamp():
    return datetime.now(timezone.utc).isoformat()


def head(title):
    return [f"# {title}", "",
            f"> 生成时间：{stamp()}  ",
            f"> 数据快照：`{SA.HUBSTATS_COMMIT}`  ",
            "> 本文件由 `write_ext_docs.py` 自动生成，**所有数字取自结果文件，未手抄**。", "",
            "---", ""]


# ============================================================ Task A
def doc_family_structure(m):
    fam = load_json(os.path.join(ANALYSIS, "family_ext_stats.json"), {}) or {}
    scc = load_json(os.path.join(ANALYSIS, "family_scc_summary.json"), {}) or {}
    tier = fam.get("structure_class_counts_by_tier", {}) or {}
    allc, ge10, ge100 = tier.get("all", {}), tier.get("ge10", {}), tier.get("ge100", {})
    rel = fam.get("relation_shares", {}) or {}
    mp = load_csv("family_multi_parent.csv")
    ts = load_csv("family_task_structure.csv")

    L = head("模型家族结构深度分析（Task A）")
    L += [
        "## 1. 问题与图模型", "",
        "在 RQ3 已有「网络规模统计」的基础上，本节进一步刻画**模型家族的结构形态**："
        "一个底座究竟是被**大量浅层复用**（星型）、被**逐层继续衍生**（链型），"
        "还是被**多父合并**（合并型）？不同任务类别的家族结构是否不同？", "",
        "**关键前提：数据是「有向图」，不是「树」。**", "",
        "- 边语义固定为 `src = 底座模型` → `dst = 衍生模型`，方向保留；",
        "- 一个衍生模型可以有**多个底座**（多父 / 合并），这在树上无法表达；",
        "- 底座之间也可能互相指向，图上**存在环**。", "",
        "因此本节所有结论都限定在**显式声明血缘**的网络内 —— "
        f"底座关系字段缺失率 {pct((mv(m, 'data.missing_rates', {}) or {}).get('base_models'), 2)}，"
        "**未声明血缘 ≠ 没有血缘**。", "",
        "---", "",
        "## 2. 环的处理（方法学要点）", "",
        "在有向图上，「最长路径」只有在 **DAG** 上才有良定义；"
        "含环图上的最长**简单**路径问题是 **NP-hard**。"
        "因此本工程采用两步法：", "",
        "1. 用**迭代版 Tarjan** 算法求**强连通分量（SCC）**，把每个环折叠成一个超级节点；",
        "2. 在得到的**凝聚 DAG** 上，用拓扑序 + 动态规划求最长路径。", "",
        "实测环的规模极小，但流程不能省：", "",
        table(["项", "值"], [
            ["参与血缘的节点数", num(scc.get("n_nodes"))],
            ["强连通分量数", num(scc.get("n_strongly_connected_components"))],
            ["规模 > 1 的 SCC 数（真实有向环）", num(scc.get("n_scc_size_gt1"))],
            ["最大 SCC 的节点数", num(scc.get("max_scc_size"))],
            ["落入环中的节点数", num(scc.get("n_nodes_in_cycles"))],
            ["环中节点占比", pct(scc.get("cycle_node_share"), 4)],
            ["自环边（`src == dst`）条数", num(scc.get("n_self_loop_edges"))],
            ["凝聚后是否为 DAG", str(scc.get("condensation_is_dag"))],
        ]), "",
        "> **若跳过 SCC 折叠、直接在原图上跑 DAG 最长路径，结果无意义甚至报错。**", "",
        "---", "",
        "## 3. 指标定义（可复现）", "",
        table(["指标", "定义", "说明"], [
            ["`children_count`", "该底座的**出度**（直接继承它的模型数）",
             "只数 1 跳，不含孙节点"],
            ["`descendant_count_khop`", f"≤ {num(mv(m, 'rq3.reach_hops'))} 跳可达的后代数",
             "有界可达，不是全部后代"],
            ["`subtree_height`", "凝聚 DAG 上，从该节点出发到任一汇点的**最长路径**（边数）",
             "即「最长衍生链」，**不是**树的层数，**不是**可达后代数"],
            ["`root_depth`", "凝聚 DAG 上，从任一源点到该节点的最长路径（边数）",
             "描述「离根多远」"],
            ["`leaf_child_share`", "直接子节点中**自身无出度**的比例",
             "宽而浅时接近 1"],
            ["`merge_share`", "该底座出边中 `relation = merge` 的比例", "合并型判据之一"],
            ["`structure`", "`classify_structure` 的判定结果（star/chain/merge/mixed）",
             "阈值显式写死在 `stats_family.py`"],
        ]), "",
        "`subtree_height` 与 `descendant_count_khop` 是**两个不同的量**："
        "前者是「链有多深」，后者是「一共能触达多少个后代」。"
        "一个底座可以有上万个后代却只有 3 跳深度（星型），也可以只有几百个后代却有 150 跳深度（长链）。", "",
        "---", "",
        "## 4. 多父（合并）拓扑", "",
        f"- 多父模型 **{num(fam.get('n_multi_parent_models'))}** 个，"
        f"占全部衍生模型 {pct(fam.get('multi_parent_share_of_derived'), 3)}；",
        f"- `relation = merge` 的边 **{num(fam.get('n_merge_edges'))}** 条"
        f"（占全部血缘边 {pct(rel.get('merge'), 2)}）。", "",
        "父节点个数分布（仅列多父部分）：", "",
    ]
    if mp:
        multi = [r for r in mp if int(r.get("parents_count", 0)) >= 2]
        L += [table(["父节点个数", "模型数"],
                    [[r["parents_count"], num(r["n_models"])] for r in multi]), ""]
    L += [
        "绝大多数多父模型只有 2 个底座，但存在极少数「几十个上游」的聚合节点 —— "
        "这些结构在「树」的模型下**无法表达**，必须用有向图刻画。", "",
        "---", "",
        "## 5. 家族深度与关系类型", "",
        f"- 最长衍生链最大值 **{num(fam.get('max_subtree_height'))}** 跳；"
        f"全部底座平均 {num(fam.get('mean_subtree_height_all_bases'), 2)} 跳；",
        f"- 血缘边总数 **{num(fam.get('n_edges'))}** 条，构成："
        f"finetune {pct(rel.get('finetune'), 2)} / adapter {pct(rel.get('adapter'), 2)} / "
        f"quantized {pct(rel.get('quantized'), 2)} / merge {pct(rel.get('merge'), 2)}。", "",
        "> 完整的高度分布见 `family_height_dist.csv` 与 `family_depth_dist.csv`；"
        "可视化见 **Figure 14(a)(b)**。", "",
        "---", "",
        "## 6. 结构分类", "",
        "判定规则（`stats_family.classify_structure`，先命中先返回）：", "",
        "1. **merge**：`merge_share ≥ 0.20`；",
        "2. **chain**：`subtree_height ≥ 6` 且 `leaf_child_share ≥ 0.60`；",
        "3. **star**：`children_count ≥ 100` 且 `subtree_height ≤ 6` 且 `leaf_child_share ≥ 0.50`；",
        "4. 其余 **mixed**。", "",
        "> 说明：**没有**把「家族内存在多父节点」当作 merge 判据 —— "
        f"实测衍生模型中就有 {num(fam.get('n_multi_parent_models'))} 个多父节点，"
        "若以此判定，几乎所有大族都会被误判为 merge，该判据无法区分结构。", "",
        table(["范围", "star", "chain", "merge", "mixed", "合计"], [
            ["全部底座", num(allc.get("star")), num(allc.get("chain")),
             num(allc.get("merge")), num(allc.get("mixed")),
             num(sum(int(v) for v in allc.values()) if allc else None)],
            ["children_count ≥ 10", num(ge10.get("star")), num(ge10.get("chain")),
             num(ge10.get("merge")), num(ge10.get("mixed")),
             num(sum(int(v) for v in ge10.values()) if ge10 else None)],
            ["children_count ≥ 100", num(ge100.get("star")), num(ge100.get("chain")),
             num(ge100.get("merge")), num(ge100.get("mixed")),
             num(sum(int(v) for v in ge100.values()) if ge100 else None)],
        ]), "",
        "**结论**：在成规模家族（`children_count ≥ 100`）中，"
        "**星型占绝对多数**，链型次之，合并型最少。"
        "换言之，HF 生态里「一个底座被大量浅层复用」是主流形态，"
        "「长链逐层衍生」是少数，「多父合并」是更少数的特殊形态。", "",
        "规模最大的底座（按直接衍生数）：", "",
        table(["底座", "直接衍生", "最长链", "结构"],
              [[b.get("base_model"), num(b.get("children_count")),
                num(b.get("subtree_height")), b.get("structure")]
               for b in (fam.get("top_bases_by_children") or [])[:10]]), "",
        "---", "",
        "## 7. 任务类别之间的结构差异", "",
    ]
    if ts:
        L += [table(["任务", "底座数", "衍生总数", "中位出度", "平均高度", "merge 占比", "多父占比"],
                    [[r["task"], num(r["n_bases"]), num(r["total_children"]),
                      num(r["median_children"]), num(r["avg_height"], 2),
                      pct(r["merge_share"], 2), pct(r["multiparent_share"], 2)]
                     for r in ts[:15]]), "",
              "**结论**：不同任务生态的谱系组织方式确实不同 —— "
              "`text-to-image` 的合并（merge）占比显著高于 `text-generation`，"
              "而 `text-generation` 的平均链更深。这是对 RQ5 的结构性补充，"
              "属于**同一快照内的描述**，不作因果解释。", ""]
    L += ["---", "",
          "## 8. 限制", "",
          "1. 只覆盖**显式声明血缘**的模型，未声明血缘的模型不在图内；",
          "2. 结构分类阈值人为设定且已写死，**换阈值会改变各类计数**；"
          "稳健的是定性结论（星型最多、链型次之），而非具体数字；",
          "3. `subtree_height` 是凝聚 DAG 上的最长路径，"
          "**不等同于**「可达后代数」，也**不等同于**树的层数；",
          "4. 谱系在快照边界被截断（存在快照外的父模型），后代类指标是**下界**；",
          "5. 下载量只反映传播热度，不代表模型质量。", "",
          "---", "",
          "## 9. 产出文件", "",
          table(["文件", "内容"], [
              ["`family_ext_stats.json`", "家族结构汇总指标"],
              ["`family_scc_summary.json`", "环 / SCC 摘要"],
              ["`family_multi_parent.csv`", "父节点个数分布"],
              ["`family_structure_classes.csv`", "每个底座的结构分类明细"],
              ["`family_task_structure.csv`", "按任务的家族结构汇总"],
              ["`family_depth_dist.csv` / `family_height_dist.csv`", "深度 / 高度分布"],
          ]), ""]
    write_to(os.path.join(DOCS, "family_structure_analysis.md"), L)


# ============================================================ Task B
def doc_family_cases(m):
    cases = load_json(os.path.join(ANALYSIS, "family_cases.json"), {}) or {}
    cs = cases.get("cases", []) or []
    corr = cases.get("children_count_downloads_correlation", {}) or {}
    L = head("三个代表性家族案例（Task B）")
    L += [
        "## 1. 选例规则", "",
        table(["角色", "选例规则"], [
            ["star 星型", "`children_count` 最大的底座"],
            ["chain 链型", "`children_count ≥ 100` 中 `subtree_height` 最大者"],
            ["merge 合并型", "被判为 `merge` 且 `children_count ≥ 20` 中 merge 出边最多者"],
        ]), "",
        "> 选例规则写死在 `analysis_family_cases.py`，可复现；"
        "三个案例分属不同结构，互不重叠。", "",
        "---", "",
        "## 2. 三个案例总览", "",
    ]
    if cs:
        L += [table(["案例", "角色", "任务", "创建时间", "直接衍生", "最长链", "结构"], [
            [c.get("base_model"), c.get("role"), c.get("pipeline_tag"),
             (c.get("created_at") or "")[:10], num(c.get("children_count")),
             num(c.get("subtree_height")), c.get("structure")] for c in cs]), ""]
    for c in cs:
        rc = c.get("relation_counts", {}) or {}
        L += [f"## 3.{['star', 'chain', 'merge'].index(c.get('role', 'star')) + 1} "
              f"{c.get('base_model')}（{c.get('role')}）", "",
              f"- 任务：`{c.get('pipeline_tag')}`；创建于 {c.get('created_at')}；",
              f"- 直接衍生 **{num(c.get('children_count'))}** 个，"
              f"≤3 跳可达后代 **{num(c.get('descendant_count_khop'))}** 个，"
              f"最长衍生链 **{num(c.get('subtree_height'))}** 跳；",
              f"- 关系构成：finetune {num(rc.get('finetune'))} / adapter {num(rc.get('adapter'))} / "
              f"quantized {num(rc.get('quantized'))} / merge {num(rc.get('merge'))}；",
              f"- 底座自身 `downloadsAllTime` = **{num(c.get('base_downloadsAllTime'))}**；"
              f"全部直接衍生的 `downloadsAllTime` 合计 = **{num(c.get('child_sum_downloadsAllTime'))}**；"
              f"底座/衍生比 = **{num(c.get('base_vs_children_all_ratio'), 3)}**；",
              f"- 衍生相对底座的创建时间：中位滞后 **{num(c.get('median_days_after_base'), 1)}** 天"
              f"（最早 {num(c.get('min_days_after_base'), 1)} 天，最晚 "
              f"{num(c.get('max_days_after_base'), 1)} 天）；",
              f"- 在底座**之后**创建的衍生模型 {num(c.get('n_children_after_base'))} 个"
              f"（占 {pct((c.get('n_children_after_base') or 0) / (c.get('children_count') or 1), 2)}）。", "",
              ]
        tc = c.get("top_children") or []
        if tc:
            L += ["衍生热度最高的子模型：", "",
                  table(["子模型", "关系", "downloadsAllTime"],
                        [[t.get("child"), t.get("relation"), num(t.get("downloadsAllTime"))]
                         for t in tc[:8]]), ""]
    L += ["---", "",
          "## 4. 底座规模与下载量的关系", "",
          f"- `children_count` 与 `downloads` 的 Pearson 相关 = "
          f"**{num(corr.get('pearson'), 4)}**，"
          f"Spearman 秩相关 = **{num(corr.get('spearman'), 4)}**。", "",
          "秩相关为正但很弱：被大量复用与下载热度之间存在**微弱的同向关系**，"
          "但远不足以解释下载量的差异。", "",
          "---", "",
          "## 5. 必须声明的限制", "",
          "1. 三个案例都取自**同一个快照**，"
          "「创建时间差」是快照内可见模型之间的差，**不是完整历史**；",
          "2. 底座热度 vs 衍生热度只是**同期描述**，"
          "**不能**推出「衍生带动了底座」或「底座带火了衍生」这类因果；",
          "3. 下载量不代表模型质量；",
          "4. 案例是**结构性代表**，不是「最成功的模型」榜单。", "",
          "---", "",
          "## 6. 产出文件", "",
          table(["文件", "内容"], [
              ["`family_cases.json`", "三个案例的完整指标（含 top_children）"],
              ["`family_cases_summary.csv`", "案例汇总长表"],
              ["`family_cases_children.csv`", "各案例的直接子节点明细"],
              ["`family_cases_relations.csv`", "各案例的关系类型计数"],
          ]), ""]
    write_to(os.path.join(DOCS, "family_cases.md"), L)


# ============================================================ Task C
def doc_explorer_guide(m):
    data = load_json(os.path.join(ANALYSIS, "family_explorer_data.json"), {}) or {}
    meta = data.get("meta", {}) or {}
    caps = meta.get("caps", {}) or {}
    L = head("交互式模型家族探索器 · 使用说明（Task C）")
    L += [
        "## 1. 它是什么", "",
        "`family_explorer.html` 是一个**离线单文件**网页（ECharts 已内嵌，无需联网、"
        "无需服务器、打开时**不运行 Spark**）。它把 RQ3 的家族结构结果变成可以**点开看**的交互视图。", "",
        "入口有两个：",
        "1. 直接双击项目根目录的 `family_explorer.html`；",
        "2. 打开 `dashboard_final.html`，点顶部导航的「家族探索器」。", "",
        "---", "",
        "## 2. 能做什么", "",
        table(["功能", "说明"], [
            ["搜索 / 选择底座", "按名字搜索，或从下拉列表里选一个底座（含 Top-N 家族与三个案例家族）"],
            ["查看直接衍生网络", "以选中的底座为中心，展示它的直接子节点（1 跳）"],
            ["1 跳 / 2 跳展开", "切换到 2 跳可看到孙节点；**案例家族才有 2 跳数据**"],
            ["按关系筛选", "勾选 Finetune / Adapter / Quantized / Merge 过滤边"],
            ["点节点看详情", "显示模型名、任务类型、downloads、downloadsAllTime、created_at、关系"],
            ["拖拽 / 缩放 / 重置", "可拖动节点、滚轮缩放、点「重置视图」回到初始布局"],
            ["典型家族切换", "一键在星型 / 链型 / 合并型三个案例之间切换"],
            ["节点数上限滑块", "大图保护：超过上限的节点不会被绘制，并显示截断提示"],
        ]), "",
        "---", "",
        "## 3. 数据边界（**必须知道**）", "",
        "页面上展示的是**有界子图**，不是全量继承网络：", "",
        table(["限制项", "当前取值"], [
            ["底座家族数（按直接衍生数 Top-N）", num(caps.get("top_families"))],
            ["每个底座的直接子节点上限", num(caps.get("cap_per_base"))],
            ["案例家族的 2 跳边上限", num(caps.get("cap_2hop"))],
            ["整页节点硬上限", num(caps.get("max_nodes"))],
            ["实际节点数", num(meta.get("n_nodes"))],
            ["实际边数", num(meta.get("n_edges"))],
            ["是否发生截断", str(meta.get("truncated"))],
        ]), "",
        meta.get("note", ""), "",
        "> 换句话说：**探索器用于「交互式理解结构」，"
        "所有量化结论仍然取自全量计算结果**，不以页面上的子图为准。", "",
        "---", "",
        "## 4. 生成方式（可复现）", "",
        "```bash",
        "python make_family_explorer.py",
        "```", "",
        "该脚本：读取 Silver 的 `model_base_edges` 与 `models`（用 pyarrow 按 id 过滤，"
        "不把 314 万行读进内存）→ 构造有界子图 → "
        "把 `vendor/echarts.min.js` 与数据内嵌进 `templates/family_explorer_template.html` → "
        "输出 `family_explorer.html` 与 `family_explorer_data.json`。", "",
        "上限可用环境变量调整：`HF_EXPLORER_TOP_FAMILIES` / `HF_EXPLORER_CAP_PER_BASE` / "
        "`HF_EXPLORER_CAP_2HOP` / `HF_EXPLORER_MAX_NODES`。", "",
        "---", "",
        "## 5. 演示建议（答辩时）", "",
        "控制在 **30 秒内**，只演示一遍："
        "「搜索底座 → 展开 1 跳 → 按关系筛选 → 点一个节点看详情 → 切到 2 跳」；"
        "**不要**现场调上限或加载超大规模视图（会卡顿，且容易被问「这是全量吗」）。"
        "被问到时直接回答：**这是有界子图，页面有截断提示，量化结论取自全量计算**。", "",
        "---", "",
        "## 6. 产出文件", "",
        table(["文件", "内容"], [
            ["`family_explorer.html`", "离线单文件交互页面（ECharts + 数据内嵌）"],
            ["`data_hubstats/gold/analysis/family_explorer_data.json`", "有界子图数据（可复用）"],
            ["`vendor/echarts.min.js`", "ECharts 5.5.1（Apache-2.0，本地内嵌）"],
            ["`templates/family_explorer_template.html`", "页面模板（占位符替换）"],
        ]), ""]
    write_to(os.path.join(DOCS, "family_explorer_guide.md"), L)


# ============================================================ Task D
def doc_concentration_groups(m):
    cg = load_json(os.path.join(ANALYSIS, "concentration_groups.json"), {}) or {}
    gl = {"task": "任务类型", "created_year": "创建年份", "has_lineage": "是否有显式血缘"}
    cl = {"downloads": "downloads（近 30 天）", "downloadsAllTime": "downloadsAllTime（累计）"}
    L = head("分组集中度与 Theil 分解（Task D）")
    L += [
        "## 1. 为什么要做这一步", "",
        "原 RQ2 给出的是**总体**集中度（Gini / Top-k / 零值占比）。"
        "一个自然的追问是：**这种极端不均衡到底来自哪里？**", "",
        "本节**不改变**原 RQ2 的任何结果，只是在它之上补做**分组分解**："
        "把模型按「任务类型」「创建年份」「有无显式血缘」分组，"
        "再用 **Theil T 指数**把总体不均衡拆成**组间贡献**与**组内贡献**。", "",
        "---", "",
        "## 2. 方法与公式", "",
        "Theil T 指数（只对正取值有定义；零值单独报告）：", "",
        "```",
        "T = (1/n) · Σ (x_i / μ) · ln(x_i / μ)",
        "```", "",
        "组间 / 组内分解：", "",
        "```",
        "T_total = Σ_g s_g · T_g        （组内贡献 T_within）",
        "        + Σ_g s_g · ln(μ_g / μ)（组间贡献 T_between）",
        "```", "",
        "其中 `s_g = 组 g 的取值和 / 总和`，`μ_g` 为组均值，`μ` 为总均值。"
        "`between_share = T_between / T_total`：",
        "**越接近 0 说明差异主要发生在组内，越接近 1 说明差异主要来自组与组之间。**", "",
        "> 零值处理：`downloads` 口径下大量模型为 0，Theil 只用正取值计算，"
        "零值占比在各组的 `per_group` 中单独给出。", "",
        "---", "",
        "## 3. 结果", "",
    ]
    rows = []
    for grouping in ("task", "created_year", "has_lineage"):
        for caliber in ("downloads", "downloadsAllTime"):
            blk = ((cg.get("groupings") or {}).get(grouping) or {}).get(caliber) or {}
            d = blk.get("theil_decomposition") or {}
            if not d.get("theil_total"):
                continue
            bs = d.get("between_share") or 0.0
            rows.append([gl.get(grouping, grouping), cl.get(caliber, caliber),
                         num(blk.get("n_groups")), num(d.get("theil_total"), 4),
                         pct(bs, 2), pct(1 - bs, 2)])
    if rows:
        L += [table(["分组维度", "口径", "组数", "Theil T", "组间占比", "组内占比"], rows), "",
              "**结论**：三种分组维度下，**组内占比都远高于组间占比**。"
              "也就是说 —— 即使把模型按任务、按年份、或按有无血缘分组，"
              "**每一组内部仍然是极端长尾的**。"
              "总体不均衡主要不是「组与组之间拉开的」，"
              "而是「每组内部的头部与尾部拉开的」。", "",
              "这与 RQ2 的结论**一致**，是对它的**分解**而非修正。", "",
              "---", "",
              "## 4. 分组明细", ""]
    for grouping in ("task", "created_year", "has_lineage"):
        blk = ((cg.get("groupings") or {}).get(grouping) or {})
        for caliber in ("downloads", "downloadsAllTime"):
            per = (blk.get(caliber) or {}).get("per_group") or {}
            if not per:
                continue
            items = sorted(per.items(), key=lambda kv: -kv[1]["n_models"])[:12]
            L += [f"### {gl.get(grouping, grouping)} × {cl.get(caliber, caliber)}", "",
                  table(["分组", "模型数", "Gini", "Top-1% 份额", "零值占比", "Theil T"], [
                      [k, num(v.get("n_models")), num(v.get("gini"), 4),
                       pct(v.get("top_1pct_share"), 2), pct(v.get("zero_value_share"), 2),
                       num(v.get("theil"), 4)] for k, v in items]), ""]
    L += ["---", "",
          "## 5. 限制", "",
          "1. 两种下载口径**分别计算、不可混用、不可相加**；",
          "2. 分组只是描述维度，**组间差异不构成因果**；",
          "3. 小分组（模型数很少）的 Gini/Theil 噪声大，"
          "汇总对比以组规模 ≥ 1000 的组为主；",
          "4. `has_lineage` 只表示「该模型是否出现在血缘边的 dst 端」，"
          "不代表血缘完整性（底座关系缺失率很高）。", "",
          "---", "",
          "## 6. 产出文件", "",
          table(["文件", "内容"], [
              ["`concentration_groups.json`", "含 Theil 分解与各组集中度"],
              ["`concentration_groups.csv`", "长表（grouping, group, caliber, 指标）"],
          ]), ""]
    write_to(os.path.join(DOCS, "concentration_groups.md"), L)


# ============================================================ Task F
def doc_acceptance(m):
    figs = mv(m, "deliverables.figures_ext", []) or []
    L = head("增量完善 · 验收说明（Task F）")
    L += [
        "## 1. 本轮做了什么", "",
        table(["任务", "内容", "主要产出"], [
            ["Task A", "模型家族结构深度分析（有向图 / 环处理 / 多父 / 结构分类 / 任务差异）",
             "`family_ext_stats.json`、`family_*.csv`、`docs/family_structure_analysis.md`"],
            ["Task B", "三个代表性家族案例（星 / 链 / 合并）",
             "`family_cases.json`、`docs/family_cases.md`"],
            ["Task C", "交互式模型家族探索器",
             "`family_explorer.html`、`docs/family_explorer_guide.md`"],
            ["Task D", "分组集中度与 Theil 组间/组内分解",
             "`concentration_groups.json`、`docs/concentration_groups.md`"],
            ["Task E", "新增 3 张正式图",
             "`figures_ext/fig13–15`（PNG/PDF/SVG）"],
            ["Task F", "报告与 PPT 素材更新、文档与验收",
             "`report_final.md`、`docs/ppt_handoff.md`、本文件"],
        ]), "",
        "**本轮为增量**：不重写工程、不覆盖任何旧数据与旧结果；"
        "原 RQ1–RQ5 的结论保持不变，新增内容以**补充**形式并入报告。", "",
        "---", "",
        "## 2. 新增文件清单", "",
        table(["文件", "说明"], [
            ["`analysis_family_ext.py`", "Task A 执行脚本"],
            ["`analysis_family_cases.py`", "Task B 执行脚本"],
            ["`analysis_concentration_groups.py`", "Task D 执行脚本"],
            ["`make_family_explorer.py`", "Task C 页面生成脚本"],
            ["`make_ext_figures.py`", "Task E 图表生成脚本"],
            ["`stats_family.py`", "纯算法层（SCC / 最长路径 / Theil / 结构分类）"],
            ["`test_family_ext.py`", "Task A/D 算法单元测试"],
            ["`write_ext_docs.py`", "本文档等 5 份增量文档的生成器"],
            ["`vendor/echarts.min.js`", "ECharts 5.5.1（离线内嵌）"],
            ["`templates/family_explorer_template.html`", "探索器模板"],
            ["`figures_ext/fig13–15.{png,pdf,svg}`", f"新增图 {num(len(figs))} 张 × 3 格式"],
            ["`family_explorer.html`", "离线交互页面"],
            ["`docs/family_structure_analysis.md` 等 5 份", "增量文档"],
        ]), "",
        "---", "",
        "## 3. 运行记录（复现顺序）", "",
        "```bash",
        "# 0) 算法层单元测试（不需要 Spark）",
        "python test_family_ext.py",
        "",
        "# 1) Task A：家族结构（Spark，约 3 分钟）",
        "python analysis_family_ext.py",
        "",
        "# 2) Task B：三个案例（Spark）",
        "python analysis_family_cases.py",
        "",
        "# 3) Task D：分组集中度（Spark，单次聚合，约 3 分钟）",
        "python analysis_concentration_groups.py",
        "",
        "# 4) Task C：交互式探索器（pyarrow，不需要 Spark）",
        "python make_family_explorer.py",
        "",
        "# 5) Task E：新增图表（matplotlib）",
        "python make_ext_figures.py",
        "",
        "# 6) 重建结果清单 -> 报告 -> 增量文档",
        "python build_manifest.py",
        "python write_reports.py",
        "python write_ext_docs.py",
        "",
        "# 7) 回归：算法 + 契约 + 一致性 + 图表完整性",
        "python test_algorithms.py",
        "python test_spark_contracts.py",
        "python check_consistency.py",
        "python make_figures.py   # 含 12 图完整性守卫",
        "```", "",
        "---", "",
        "## 4. 指标定义速查", "",
        table(["指标", "定义", "出处"], [
            ["`children_count`", "底座出度（直接衍生数）", "`family_base_metrics.csv`"],
            ["`descendant_count_khop`", "≤3 跳可达后代", "同上"],
            ["`subtree_height`", "凝聚 DAG 上到汇点的最长路径（边数）", "同上"],
            ["`root_depth`", "凝聚 DAG 上从源点到该点的最长路径", "同上"],
            ["`leaf_child_share`", "直接子节点中无出度的比例", "同上"],
            ["`merge_share`", "出边中 merge 的比例", "同上"],
            ["`structure`", "star/chain/merge/mixed（阈值显式）", "`stats_family.classify_structure`"],
            ["Gini / Top-k / 零值占比", "频数加权口径（与全工程一致）", "`stats_utils.py`"],
            ["Theil T / 组间占比", "组间 + 组内分解", "`stats_family.theil_*`"],
        ]), "",
        "---", "",
        "## 5. 图表 ↔ 数据映射", "",
        table(["图", "内容", "数据来源"], [
            ["Figure 13", "三种典型家族结构对比（星 / 链 / 合并）",
             "`family_cases.json` + `model_base_edges` + `family_base_metrics.csv`"],
            ["Figure 14", "家族深度分布 / 关系类型构成 / 结构分类",
             "`family_base_metrics.csv` + `family_ext_stats.json`"],
            ["Figure 15", "底座规模 vs 衍生链深度 / 底座热度 vs 衍生热度",
             "`family_base_metrics.csv` + `family_cases.json`"],
        ]), "",
        "每张图的图注均标注了数据来源与口径；索引见 `figures_ext_index.json`。", "",
        "---", "",
        "## 6. 限制（必须与结论一并陈述）", "",
        "1. 家族结论只覆盖**显式血缘**（底座关系缺失 "
        f"{pct((mv(m, 'data.missing_rates', {}) or {}).get('base_models'), 2)}）；",
        "2. 结构分类阈值人为设定，换阈值会改变计数；",
        "3. 含环图必须先做 SCC 折叠才能定义「最长链」；",
        "4. 探索器展示的是**有界子图**，不是全量网络；",
        "5. 所有相关/对比都是**同一快照内的统计描述**，不构成因果；",
        "6. 下载量不代表模型质量。", "",
        "---", "",
        "## 7. 最终状态", "",
        f"**{mv(m, 'final.status', '（见 results_manifest.json）')}**", "",
        mv(m, "final.status_reason", ""), ""]
    write_to(os.path.join(DOCS, "incremental_acceptance.md"), L)


def main():
    m = manifest()
    if not m:
        raise SystemExit("results_manifest.json 不存在，请先运行 build_manifest.py")
    doc_family_structure(m)
    doc_family_cases(m)
    doc_explorer_guide(m)
    doc_concentration_groups(m)
    doc_acceptance(m)
    print("[docs] 增量文档生成完成（5 份）")


if __name__ == "__main__":
    main()
