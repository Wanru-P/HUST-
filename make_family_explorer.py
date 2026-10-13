#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_family_explorer.py — 增量完善 Task C：交互式 **Model Family Explorer**。

从现有 Silver 结果构造一个**有界**的家族子图数据集，并生成**离线单文件**交互页面：
  * family_explorer_data.json  有界子图（节点属性 + 边），供页面加载/复用；
  * family_explorer.html       内嵌 ECharts 与数据，双击即可离线使用。

有界策略（务必在页面与报告中标注这是**代表性局部子图**，不是全量图）：
  * 底座集合 = Top-N（按 children_count）∪ 三个案例家族；
  * 每个底座只保留 downloadsAllTime 最高的若干直接子节点；
  * 仅对案例家族额外保留 2 跳（孙节点），并同样设上限；
  * 总节点数、总边数设硬上限，超限即截断并标记 truncated。

用法：python make_family_explorer.py
"""
import json
import os
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import schema_adapter as SA

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
VENDOR = os.path.join(PROJECT_ROOT, "vendor", "echarts.min.js")

TOP_FAMILIES = int(os.environ.get("HF_EXPLORER_TOP_FAMILIES", "30"))
CAP_PER_BASE = int(os.environ.get("HF_EXPLORER_CAP_PER_BASE", "150"))
CAP_2HOP = int(os.environ.get("HF_EXPLORER_CAP_2HOP", "400"))
MAX_NODES = int(os.environ.get("HF_EXPLORER_MAX_NODES", "6000"))


def load_models_subset(node_ids):
    """只取指定节点集合的模型属性（用 pyarrow 过滤，避免把 314 万行读进内存）。"""
    tbl = pq.read_table(os.path.join(SILVER, "models"),
                        columns=["model_id", "pipeline_tag", "downloads",
                                 "downloadsAllTime", "created_at"])
    mask = pc.is_in(tbl["model_id"], value_set=pa.array(list(node_ids)))
    sub = tbl.filter(mask).to_pandas()
    sub = sub.set_index("model_id")
    return sub


def main():
    edges = pq.read_table(os.path.join(SILVER, "model_base_edges"),
                          columns=["src", "dst", "relation"]).to_pandas()
    metrics = pd.read_csv(os.path.join(ANALYSIS, "family_base_metrics.csv"))

    top_bases = metrics.sort_values("children_count", ascending=False).head(TOP_FAMILIES)
    case_bases = []
    cases_path = os.path.join(ANALYSIS, "family_cases.json")
    if os.path.exists(cases_path):
        with open(cases_path, encoding="utf-8") as f:
            case_bases = [c["base_model"] for c in json.load(f)["cases"]]
    base_ids = list(dict.fromkeys(list(top_bases["base_model"]) + case_bases))

    by_src = {k: v for k, v in edges.groupby("src")}
    # 候选节点：底座 + 直接子节点（+ 案例家族的孙节点）
    candidate = set(base_ids)
    for b in base_ids:
        if b in by_src:
            candidate.update(by_src[b]["dst"].tolist())
    case_set = set(case_bases)
    grandchildren = {}
    for b in case_bases:
        if b in by_src:
            kids = by_src[b]["dst"].tolist()
            gk = set()
            for k in kids:
                if k in by_src:
                    gk.update(by_src[k]["dst"].tolist())
            grandchildren[b] = gk
            candidate.update(gk)

    attrs = load_models_subset(candidate)

    nodes = {}
    links = []

    def add_node(nid, role):
        if nid in nodes:
            if role not in nodes[nid]["roles"]:
                nodes[nid]["roles"].append(role)
            return
        a = attrs.loc[nid] if nid in attrs.index else None
        nodes[nid] = {
            "id": nid,
            "roles": [role],
            "task": (a["pipeline_tag"] if a is not None and pd.notna(a["pipeline_tag"]) else "unknown"),
            "downloads": int(a["downloads"]) if a is not None and pd.notna(a["downloads"]) else 0,
            "downloadsAllTime": int(a["downloadsAllTime"]) if a is not None and pd.notna(a["downloadsAllTime"]) else 0,
            "created_at": (a["created_at"] if a is not None and pd.notna(a["created_at"]) else None),
        }

    families = []
    for b in base_ids:
        if b not in by_src:
            continue
        sub = by_src[b].copy()
        # 按子节点下载量取前 CAP_PER_BASE
        sub["dl"] = sub["dst"].map(lambda x: int(attrs.loc[x, "downloadsAllTime"])
                                   if x in attrs.index and pd.notna(attrs.loc[x, "downloadsAllTime"]) else 0)
        sub = sub.sort_values("dl", ascending=False).head(CAP_PER_BASE)
        row = metrics[metrics["base_model"] == b]
        add_node(b, "base")
        for _, e in sub.iterrows():
            add_node(e["dst"], "child")
            links.append({"s": b, "t": e["dst"], "r": e["relation"]})
        # 案例家族追加 2 跳
        n_2hop = 0
        if b in case_set and b in grandchildren:
            for g in list(grandchildren[b])[:CAP_2HOP]:
                if g in by_src:
                    for _, e2 in by_src[g].head(CAP_PER_BASE // 3).iterrows():
                        add_node(g, "child")
                        add_node(e2["dst"], "grandchild")
                        links.append({"s": g, "t": e2["dst"], "r": e2["relation"]})
                        n_2hop += 1
        families.append({
            "id": b,
            "children_count": int(row["children_count"].iloc[0]) if len(row) else None,
            "subtree_height": int(row["subtree_height"].iloc[0]) if len(row) else None,
            "structure": (row["structure"].iloc[0] if len(row) else "unknown"),
            "in_dataset_children": int(len(sub)),
            "n_2hop_edges": int(n_2hop),
            "is_case": b in case_set,
        })

    truncated = len(nodes) > MAX_NODES
    if truncated:
        keep = set(list(nodes.keys())[:MAX_NODES])
        nodes = {k: v for k, v in nodes.items() if k in keep}
        links = [l for l in links if l["s"] in keep and l["t"] in keep]

    data = {
        "meta": {
            "snapshot_sha": SA.HUBSTATS_COMMIT,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_nodes": len(nodes),
            "n_edges": len(links),
            "caps": {"top_families": TOP_FAMILIES, "cap_per_base": CAP_PER_BASE,
                     "cap_2hop": CAP_2HOP, "max_nodes": MAX_NODES},
            "truncated": truncated,
            "note": ("本页为**代表性局部子图**：只含 Top-N 家族的直接子节点与案例家族的 2 跳，"
                     "已按上限截断，**不是全量继承网络**；结论限定在显式血缘关系内。"
                     "可选的时间轴仅按快照记录的 createdAt 重建，不是完整历史回放。"),
        },
        "families": families,
        "nodes": list(nodes.values()),
        "edges": links,
    }
    out_json = os.path.join(ANALYSIS, "family_explorer_data.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    build_html(data, out_json)
    print(f"[explorer] 节点={len(nodes)} 边={len(links)} 家族={len(families)} "
          f"truncated={truncated} -> {out_json}")


def build_html(data, out_json):
    """生成离线单文件页面：内嵌 echarts.min.js 与数据。"""
    with open(VENDOR, encoding="utf-8") as f:
        echarts_js = f.read()
    with open(os.path.join(PROJECT_ROOT, "templates", "family_explorer_template.html"),
              encoding="utf-8") as f:
        tpl = f.read()
    html = tpl.replace("/*__ECHARTS__*/", echarts_js)
    html = html.replace("/*__DATA__*/", json.dumps(data, ensure_ascii=False, separators=(",", ":")))
    out_html = os.path.join(PROJECT_ROOT, "family_explorer.html")
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[explorer] 写出 {out_html}（{os.path.getsize(out_html)/2**20:.2f} MB，离线可用）")


if __name__ == "__main__":
    main()
