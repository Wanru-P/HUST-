#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
local_analysis.py — 本地（pandas / networkx / scikit-learn）**等价验证与可视化**路径。

⚠️ 定位（必须与报告口径一致）
    本脚本**不是**正式结果来源。正式结果全部来自 PySpark 路径
    （analysis_evo / analysis_graph / analysis_graph_gf / analysis_ml / analysis_rq5）。
    本脚本的用途只有两个：
      (1) 在**同一批行**上独立复算 RQ1–RQ5，用于交叉验证 Spark 结果；
      (2) 用 networkx 生成小规模图可视化子图（Spark 侧不画全图）。
    **绝不允许**把这里的数字当成 Spark 的结果，也绝不允许用 networkx 结果冒充 GraphFrames。

⚠️ 样本范围
    HF_LOCAL_SAMPLE_PCT（默认 5）控制确定性选样比例：
        crc32(model_id) % 100 < pct
    该表达式与 Spark 的 `F.pmod(F.crc32(id), 100) < pct` **逐行等价**
    （已验证 java.util.zip.CRC32 与 zlib.crc32 对同一 UTF-8 字节给出相同结果），
    因此本地与 Spark 落在**完全相同的行集合**上，可以直接对拍。

⚠️ 本次修复的原有问题
    1. RQ4 原来 `pre.fit(X)` 在**全量**上拟合预处理器，属于训练/测试信息泄漏；
       现在所有预处理只在训练集上 fit，指标只在测试集上计算，并补上简单基线。
    2. RQ4 的特征集与 Spark 不一致；现在统一为
       likes_log / model_age_days / tag_count / children_count / maintenance_span_days
       + task / library_top（Top-50 词表来自训练集）。
    3. RQ3 的 `family_size` 只是 1 跳直系规模，却容易被当成"家族规模"；
       现在改名 `direct_family_size`，并把 PageRank 的正/反向含义写清楚。
    4. Gini 曾在多处各写一遍；现在统一调用 `stats_utils`（唯一口径实现）。

用法：HF_LOCAL_SAMPLE_PCT=5 python local_analysis.py
"""
import json
import os
from datetime import datetime, timezone

import networkx as nx
import numpy as np
import pandas as pd

import stats_utils as SU
from local_etl import GOLD, build_gold, load_bronze

ANALYSIS = os.path.join(GOLD, "analysis")
os.makedirs(ANALYSIS, exist_ok=True)

TOP_N_HUBS = 15
VIZ_MAX_NODES = 800
TOP_TAGS = 15
TOP_TASKS = 10
TOP_LIBRARIES = 50
MIN_GROUP_FOR_COMPARISON = 200
SPLIT_SEED = 42
TRAIN_FRACTION = 0.8

NUM_FEATURES = ["likes_log", "model_age_days", "tag_count", "children_count",
                "maintenance_span_days"]
CAT_FEATURES = ["task", "library_top"]


def save_csv(df, name):
    df.to_csv(os.path.join(ANALYSIS, name), index=False)
    print(f"[local] {name:32s} rows={len(df)}")


def save_json(obj, name):
    with open(os.path.join(ANALYSIS, name), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


def freq_of(series):
    """取值 -> (升序唯一值列表, 频数列表)，供 stats_utils 使用。"""
    v, f = np.unique(np.asarray(series, dtype="int64"), return_counts=True)
    return [int(x) for x in v], [int(x) for x in f]


def regression_metrics(y_true, pred):
    """RMSE / MAE / R²（只在测试集上调用）。"""
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    return {"rmse": round(float(np.sqrt(mean_squared_error(y_true, pred))), 4),
            "mae": round(float(mean_absolute_error(y_true, pred)), 4),
            "r2": round(float(r2_score(y_true, pred)), 4)}


def importance_source_name(feature_name):
    """把 ColumnTransformer 展开后的特征名归回源列。

    cat__task_xxx / cat__library_top_xxx / remainder__likes_log
    -> task / library_top / likes_log
    """
    if feature_name.startswith("remainder__"):
        return feature_name.split("remainder__", 1)[1]
    if feature_name.startswith("cat__"):
        body = feature_name.split("cat__", 1)[1]
        return "library_top" if body.startswith("library_top_") else "task"
    return feature_name


# ============================== RQ1 ==============================
def rq1_evolution(g):
    monthly = (g.groupby("created_month").size().rename("new_models")
               .reset_index().rename(columns={"created_month": "month"})
               .sort_values("month").reset_index(drop=True))
    snap_month = str(g["created_month"].max())
    monthly["is_partial"] = monthly["month"] == snap_month
    save_csv(monthly, "monthly_new_models.csv")

    task = g["pipeline_tag"].fillna("unknown").rename("task")
    task_trend = (g.groupby(["created_month", task]).size().rename("new_models")
                  .reset_index().rename(columns={"created_month": "month"})
                  .sort_values(["month", "task"]))
    save_csv(task_trend, "pipeline_trend.csv")

    lib_trend = (g.groupby(["created_month", g["library_name"].fillna("unknown").rename("library")])
                 .size().rename("new_models").reset_index()
                 .rename(columns={"created_month": "month"}).sort_values(["month", "library"]))
    save_csv(lib_trend, "library_trend.csv")

    tag_long = g[["created_month", "tags"]].explode("tags").dropna(subset=["tags"])
    top_tags = (tag_long.groupby("tags").size().sort_values(ascending=False)
                .head(TOP_TAGS).index.tolist())
    tag_top = (tag_long[tag_long["tags"].isin(top_tags)]
               .groupby(["created_month", "tags"]).size().rename("new_models")
               .reset_index().rename(columns={"created_month": "month", "tags": "tag"})
               .sort_values(["month", "tag"]))
    save_csv(tag_top, "tag_trend_top.csv")

    totals = task_trend.groupby("task")["new_models"].sum().sort_values(ascending=False)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "local_pandas",
        "role": "verification_and_visualization_only",
        "n_models": int(len(g)),
        "first_month": str(monthly["month"].iloc[0]),
        "last_month": str(monthly["month"].iloc[-1]),
        "n_months": int(len(monthly)),
        "partial_month": snap_month,
        "peak_month": str(monthly.loc[monthly["new_models"].idxmax(), "month"]),
        "peak_count": int(monthly["new_models"].max()),
        "unknown_task_models": int(totals.get("unknown", 0)),
        "unknown_task_share": round(float(totals.get("unknown", 0) / len(g)), 6),
        "top10_tasks_by_total": {k: int(v) for k, v in totals.head(TOP_TASKS).items()},
        "object_note": ("研究对象是快照中仍可见模型的创建时间分布，"
                        "不是历史上所有曾发布模型的完整记录。"),
    }
    save_json(summary, "summary_rq1.json")
    return monthly, summary


# ============================== RQ2 ==============================
def rq2_concentration(g):
    out = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "engine": "local_pandas",
           "role": "verification_only",
           "n_models": int(len(g)),
           "method": "stats_utils 频数加权秩公式（与 Spark 同一实现）",
           "metric_note": "downloads（近 30 天）与 downloadsAllTime（历史累计）独立成列，不混合。",
           "powerlaw_note": "只描述长尾形态，未做严格幂律拟合与模型比较，不宣称幂律。"}

    lorenz_frames, ccdf_frames = [], []
    for metric in ("downloads", "downloadsAllTime"):
        v, f = freq_of(g[metric].fillna(0))
        out[metric] = SU.concentration_stats(v, f)
        pts = SU.lorenz_points(v, f)
        lorenz_frames.append(pd.DataFrame({
            "metric": metric,
            "cum_model_pct": [p[0] for p in pts],
            "cum_download_pct": [p[1] for p in pts]}))
        cc = SU.ccdf_points(v, f)
        ccdf_frames.append(pd.DataFrame({
            "metric": metric, "value": [c[0] for c in cc],
            "ccdf": [c[1] for c in cc]}))

    save_csv(pd.concat(lorenz_frames, ignore_index=True), "lorenz_curve.csv")
    save_csv(pd.concat(ccdf_frames, ignore_index=True), "ccdf.csv")
    save_json(out, "summary_rq2.json")
    return out


# ============================== RQ3 ==============================
def rq3_graph(g, edges):
    """networkx 图统计 + 小规模可视化子图。

    方向约定（与 Spark 侧一致）：
        边 (src, dst) = (base model, derived model)
        正向 PageRank（base→derived）：高分是"衍生链末端"，**不是**基础模型影响力
        反向 PageRank（derived→base）：高分才是"被大量模型当作底座依赖"的中心
    PageRank 只反映图结构中的被引用强度，**不等于模型质量**。
    """
    nodes_in_models = set(g["model_id"])
    srcs, dsts = edges["src"].tolist(), edges["dst"].tolist()
    involved = sorted(set(srcs) | set(dsts))
    external_parents = sorted(set(srcs) - nodes_in_models)

    G = nx.DiGraph()
    G.add_nodes_from(involved)
    G.add_edges_from(zip(srcs, dsts))
    GU = G.to_undirected()

    out_deg = edges.groupby("src").size().sort_values(ascending=False)
    in_deg = edges.groupby("dst").size().sort_values(ascending=False)

    pr_fwd = nx.pagerank(G, alpha=0.85, max_iter=30)
    G_rev = G.reverse(copy=True)
    pr_rev = nx.pagerank(G_rev, alpha=0.85, max_iter=30)

    comps = sorted(nx.connected_components(GU), key=len, reverse=True)
    comms = sorted(nx.algorithms.community.label_propagation_communities(GU),
                   key=len, reverse=True)

    stats = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "local_networkx",
        "role": "verification_and_visualization_only",
        "warning": ("本文件是 networkx 在**有界样本**上的复算，用于交叉验证与可视化；"
                    "正式图统计来自 Spark（graph_stats.json / graphframes_result.json）。"
                    "不得把 networkx 结果冒充 GraphFrames 结果。"),
        "n_models_total": int(len(g)),
        "n_edges": int(G.number_of_edges()),
        "n_nodes_involved": int(G.number_of_nodes()),
        "n_distinct_base_models": int(edges["src"].nunique()),
        "n_distinct_derived_models": int(edges["dst"].nunique()),
        "n_external_parents": len(external_parents),
        "external_parent_share_of_nodes": round(len(external_parents) / G.number_of_nodes(), 6),
        "n_isolated_models": int(len(g) - G.number_of_nodes()),
        "max_children_count": int(out_deg.iloc[0]) if len(out_deg) else 0,
        "avg_children_count": round(float(out_deg.sum() / max(1, G.number_of_nodes())), 4),
        "n_weak_components_in_graph": len(comps),
        "n_weak_components_total": int(len(comps) + (len(g) - G.number_of_nodes())),
        "largest_component_size": int(len(comps[0])) if comps else 0,
        "n_communities_lpa": len(comms),
        "top_communities": [{"size": len(c), "sample": sorted(c)[:3]} for c in comms[:5]],
        "top_out_degree": [{"id": k, "children": int(v)} for k, v in out_deg.head(TOP_N_HUBS).items()],
        "top_in_degree": [{"id": k, "parents": int(v)} for k, v in in_deg.head(TOP_N_HUBS).items()],
        "top_pagerank_forward_base_to_derived":
            [{"id": k, "score": round(float(v), 6)}
             for k, v in sorted(pr_fwd.items(), key=lambda kv: -kv[1])[:TOP_N_HUBS]],
        "top_pagerank_reverse_derived_to_base":
            [{"id": k, "score": round(float(v), 6)}
             for k, v in sorted(pr_rev.items(), key=lambda kv: -kv[1])[:TOP_N_HUBS]],
        "pagerank_direction_note": (
            "forward(base→derived) 高分 = 衍生链末端；reverse(derived→base) 高分 = 被依赖中心。"
            "PageRank 只反映被引用强度，不等于模型质量。"),
        "family_size_note": (
            "direct_family_size = children_count + 1 只是 1 跳直系规模，"
            "不等于连通分量规模，也不等于可达后代数。"),
    }
    save_json(stats, "graph_stats_local.json")

    top_fam = pd.DataFrame({"base_model": out_deg.index[:TOP_N_HUBS],
                            "num_derived": out_deg.values[:TOP_N_HUBS]})
    save_csv(top_fam, "top_families.csv")

    size_dist = pd.Series([len(c) for c in comps]).value_counts().sort_index()
    save_csv(pd.DataFrame({"component_size": size_dist.index,
                           "num_components": size_dist.values}), "component_size_dist.csv")

    # ---- 可视化子图：最大连通分量内按度数取前 N 个节点（不画全图）----
    big = comps[0] if comps else set()
    deg_map = dict(G.degree())
    picked = sorted(big, key=lambda n: (-deg_map.get(n, 0), n))[:VIZ_MAX_NODES]
    picked_set = set(picked)
    links = [(s, d) for s, d in zip(srcs, dsts)
             if s in picked_set and d in picked_set]
    dl_map = dict(zip(g["model_id"], g["downloads"]))
    viz = {
        "nodes": [{"id": n, "degree": int(deg_map.get(n, 0)),
                   "children": int(out_deg.get(n, 0)),
                   "downloads": int(dl_map.get(n, 0))} for n in picked],
        "links": [{"src": s, "dst": d} for s, d in links],
        "component_size": int(len(big)),
        "n_nodes_shown": len(picked),
        "n_links_shown": len(links),
        "truncated": bool(len(big) > VIZ_MAX_NODES),
        "source": "networkx on bounded sample（仅用于可视化，不代表全量图）",
    }
    save_json(viz, "family_graph.json")
    stats["viz_subgraph"] = {k: viz[k] for k in
                             ("component_size", "n_nodes_shown", "n_links_shown", "truncated")}
    save_json(stats, "graph_stats_local.json")
    return stats


# ============================== RQ4 ==============================
def rq4_ml(g):
    """与 Spark 侧完全一致的定位、特征与划分方式。

    定位：当前快照下模型属性与下载热度的**统计关联**，不是未来是否会火的预测。
    label = log1p(downloads)。likes 与 downloads 同取自一个快照，存在同期相关。
    """
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    df = g.copy()
    df["task"] = df["pipeline_tag"].fillna("unknown")
    df["maintenance_span_days"] = df["update_interval_days"].fillna(0)
    df["label"] = df["downloads_log"]
    for c in NUM_FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    n_dup_ids = int(df["model_id"].duplicated().sum())
    idx_tr, idx_te = train_test_split(df.index.to_numpy(),
                                      train_size=TRAIN_FRACTION,
                                      random_state=SPLIT_SEED)
    train, test = df.loc[idx_tr].copy(), df.loc[idx_te].copy()
    overlap = len(set(train["model_id"]) & set(test["model_id"]))

    # 类别词表只在训练集上统计
    top_libs = train["library_name"].value_counts().head(TOP_LIBRARIES).index.tolist()
    for part in (train, test):
        part["library_top"] = np.where(part["library_name"].isin(top_libs),
                                       part["library_name"], "other")

    corr = {}
    for c in NUM_FEATURES:
        corr[c] = {"pearson": round(float(df[c].corr(df["label"], method="pearson")), 4),
                   "spearman": round(float(df[c].corr(df["label"], method="spearman")), 4)}
    save_csv(pd.DataFrame([{"feature": k, **v} for k, v in corr.items()]),
             "ml_correlations.csv")

    pre = ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="infrequent_if_exist",
                               min_frequency=2, sparse_output=False), CAT_FEATURES)],
        remainder="passthrough")
    pre.fit(train[NUM_FEATURES + CAT_FEATURES])
    names = list(pre.get_feature_names_out())
    Xtr = pre.transform(train[NUM_FEATURES + CAT_FEATURES])
    Xte = pre.transform(test[NUM_FEATURES + CAT_FEATURES])
    ytr, yte = train["label"].to_numpy(), test["label"].to_numpy()

    comparison = [{"model": "Baseline_mean",
                   **regression_metrics(yte, np.full_like(yte, ytr.mean()))}]
    importances = {}
    models = [
        ("LinearRegression", LinearRegression()),
        ("RandomForest", RandomForestRegressor(n_estimators=60, max_depth=12,
                                               random_state=SPLIT_SEED, n_jobs=-1)),
        ("GBT", GradientBoostingRegressor(random_state=SPLIT_SEED)),
    ]
    for name, reg in models:
        pipe = Pipeline([("pre", pre), ("reg", reg)])
        pipe.fit(train[NUM_FEATURES + CAT_FEATURES], ytr)
        pred = pipe.predict(test[NUM_FEATURES + CAT_FEATURES])
        comparison.append({"model": name, **regression_metrics(yte, pred)})
        if name in ("RandomForest", "GBT"):
            imp = pipe.named_steps["reg"].feature_importances_
            agg = {}
            for fname, val in zip(names, imp):
                src = importance_source_name(fname)
                agg[src] = agg.get(src, 0.0) + float(val)
            importances[name] = dict(sorted(agg.items(), key=lambda kv: -kv[1]))

    save_csv(pd.DataFrame(comparison), "ml_model_comparison.csv")
    save_csv(pd.DataFrame([{"feature": k, "importance": round(v, 6)}
                           for k, v in importances["RandomForest"].items()]),
             "ml_feature_importance.csv")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "local_sklearn",
        "role": "verification_only",
        "research_positioning": ("当前快照下属性与下载热度的统计关联，不是前瞻性预测；"
                                 "likes 与 downloads 同源，存在同期相关。"),
        "target": "label = log1p(downloads)",
        "n_rows": int(len(df)),
        "n_duplicate_model_id": n_dup_ids,
        "split": {"seed": SPLIT_SEED, "train_fraction": TRAIN_FRACTION,
                  "n_train": int(len(train)), "n_test": int(len(test)),
                  "train_test_overlap": int(overlap)},
        "features": {"numeric": NUM_FEATURES, "categorical": CAT_FEATURES,
                     "encoding": "one-hot，词表只在训练集 fit",
                     "top_libraries_from_train": top_libs,
                     "rename_note": ("update_interval_days = lastModified - createdAt，"
                                     "含义是维护跨度，不是更新频率；对外名 "
                                     "maintenance_span_days。")},
        "correlations": corr,
        "metrics_on_test": {c["model"]: c for c in comparison},
        "feature_importance": importances,
    }
    save_json(summary, "ml_summary.json")
    return summary


# ============================== RQ5 ==============================
def rq5_task_ecosystem(g, edges):
    parents = edges.groupby("dst").size()
    d = g.copy()
    d["task"] = d["pipeline_tag"].fillna("unknown")
    d["downloads"] = d["downloads"].fillna(0).astype("int64")
    d["downloadsAllTime"] = d["downloadsAllTime"].fillna(0).astype("int64")
    d["parents_count"] = d["model_id"].map(parents).fillna(0).astype("int64")
    n_total = len(d)

    rows = []
    for task, sub in d.groupby("task"):
        v_dl, c_dl = freq_of(sub["downloads"])
        v_at, c_at = freq_of(sub["downloadsAllTime"])
        st_dl, st_at = SU.concentration_stats(v_dl, c_dl), SU.concentration_stats(v_at, c_at)
        n = len(sub)
        rows.append({
            "task": task,
            "is_unknown": task == "unknown",
            # 与 analysis_rq5.py 保持完全一致：comparable = 「有资格参与跨任务比较」。
            # unknown 不是真实任务，必须为 False，否则 n_tasks_comparable 会与
            # Spark 路径差 1（这里按全表求和，Spark 路径先剔除 unknown 再计数）。
            "comparable": bool(task != "unknown"
                               and n >= MIN_GROUP_FOR_COMPARISON
                               and st_dl["distinct_values"] >= 20),
            "n_models": n,
            "n_models_share": round(n / n_total, 8),
            "downloads_total": int(sub["downloads"].sum()),
            "downloadsAllTime_total": int(sub["downloadsAllTime"].sum()),
            "downloads_median": SU.quantile_from_freq(v_dl, c_dl, 0.5),
            "downloadsAllTime_median": SU.quantile_from_freq(v_at, c_at, 0.5),
            "gini_downloads": st_dl["gini"],
            "gini_downloadsAllTime": st_at["gini"],
            "top_10pct_share_downloads": st_dl["top_10pct_share"],
            "top_10pct_share_downloadsAllTime": st_at["top_10pct_share"],
            "zero_share_downloads": st_dl["zero_value_share"],
            "avg_model_age_days": round(float(sub["model_age_days"].mean()), 4),
            "median_model_age_days": SU.quantile_from_freq(
                *freq_of(sub["model_age_days"].fillna(0).astype("int64")), 0.5),
            "base_relation_coverage": round(float((sub["parents_count"] > 0).mean()), 8),
            "avg_children_count": round(float(sub["children_count"].mean()), 4),
            "max_children_count": int(sub["children_count"].max()),
            "avg_direct_family_size": round(float(sub["children_count"].mean()) + 1.0, 4),
            "avg_maintenance_span_days": round(
                float(sub["update_interval_days"].fillna(0).mean()), 4),
        })
    out = pd.DataFrame(rows).sort_values("n_models", ascending=False).reset_index(drop=True)
    save_csv(out, "rq5_task_ecosystem.csv")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "local_pandas",
        "role": "verification_only",
        "n_models_total": n_total,
        "n_tasks_labeled": int((~out["is_unknown"]).sum()),
        # 显式只在已标注任务里计数（与 analysis_rq5.py 的 `labeled` 口径逐字一致），
        # 不依赖 unknown 行的 comparable 取值，避免将来再出现两引擎差 1 的隐性分歧。
        "n_tasks_comparable": int(out.loc[~out["is_unknown"], "comparable"].sum()),
        "min_group_for_comparison": MIN_GROUP_FOR_COMPARISON,
        "threshold_note": (f"n_models < {MIN_GROUP_FOR_COMPARISON} 或唯一取值过少的任务组"
                           "comparable=False，其 Gini 不可与大组直接比较；"
                           "unknown 组（任务标签缺失）恒为 comparable=False，不参与比较。"),
        "unknown_note": "pipeline_tag 缺失统一记为 unknown，单独报告，不参与任务比较。",
        "caliber_note": "downloads 与 downloadsAllTime 独立成列，不混合。",
        "statistics_method": "stats_utils 频数加权口径（与 Spark 同一实现）。",
    }
    save_json(summary, "rq5_summary.json")
    return out


# ============================== main ==============================
def main():
    models, model_tags, edges = load_bronze()
    g, gold_tables = build_gold(models, edges)
    pct = os.environ.get("HF_LOCAL_SAMPLE_PCT", "100")
    print(f"[local] 样本比例={pct}%  模型={len(g):,}  标签行={len(model_tags):,}  "
          f"边={len(edges):,}", flush=True)

    monthly, rq1 = rq1_evolution(g)
    rq2 = rq2_concentration(g)
    rq3 = rq3_graph(g, edges)
    rq4 = rq4_ml(g)
    rq5 = rq5_task_ecosystem(g, edges)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "local_pandas",
        "role": "verification_and_visualization_only",
        "sample_pct": int(pct),
        "sample_rule": "crc32(model_id) % 100 < pct（与 Spark 逐行等价）",
        "n_models": int(len(g)),
        "n_edges": int(len(edges)),
        "rq1": {k: rq1[k] for k in ("n_models", "first_month", "last_month",
                                    "peak_month", "peak_count", "partial_month",
                                    "unknown_task_share")},
        "rq2": {m: rq2[m] for m in ("downloads", "downloadsAllTime")},
        "rq3": {k: rq3[k] for k in ("n_edges", "n_nodes_involved", "n_distinct_base_models",
                                    "n_external_parents", "n_weak_components_in_graph",
                                    "largest_component_size", "max_children_count")},
        "rq4": {"split": rq4["split"], "metrics_on_test": rq4["metrics_on_test"],
                "top_importance": dict(list(rq4["feature_importance"]["RandomForest"].items())[:5])},
        "rq5_top5": rq5.head(5).to_dict(orient="records"),
        "disclaimer": ("本文件仅为本地等价验证/可视化，正式结论以 Spark 路径为准；"
                       "不得用 networkx 结果冒充 GraphFrames。"),
    }
    save_json(summary, "summary.json")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
