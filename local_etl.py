#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
local_etl.py — 本地（pandas）等价 ETL，产出与 spark_etl.py 一致的 Silver 三表 + Gold 七表。

Phase 1A 修订：
  - 与 spark_etl.py 共用 schema_adapter 的路径解析与列契约；
  - Bronze 输入同时支持 hub-stats 的 models.parquet 与旧 Demo 的 models.jsonl；
  - 产物按 HF_ENGINE 隔离：spark -> silver/gold，local -> silver_local/gold_local。
  - 对外仍导出 GOLD / DATA_DIR / build_gold / load_bronze（local_analysis.py 依赖）。

用法：
    python local_etl.py
    # 新正式数据：HF_DATA_DIR=<...>/data_hubstats  HF_ENGINE=local
    # 旧 Demo：HF_DATA_DIR=<...>/data  HF_BRONZE_PATH=<...>/data/bronze/models.jsonl
"""
import json
import math
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import schema_adapter as SA
import stats_utils as SU

DIRS = SA.resolve_dirs()
DATA_DIR = DIRS["root"]
SILVER = DIRS["silver"]
GOLD = DIRS["gold"]
BRONZE = SA.bronze_path()

NUMERIC_COLS = ["downloads", "downloadsAllTime", "likes"]
PARQUET_BRONZE_COLS = ["id", "author", "createdAt", "lastModified", "downloads",
                       "downloadsAllTime", "likes", "pipeline_tag", "library_name",
                       "tags", "baseModels"]


def _norm_base_model(v):
    """cardData.base_model 可能是单字符串、列表或缺失 → 统一成 list[str] 或 None。"""
    if v is None:
        return None
    if isinstance(v, list):
        vals = [str(x) for x in v if x]
        return vals or None
    return [str(v)]


def _iso_z(series):
    """datetime / ISO 字符串序列 -> 统一 ISO-8601 UTC 字符串（与旧 Demo 格式一致）。
    用 pd.to_datetime 同时兼容 timestamp 与字符串两种输入。"""
    return pd.to_datetime(series, utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _load_parquet(path, manifest):
    """读 hub-stats parquet -> (models, model_tags, edges)。

    Phase 2 起支持 HF_LOCAL_SAMPLE_PCT（默认 100）：按
    `crc32(model_id) % 100 < pct` 做**与 Spark 完全一致**的确定性选样。
    为控制内存，逐 batch 过滤（不把 314 万行整体读进内存再筛）。
    """
    import pyarrow.parquet as pq
    names = set(pq.read_schema(path).names)
    cols = [c for c in PARQUET_BRONZE_COLS if c in names]
    pct = int(os.environ.get("HF_LOCAL_SAMPLE_PCT", "100") or "100")
    if pct < 100:
        raw = _read_parquet_sampled(path, cols, pct)
    else:
        raw = pd.read_parquet(path, columns=cols)

    models = pd.DataFrame({
        "model_id": raw["id"],
        "author": raw.get("author"),
        "created_at": _iso_z(raw["createdAt"]),
        "last_modified": _iso_z(raw["lastModified"]),
        "downloads": raw.get("downloads"),
        "downloadsAllTime": raw.get("downloadsAllTime"),
        "likes": raw.get("likes"),
        "pipeline_tag": raw.get("pipeline_tag"),
        "library_name": raw.get("library_name"),
        "tags": raw.get("tags"),
        "gated": None,
        "base_models": raw["baseModels"].apply(
            lambda bm: [m["id"] for m in bm["models"] if m.get("id")] or None
            if isinstance(bm, dict) else None),
        "base_relation": raw["baseModels"].apply(
            lambda bm: bm.get("relation") if isinstance(bm, dict) else None),
        "scraped_at": manifest.get("verified_at", ""),
        "source": SA.HUBSTATS_DATASET,
        "snapshot_sha": manifest.get("commit", ""),
    })
    models["author"] = models["author"].fillna(models["model_id"].str.split("/").str[0])
    models["tags"] = models["tags"].apply(
        lambda t: list(t) if isinstance(t, (list, tuple, np.ndarray)) else None)
    models = models.dropna(subset=["model_id", "created_at"]).drop_duplicates(
        subset=["model_id"]).reset_index(drop=True)
    for c in NUMERIC_COLS:
        models[c] = pd.to_numeric(models[c], errors="coerce")
    return models, _explode_tags(models), _build_edges(models)


def _crc32_keep(series, pct):
    """与 Spark `F.pmod(F.crc32(col), 100) < pct` 逐行等价的布尔掩码。

    Spark 的 crc32 用 java.util.zip.CRC32，Python 的 zlib.crc32 用同一个 CRC-32
    多项式，对同一 UTF-8 字节序列给出**相同**结果（都是无符号 32 位）。
    因此两侧选出的行集合完全相同，可以在同一批行上直接对拍。
    """
    import zlib
    return series.map(lambda s: zlib.crc32(str(s).encode("utf-8")) % 100 < pct)


def _read_parquet_sampled(path, cols, pct, batch_size=200000):
    """逐 batch 读 parquet 并按 crc32 选样，避免把全量读进内存后再过滤。"""
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    parts = []
    for batch in pf.iter_batches(batch_size=batch_size, columns=cols):
        chunk = batch.to_pandas()
        parts.append(chunk[_crc32_keep(chunk["id"], pct)])
    out = pd.concat(parts, ignore_index=True)
    print(f"[local_etl] crc32 选样 pct={pct}% -> {len(out):,} 行", flush=True)
    return out


def _explode_tags(models):
    """tags 长表 (model_id, tag)。"""
    tag_rows = [(m["model_id"], t) for _, m in models.iterrows()
                for t in (m["tags"] or []) if t]
    return pd.DataFrame(tag_rows, columns=["model_id", "tag"])


def _build_edges(models):
    """base_model 边表 (src, dst, relation)。"""
    edge_rows = [(bm, m["model_id"], m["base_relation"] or "base_model")
                 for _, m in models.iterrows() for bm in (m["base_models"] or [])]
    return pd.DataFrame(edge_rows, columns=["src", "dst", "relation"])


def load_bronze():
    """读 Bronze（parquet 或 jsonl）→ (models, model_tags, edges)（与 spark_etl.py 等价）。"""
    manifest = SA.read_manifest(DATA_DIR)
    if SA.bronze_kind(BRONZE) == "parquet":
        return _load_parquet(BRONZE, manifest)

    rows = []
    with open(BRONZE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            mid = rec.get("id")
            created = rec.get("createdAt")
            if not mid or not created:
                continue
            card = rec.get("cardData")
            if isinstance(card, str):
                try:
                    card = json.loads(card)
                except json.JSONDecodeError:
                    card = {}
            card = card if isinstance(card, dict) else {}
            rows.append({
                "model_id": mid,
                "author": mid.split("/")[0],
                "created_at": created,
                "last_modified": rec.get("lastModified"),
                "downloads": rec.get("downloads"),
                "downloadsAllTime": rec.get("downloadsAllTime"),
                "likes": rec.get("likes"),
                "pipeline_tag": rec.get("pipeline_tag"),
                "library_name": rec.get("library_name"),
                "tags": rec.get("tags"),
                "gated": rec.get("gated"),
                "base_models": _norm_base_model(card.get("base_model")),
                "base_relation": None,
                "scraped_at": rec.get("_scraped_at"),
                "source": "hf_api",
                "snapshot_sha": "",
            })

    models = pd.DataFrame(rows).drop_duplicates(subset=["model_id"]).reset_index(drop=True)
    for c in NUMERIC_COLS:
        if c in models.columns:
            models[c] = pd.to_numeric(models[c], errors="coerce")
    return models, _explode_tags(models), _build_edges(models)


def build_gold(models, edges):
    """派生字段（§10）+ Gold 七表（§14）。返回 (base_gold, dict_of_gold_tables)。"""
    today = pd.Timestamp.now(tz="UTC").normalize()
    g = models.copy()
    g["created_ts"] = pd.to_datetime(g["created_at"], utc=True, errors="coerce")
    g["modified_ts"] = pd.to_datetime(g["last_modified"], utc=True, errors="coerce")
    g["model_age_days"] = (today - g["created_ts"]).dt.days
    g["update_interval_days"] = (g["modified_ts"] - g["created_ts"]).dt.days
    for c in NUMERIC_COLS:
        g[c] = g[c].fillna(0)
    g["downloads_log"] = (g["downloads"] + 1).apply(math.log)
    g["likes_log"] = (g["likes"] + 1).apply(math.log)
    g["tag_count"] = g["tags"].apply(lambda t: len(t) if isinstance(t, list) else 0)
    g["year"] = g["created_ts"].dt.year
    g["month_num"] = g["created_ts"].dt.month
    g["created_month"] = g["created_ts"].dt.strftime("%Y-%m")
    g["log_downloads"] = g["downloads_log"]

    children = (edges.groupby("src").size().rename("children_count")
                .reset_index().rename(columns={"src": "model_id"}))
    g = g.merge(children, on="model_id", how="left")
    g["children_count"] = g["children_count"].fillna(0).astype("int64")
    g["family_size"] = g["children_count"] + 1

    n = len(g)
    total_dl = g["downloads"].sum() or 1

    task_monthly = (g.groupby(["created_month", g["pipeline_tag"].fillna("unknown").rename("task")])
                    .size().rename("new_models").reset_index()
                    .rename(columns={"created_month": "month"}))
    library_monthly = (g.groupby(["created_month", g["library_name"].fillna("unknown").rename("library")])
                       .size().rename("new_models").reset_index()
                       .rename(columns={"created_month": "month"}))
    tag_long = g[["created_month", "model_id", "tags"]].explode("tags").dropna(subset=["tags"])
    tag_monthly = (tag_long.groupby(["created_month", "tags"]).size().rename("new_models")
                   .reset_index().rename(columns={"created_month": "month", "tags": "tag"}))

    dl = g[["model_id", "downloads", "downloadsAllTime", "downloads_log"]].sort_values(
        ["downloads", "model_id"], kind="mergesort").reset_index(drop=True)
    dl["rank"] = dl.index + 1
    dl["cum_model_pct"] = dl["rank"] / n
    dl["cum_download_pct"] = dl["downloads"].cumsum() / total_dl

    model_family_stats = (g.groupby("family_size").size().rename("num_models")
                          .reset_index().sort_values("family_size"))

    model_lifecycle = g[["model_id", "created_at", "last_modified",
                         "model_age_days", "update_interval_days"]].copy()

    model_adoption_features = g[["model_id", "model_age_days", "likes", "likes_log",
                                 "pipeline_tag", "library_name", "tag_count",
                                 "children_count", "family_size", "update_interval_days",
                                 "downloads_log"]].rename(columns={
        "library_name": "library", "downloads_log": "label"}).copy()

    # lorenz_points：频数聚合后的累计点（与 spark_etl.py 同口径、同列名）。
    # 每个点都是真实累计值；点的减少来自把同一下载量的模型合并，而非抽样。
    lorenz_parts = []
    for metric in ("downloads", "downloadsAllTime"):
        vals, cnts = np.unique(g[metric].to_numpy(), return_counts=True)
        cum_f = np.cumsum(cnts)
        cum_v = np.cumsum(vals.astype("float64") * cnts)
        tot = float(cum_v[-1]) if len(cum_v) else 0.0
        lorenz_parts.append(pd.DataFrame({
            "metric": metric,
            "value": vals.astype("int64"),
            "freq": cnts.astype("int64"),
            "cum_freq": cum_f.astype("int64"),
            "cum_model_pct": cum_f / int(cnts.sum()),
            "cum_download_pct": cum_v / (tot if tot else 1.0),
        }))

    gold_tables = {
        "task_monthly_stats": task_monthly,
        "library_monthly_stats": library_monthly,
        "tag_monthly_stats": tag_monthly,
        "download_distribution": dl,
        "model_family_stats": model_family_stats,
        "model_lifecycle": model_lifecycle,
        "model_adoption_features": model_adoption_features,
        "lorenz_points": pd.concat(lorenz_parts, ignore_index=True),
    }
    return g, gold_tables


def _write_parquet(df, path):
    """把表写成 <path>/part-00000.parquet。

    ⚠️ 写前清掉同目录下其它引擎遗留的 `part-*.parquet`：
    否则 pd.read_parquet(目录) 会把两份数据一起读进来（表现为行数翻倍/重复 id）。
    """
    os.makedirs(path, exist_ok=True)
    for f in os.listdir(path):
        if f.endswith(".parquet"):
            os.remove(os.path.join(path, f))
    df.to_parquet(os.path.join(path, "part-00000.parquet"), index=False)


def main():
    models, model_tags, edges = load_bronze()
    print(f"[cfg] engine={DIRS['engine']}  bronze={BRONZE}")
    print(f"[bronze] 有效模型: {len(models)}  tags 行: {len(model_tags)}  边: {len(edges)}")

    _write_parquet(models, os.path.join(SILVER, "models"))
    _write_parquet(model_tags, os.path.join(SILVER, "model_tags"))
    _write_parquet(edges, os.path.join(SILVER, "model_base_edges"))
    print(f"[silver] 写出 models / model_tags / model_base_edges -> {SILVER}")

    base_gold, gold_tables = build_gold(models, edges)
    _write_parquet(base_gold, os.path.join(GOLD, "models_enriched"))
    for name, df in gold_tables.items():
        _write_parquet(df, os.path.join(GOLD, name))

    # 集中度统计（与 spark_etl.py 共用 stats_utils，保证两引擎逐位一致）
    concentration = {
        "snapshot_sha": str(models["snapshot_sha"].iloc[0]) if len(models) else "",
        "source": str(models["source"].iloc[0]) if len(models) else "",
        "n_models": int(len(base_gold)),
    }
    for metric in ("downloads", "downloadsAllTime"):
        vals, cnts = np.unique(base_gold[metric].to_numpy(), return_counts=True)
        concentration[metric] = SU.concentration_stats(list(vals), list(cnts))
    with open(os.path.join(GOLD, "concentration_stats.json"), "w", encoding="utf-8") as f:
        json.dump(concentration, f, ensure_ascii=False, indent=2)

    print("[gold] 写出 models_enriched + " + " + ".join(gold_tables.keys())
          + f" + concentration_stats.json -> {GOLD}")
    print(f"[gold] 行数: {len(base_gold)}  Gini(downloads)="
          f"{concentration['downloads']['gini']}  "
          f"Gini(downloadsAllTime)={concentration['downloadsAllTime']['gini']}")


if __name__ == "__main__":
    main()
