# -*- coding: utf-8 -*-
"""Part B · RQ4 时间切分对照（补充分析）。

同一份特征、同一份 20% 确定性哈希建模样本、同一套模型，唯一变量是 train/test 切分方式：
  random   : 随机 80/20（seed=42，即原报告口径）
  temporal : 按 createdAt 排序，最老 80% 训练 / 最新 20% 测试
  calendar : 2026-01-01 之前训练 / 之后测试
若 temporal 的 R² 明显低于 random，说明模型性能对创建时间队列的分布差异较敏感。
⚠️ 特征与目标均取自同一快照，该差值**不能**唯一归因于「时间泄漏」（队列分布变化、
   训练样本构成等同样可能贡献），也不代表严格的未来热度预测。

用法：
    HF_BRONZE_PATH=/path/to/models.parquet python supplementary/time_split_rq4.py
输入：原始 models.parquet（默认 data_hubstats/bronze/models.parquet）
输出：supplementary/rq4_time_split_result.json
依赖：pandas / pyarrow / scikit-learn（见 requirements.txt 之外需另装）。
"""
import json
import math
import os
import zlib

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import OneHotEncoder

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET = os.environ.get(
    "HF_BRONZE_PATH",
    os.path.join(PROJECT_ROOT, "data_hubstats", "bronze", "models.parquet"))
OUT_DIR = os.environ.get("SUPP_OUT_DIR", os.path.join(PROJECT_ROOT, "supplementary"))
OUT_PATH = os.path.join(OUT_DIR, "rq4_time_split_result.json")
SNAPSHOT_DATE = pd.Timestamp("2026-10-11", tz="UTC")   # 与 model_age_days 口径一致
SAMPLE_MODULUS = 5       # crc32(id)%5==0 -> 20% 建模样本（与 analysis_ml.py 一致）
SEED = 42
TOP_LIBRARIES = 50
NUM_FEATURES = ["likes_log", "model_age_days", "tag_count", "children_count",
                "maintenance_span_days"]
COLS = ["id", "createdAt", "lastModified", "downloads", "likes",
        "pipeline_tag", "library_name", "tags", "baseModels"]


def crc32_keep(ids, mod):
    return np.array([zlib.crc32(str(s).encode("utf-8")) % mod == 0 for s in ids])


def extract_base_ids(bm):
    """baseModels: {relation, models:[{_id,id},...]} -> list[str] 底座 id。
    ⚠️ pyarrow 读出的 `models` 是 numpy object array，须接受 list/tuple/ndarray。"""
    if bm is None:
        return []
    if isinstance(bm, dict):
        models = bm.get("models")
        if isinstance(models, (list, tuple, np.ndarray)):
            return [str(m["id"]) for m in models
                    if isinstance(m, dict) and m.get("id")]
    return []


def tag_len(t):
    return len(t) if isinstance(t, (list, tuple, np.ndarray)) else 0


def load_and_featurize():
    print("[1/5] 读 parquet ...", flush=True)
    raw = pd.read_parquet(PARQUET, columns=COLS)
    print(f"      原始行数 = {len(raw):,}", flush=True)
    # 与正式工程统一去重口径：源数据有 5 个重复 id。去重后血缘边 = 919,839（与报告一致），
    # 未去重则为 919,843。特征构建必须与正式工程同口径，随机/时间切分的比较才干净。
    raw = raw.drop_duplicates(subset=["id"]).reset_index(drop=True)
    print(f"      去重后行数 = {len(raw):,}", flush=True)

    print("[2/5] 派生特征 ...", flush=True)
    g = pd.DataFrame({
        "model_id": raw["id"],
        "created_ts": pd.to_datetime(raw["createdAt"], utc=True, errors="coerce"),
        "modified_ts": pd.to_datetime(raw["lastModified"], utc=True, errors="coerce"),
        "downloads": pd.to_numeric(raw["downloads"], errors="coerce").fillna(0),
        "likes": pd.to_numeric(raw["likes"], errors="coerce").fillna(0),
        "pipeline_tag": raw["pipeline_tag"],
        "library_name": raw["library_name"],
    })
    g["likes_log"] = np.log1p(g["likes"])
    g["tag_count"] = raw["tags"].apply(tag_len)
    g["maintenance_span_days"] = (g["modified_ts"] - g["created_ts"]).dt.days
    g["model_age_days"] = (SNAPSHOT_DATE - g["created_ts"]).dt.days
    g["label"] = np.log1p(g["downloads"])
    g["task"] = g["pipeline_tag"].fillna("unknown")

    print("[3/5] children_count（baseModels 入度）...", flush=True)
    edges = [(base, mid) for mid, bm in zip(raw["id"], raw["baseModels"])
             for base in extract_base_ids(bm)]
    edf = pd.DataFrame(edges, columns=["src", "dst"])
    children = edf.groupby("src").size().rename("children_count")
    g = g.merge(children, left_on="model_id", right_index=True, how="left")
    g["children_count"] = g["children_count"].fillna(0).astype("int64")
    print(f"      边数 = {len(edf):,}", flush=True)

    g = g.dropna(subset=["created_ts", "model_id"]).reset_index(drop=True)
    print(f"      有效模型 = {len(g):,}", flush=True)
    return g


def add_lib_top(df, top_libs):
    """把非 Top-N 库统一并入 other（模块级定义，避免嵌套函数）。"""
    return df.assign(library_top=df["library_name"].where(
        df["library_name"].isin(top_libs), "other").fillna("other"))


def onehot_matrix(train, test, top_libs):
    tr, te = add_lib_top(train, top_libs), add_lib_top(test, top_libs)
    enc = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    enc.fit(tr[["task", "library_top"]])
    Xtr = np.hstack([enc.transform(tr[["task", "library_top"]]),
                     tr[NUM_FEATURES].to_numpy(dtype=float)])
    Xte = np.hstack([enc.transform(te[["task", "library_top"]]),
                     te[NUM_FEATURES].to_numpy(dtype=float)])
    return Xtr, Xte


def evaluate(Xtr, ytr, Xte, yte):
    models = {
        "Baseline_mean": None,
        "LinearRegression": Ridge(alpha=0.1, max_iter=2000, random_state=SEED),
        "RandomForest": RandomForestRegressor(n_estimators=60, max_depth=12,
                                              random_state=SEED, n_jobs=-1),
        "GBT": GradientBoostingRegressor(n_estimators=40, max_depth=6,
                                         random_state=SEED),
    }
    out = {}
    base = np.full(len(yte), ytr.mean())
    out["Baseline_mean"] = {"r2": r2_score(yte, base),
                            "rmse": math.sqrt(mean_squared_error(yte, base)),
                            "mae": mean_absolute_error(yte, base)}
    for name, m in models.items():
        if m is None:
            continue
        m.fit(Xtr, ytr)
        pred = m.predict(Xte)
        out[name] = {"r2": r2_score(yte, pred),
                     "rmse": math.sqrt(mean_squared_error(yte, pred)),
                     "mae": mean_absolute_error(yte, pred)}
    return out


def run_split(g, split_name, train_idx, test_idx, top_libs):
    tr, te = g.iloc[train_idx], g.iloc[test_idx]
    Xtr, Xte = onehot_matrix(tr, te, top_libs)
    res = evaluate(Xtr, tr["label"].to_numpy(), Xte, te["label"].to_numpy())
    print(f"\n===== {split_name}  train={len(tr):,}  test={len(te):,} =====", flush=True)
    for k, v in res.items():
        print(f"  {k:<15} RMSE={v['rmse']:.4f}  MAE={v['mae']:.4f}  R2={v['r2']:.4f}",
              flush=True)
    return res


def main():
    g = load_and_featurize()

    print("[4/5] 确定性哈希 20% 建模样本（crc32(id)%5==0）...", flush=True)
    sample_mask = crc32_keep(g["model_id"], SAMPLE_MODULUS)
    s = g[sample_mask].reset_index(drop=True)
    print(f"      建模样本 = {len(s):,}", flush=True)

    rng = np.random.RandomState(SEED)
    perm = rng.permutation(len(s))
    n_train = int(len(s) * 0.8)

    # random
    train_idx, test_idx = perm[:n_train], perm[n_train:]
    top_libs_r = s.iloc[train_idx]["library_name"].value_counts().head(TOP_LIBRARIES).index.tolist()
    res_r = run_split(s, "random (seed=42, 80/20)", train_idx, test_idx, top_libs_r)

    # temporal
    order = np.argsort(s["created_ts"].to_numpy(), kind="mergesort")
    train_idx, test_idx = order[:n_train], order[n_train:]
    cutoff = str(s["created_ts"].iloc[train_idx].max().date())
    top_libs_t = s.iloc[train_idx]["library_name"].value_counts().head(TOP_LIBRARIES).index.tolist()
    res_t = run_split(s, f"temporal (createdAt < {cutoff})", train_idx, test_idx, top_libs_t)

    # calendar
    cutoff2 = pd.Timestamp("2026-01-01", tz="UTC")
    train_idx = np.where(s["created_ts"] < cutoff2)[0]
    test_idx = np.where(s["created_ts"] >= cutoff2)[0]
    top_libs_c = s.iloc[train_idx]["library_name"].value_counts().head(TOP_LIBRARIES).index.tolist()
    res_c = run_split(s, "calendar (train<2026-01-01, test>=2026-01-01)",
                      train_idx, test_idx, top_libs_c)

    print("\n[5/5] 汇总（R²）：", flush=True)
    for name in res_r:
        print(f"  {name:<15} random={res_r[name]['r2']:.4f}  "
              f"temporal={res_t[name]['r2']:.4f}  calendar={res_c[name]['r2']:.4f}",
              flush=True)

    out = {
        "snapshot": str(SNAPSHOT_DATE.date()),
        "n_sample": int(len(s)),
        "cutoffs": {"temporal": cutoff, "calendar": "2026-01-01"},
        "models": {name: {
            "random": round(res_r[name]["r2"], 6),
            "temporal": round(res_t[name]["r2"], 6),
            "calendar": round(res_c[name]["r2"], 6),
        } for name in res_r},
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n-> {OUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
