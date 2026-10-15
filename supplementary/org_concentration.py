# -*- coding: utf-8 -*-
"""① 发布账号层面的下载量集中度分析（author 维度）—— 在原始快照上补做 RQ2 缺失的分析维度。

术语（重要）：`author` 字段是**发布账号**，同时包含个人账号与机构账号，
因此本文一律称「发布账号」，不称「组织」，也不称「机构」。
（内部变量名/JSON 键名沿用历史命名 account/org，仅为兼容下游脚本与图表。）

问题：模型层 Gini = 0.995355，这个极端集中是否同时体现在发布账号层面？
数据里早有 author 字段，但从未启用。

附带检验：RQ4 里 tag_count 是最强特征（重要性 0.413），它与发布账号规模
之间是否存在**简单线性相关**（只报相关系数，不做代理/因果判定）。

用法：
    python org_concentration.py
输出：org_concentration_result.json（同目录）
"""
import json
import os

import numpy as np
import pandas as pd

def _find_parquet():
    """原始快照位置：优先环境变量 HF_BRONZE_PATH，其次仓库约定位置，否则报错退出。"""
    p = os.environ.get("HF_BRONZE_PATH")
    if p and os.path.exists(p):
        return p
    here = os.path.dirname(os.path.abspath(__file__))
    for up in (here, os.path.dirname(here), os.path.dirname(os.path.dirname(here))):
        cand = os.path.join(up, "data_hubstats", "bronze", "models.parquet")
        if os.path.exists(cand):
            return cand
    raise SystemExit(
        "找不到原始快照 models.parquet。请用环境变量指定，例如：\n"
        "    HF_BRONZE_PATH=/path/to/models.parquet python org_concentration.py")


PARQUET = _find_parquet()
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "org_concentration_result.json")


def gini(x):
    """标准 Gini（升序排序公式）。x 为非负数组。"""
    x = np.sort(np.asarray(x, dtype=float))
    n = x.size
    s = x.sum()
    if n == 0 or s <= 0:
        return float("nan")
    idx = np.arange(1, n + 1, dtype=float)
    return float((2.0 * (idx * x).sum()) / (n * s) - (n + 1.0) / n)


def theil_t(x):
    """Theil T。

    标准 Theil T 允许通过极限约定 0·ln0 = 0 纳入零值；
    **本实验主动选择只在正下载模型上计算 Theil**，零值占比单独报告。
    """
    x = np.asarray(x, dtype=float)
    x = x[x > 0]
    if x.size == 0:
        return float("nan")
    mu = x.mean()
    return float(np.mean((x / mu) * np.log(x / mu)))


def main():
    print("[1/4] 读 parquet ...", flush=True)
    raw = pd.read_parquet(PARQUET, columns=["id", "author", "downloads",
                                            "downloadsAllTime", "tags", "likes"])
    # 与项目正式口径统一：按 id 去重（源数据有 5 个重复 id）
    raw = raw.drop_duplicates(subset=["id"]).reset_index(drop=True)
    print(f"      去重后行数 = {len(raw):,}", flush=True)

    d = pd.DataFrame({
        "id": raw["id"].astype(str),
        "author": raw["author"].astype(str),
        "downloads": pd.to_numeric(raw["downloads"], errors="coerce").fillna(0).to_numpy(),
        "downloadsAllTime": pd.to_numeric(raw["downloadsAllTime"], errors="coerce").fillna(0).to_numpy(),
        "tag_count": raw["tags"].apply(lambda t: len(t) if isinstance(t, (list, tuple, np.ndarray)) else 0).to_numpy(),
    })
    # author 缺失时用 repo id 的前缀兜底
    derived = d["id"].str.split("/").str[0]
    miss = d["author"].isin(["None", "nan", ""]) | d["author"].isna()
    print(f"      author 缺失/空 = {int(miss.sum()):,}（{miss.mean():.6%}）→ 用 id 前缀兜底", flush=True)
    # d["org"] 内部键名沿用历史命名，语义 = 发布账号
    d["org"] = np.where(miss, derived, d["author"])
    zero_share = float((d["downloads"] == 0).mean())

    # ---- 1) 模型层 vs 发布账号层 ----
    print("[2/4] 模型层 / 发布账号层集中度 ...", flush=True)
    g_model = gini(d["downloads"].to_numpy())
    g_model_all = gini(d["downloadsAllTime"].to_numpy())

    org = d.groupby("org").agg(
        n_models=("id", "size"),
        dl=("downloads", "sum"),
        dl_all=("downloadsAllTime", "sum"),
        tag_mean=("tag_count", "mean"),
    ).sort_values("dl", ascending=False)
    n_orgs = len(org)
    g_org = gini(org["dl"].to_numpy())
    g_org_all = gini(org["dl_all"].to_numpy())
    print(f"      模型数 = {len(d):,}   发布账号数 = {n_orgs:,}", flush=True)
    print(f"      模型层 Gini(downloads)            = {g_model:.6f}", flush=True)
    print(f"      发布账号层 Gini(账号累计下载)     = {g_org:.6f}", flush=True)
    print(f"      模型层 Gini(downloadsAllTime)     = {g_model_all:.6f}", flush=True)
    print(f"      发布账号层 Gini(累计)             = {g_org_all:.6f}", flush=True)
    print(f"      零下载模型占比（单独报告）        = {zero_share:.6%}", flush=True)

    # ---- 2) Theil 组内/组间分解（按发布账号分组，本实验只在正下载模型上计算）----
    print("[3/4] Theil 分解（按发布账号分组，本实验只计 downloads>0）...", flush=True)
    pos = d[d["downloads"] > 0]
    T_total = theil_t(pos["downloads"].to_numpy())
    mu = pos["downloads"].mean()
    tot = pos["downloads"].sum()
    grp = pos.groupby("org")["downloads"].agg(["size", "sum", "mean"])
    s_g = grp["sum"] / tot
    T_between = float((s_g * np.log(grp["mean"] / mu)).sum())
    T_within = T_total - T_between
    share_between = T_between / T_total if T_total else float("nan")
    print(f"      Theil T 总 = {T_total:.6f}", flush=True)
    print(f"        组间 = {T_between:.6f}  占比 {share_between:.4%}", flush=True)
    print(f"        组内 = {T_within:.6f}  占比 {1 - share_between:.4%}", flush=True)

    # ---- 3) tag_count 与发布账号规模的简单线性相关 ----
    print("[4/4] tag_count vs 发布账号规模（简单线性相关）...", flush=True)

    def corr(a, b):
        """Pearson 简单线性相关系数（不做任何模型/因果推断）。"""
        a = np.asarray(a, dtype=float)
        b = np.asarray(b, dtype=float)
        m = np.isfinite(a) & np.isfinite(b)
        if m.sum() < 3:
            return float("nan")
        return float(np.corrcoef(a[m], b[m])[0, 1])

    d2 = d.join(org["n_models"], on="org")
    ld = np.log1p(d2["downloads"].to_numpy())
    tc = d2["tag_count"].to_numpy(dtype=float)
    osz = d2["n_models"].to_numpy(dtype=float)
    c_tag_dl = corr(tc, ld)
    c_size_dl = corr(osz, ld)
    c_tag_size = corr(tc, osz)

    # 组内去均值后再看 tag_count 与 downloads 的简单线性相关（账号规模被消掉）
    d2 = d2.assign(ld=ld)
    d2["tc_c"] = d2["tag_count"] - d2.groupby("org")["tag_count"].transform("mean")
    d2["ld_c"] = d2["ld"] - d2.groupby("org")["ld"].transform("mean")
    c_tag_dl_within = corr(d2["tc_c"], d2["ld_c"])
    print(f"      corr(tag_count, log1p downloads)              = {c_tag_dl:+.4f}", flush=True)
    print(f"      corr(发布账号模型数, log1p downloads)         = {c_size_dl:+.4f}", flush=True)
    print(f"      corr(tag_count, 发布账号模型数)               = {c_tag_size:+.4f}", flush=True)
    print(f"      【组内去均值】corr(tag_count, downloads)      = {c_tag_dl_within:+.4f}", flush=True)

    out = {
        "terminology": {
            "grouping_field": "author",
            "grouping_unit": "发布账号（同时包含个人账号与机构账号，不称「组织」）",
            "grouping_unit_en": "publishing account",
            "internal_key_note": "JSON/CSV 中沿用历史键名 org/account，语义均为发布账号",
            "prefix_note": "血缘「系」= 底座模型 id 的官方发布账号前缀（org 段），不是真实架构类型归因",
        },
        "n_models": int(len(d)),
        "n_accounts": int(n_orgs),
        "n_orgs": int(n_orgs),          # 兼容历史键名
        "author_missing_share": round(float(miss.mean()), 8),
        "zero_download_share": round(zero_share, 8),
        "gini_model_downloads": round(g_model, 6),
        "gini_org_downloads": round(g_org, 6),
        "gini_model_downloadsAllTime": round(g_model_all, 6),
        "gini_org_downloadsAllTime": round(g_org_all, 6),
        "theil": {
            "total": round(T_total, 6),
            "between": round(T_between, 6),
            "within": round(T_within, 6),
            "between_share": round(share_between, 6),
            "within_share": round(1 - share_between, 6),
            "note": "在正下载模型上按发布账号分组计算；本实验选择只在正下载模型上计算 Theil，"
                    "零值占比单独报告（标准 Theil T 允许经 0·ln0=0 的极限约定纳入零值）。",
        },
        "tag_count_proxy": {
            "corr_type": "Pearson 简单线性相关（不是模型特征重要性，也不构成因果或代理判定）",
            "corr_tagcount_logdownloads": round(c_tag_dl, 6),
            "corr_orgsize_logdownloads": round(c_size_dl, 6),
            "corr_tagcount_orgsize": round(c_tag_size, 6),
            "corr_tagcount_logdownloads_within_org": round(c_tag_dl_within, 6),
        },
        "top20_orgs_by_downloads": [
            {"account": i, "n_models": int(r.n_models), "downloads": int(r.dl),
             "downloadsAllTime": int(r.dl_all), "tag_mean": round(float(r.tag_mean), 3)}
            for i, r in org.head(20).iterrows()],
        "top20_orgs_by_n_models": [
            {"account": i, "n_models": int(r.n_models), "downloads": int(r.dl)}
            for i, r in org.sort_values("n_models", ascending=False).head(20).iterrows()],
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n-> {OUT}", flush=True)


if __name__ == "__main__":
    main()
