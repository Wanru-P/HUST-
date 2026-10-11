#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis_evo.py — RQ1（生态演化）+ RQ2（注意力集中与长尾）。

RQ1 研究对象（**必须写清楚**）：
    本数据是**某个固定时间点的公开模型仓库快照**。因此研究对象是
    「快照中仍然可见的模型的创建时间分布」，**不是**历史上所有曾经发布过的模型的完整记录。
    已被删除/下架的历史模型不在数据里，早期月份的绝对量会偏低。

RQ2 口径（**两种独立输出，绝不混合**）：
    `downloads`        近 30 天下载计数
    `downloadsAllTime` 历史累计下载计数
    Gini 由 `stats_utils.gini_freq_weighted`（频数加权秩公式）精确计算，非采样近似；
    Top-k 用明确排名定义：按值降序取前 max(1, int(n·p)) 个模型，并列值整体同权。
    **不宣称幂律**——未做严格的分布拟合与模型比较，只描述长尾形态。

输出（`gold/analysis/`）：
    monthly_new_models.csv / pipeline_trend.csv / library_trend.csv / tag_trend_top.csv
    lorenz_curve.csv / ccdf.csv / summary_rq1.json / summary_rq2.json

用法：python analysis_evo.py
"""
import json
import os
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
GOLD = DIRS["gold"]
ANALYSIS = os.path.join(GOLD, "analysis")
os.makedirs(ANALYSIS, exist_ok=True)

TOP_TAGS = 15
TOP_TASKS = 10


def save(df, name):
    df.to_csv(os.path.join(ANALYSIS, name), index=False)
    print(f"[rq1/rq2] {name:26s} rows={len(df)}")


def main():
    spark = SS.build("HF_Analysis_Evo")
    enriched = spark.read.parquet(os.path.join(GOLD, "models_enriched"))
    n_models = enriched.count()

    # ================= RQ1 =================
    monthly = (enriched.groupBy("created_month").agg(F.count("*").alias("new_models"))
               .orderBy("created_month").toPandas())
    monthly = monthly.rename(columns={"created_month": "month"})

    # 快照截止到哪一天 -> 那个月是不完整月份（不能与其他完整月份直接比较）
    snap_max = enriched.agg(F.max("created_at")).first()[0]
    snap_month = str(snap_max)[:7]
    monthly["is_partial"] = monthly["month"] == snap_month
    save(monthly, "monthly_new_models.csv")

    # 任务趋势：task_monthly_stats 已把 pipeline_tag 空值填成 "unknown"，总量守恒
    task_trend = (spark.read.parquet(os.path.join(GOLD, "task_monthly_stats"))
                  .orderBy("month", "task").toPandas())
    save(task_trend, "pipeline_trend.csv")

    lib_trend = (spark.read.parquet(os.path.join(GOLD, "library_monthly_stats"))
                 .orderBy("month", "library").toPandas())
    save(lib_trend, "library_trend.csv")

    tag_full = spark.read.parquet(os.path.join(GOLD, "tag_monthly_stats"))
    top_tags = [r["tag"] for r in
                tag_full.groupBy("tag").agg(F.sum("new_models").alias("t"))
                .orderBy(F.desc("t")).limit(TOP_TAGS).collect()]
    tag_top = (tag_full.filter(F.col("tag").isin(top_tags))
               .orderBy("month", "tag").toPandas())
    save(tag_top, "tag_trend_top.csv")

    task_totals = (task_trend.groupby("task")["new_models"].sum()
                   .sort_values(ascending=False))
    unknown_share = float(task_totals.get("unknown", 0) / n_models)

    # ---- 日期地板检验：最早的创建日是否出现异常堆积 ----
    # 若某个单日集中了首月的绝大多数模型，该日数值就**不能**当作真实发布量。
    # 本检验只量化这一聚集现象，不对其成因（元数据迁移 / 时间戳记录下限 / 其它）
    # 下任何结论：仅凭本快照无法区分这些解释，缺证据时只报告已观测到的现象。
    days = (enriched.withColumn("created_day", F.substring("created_at", 1, 10))
            .groupBy("created_day").agg(F.count("*").alias("new_models"))
            .orderBy("created_day"))
    first_day = days.first()
    earliest_day = str(first_day["created_day"])
    earliest_day_count = int(first_day["new_models"])
    first_month_count = int(monthly["new_models"].iloc[0])
    date_floor_share = round(earliest_day_count / first_month_count, 6) if first_month_count else None

    # 进一步刻画：统计最早一天上的**不同时间戳个数**与众数时间戳占比。
    # 这一步只产出「已观测到的现象」证据（同一时间戳高度聚集），
    # 不用于推断根因 —— 快照本身无法区分「平台元数据迁移写入」与「时间戳记录下限」。
    day_ts = (enriched.filter(F.substring("created_at", 1, 10) == earliest_day)
              .groupBy("created_at").agg(F.count("*").alias("n"))
              .orderBy(F.desc("n")))
    n_day_ts = day_ts.count()
    modal = day_ts.first()
    modal_ts = str(modal["created_at"])
    modal_count = int(modal["n"])
    modal_share = round(modal_count / earliest_day_count, 6) if earliest_day_count else None

    rq1 = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "spark",
        "snapshot_sha": SA.HUBSTATS_COMMIT,
        "n_models": int(n_models),
        "first_month": str(monthly["month"].iloc[0]),
        "last_month": str(monthly["month"].iloc[-1]),
        "n_months": int(len(monthly)),
        "snapshot_max_created_at": str(snap_max),
        "partial_month": snap_month,
        "partial_month_note": (
            f"{snap_month} 是快照截止所在月，尚未走完，其新增数不能与其他完整月份直接比较。"),
        "peak_month": str(monthly.loc[monthly["new_models"].idxmax(), "month"]),
        "peak_count": int(monthly["new_models"].max()),
        "earliest_day": earliest_day,
        "earliest_day_count": earliest_day_count,
        "earliest_day_distinct_timestamps": int(n_day_ts),
        "earliest_day_modal_timestamp": modal_ts,
        "earliest_day_modal_count": modal_count,
        "earliest_day_modal_share": modal_share,
        "first_month_count": first_month_count,
        "date_floor_share": date_floor_share,
        "date_floor_note": (
            f"{earliest_day} 单日即集中 {earliest_day_count:,} 个模型，占首月 "
            f"{first_month_count:,} 个的 {date_floor_share:.1%}；而该日只有 {n_day_ts} 个不同时间戳，"
            f"其中 {modal_ts} 一个时间戳就占 {modal_count:,} 个（{modal_share:.1%}）。"
            "以上是**已观测到的现象**：同一时间戳在最早一天高度聚集，与逐日发布过程不符，"
            "因此不能解读为「自然发布高峰」。仅凭本快照**无法确认根因**"
            "（既可能是平台早期元数据的迁移写入，也可能是时间戳记录下限，二者都缺少可核验证据），"
            "故不作因果推断；受此影响，首月数值不可与其他月份直接比较。"),
        "unknown_task_models": int(task_totals.get("unknown", 0)),
        "unknown_task_share": round(unknown_share, 6),
        "unknown_note": (
            "pipeline_tag 缺失率约 65.6%。缺失未被推断成任何具体任务，"
            "统一标记为 unknown 并单独报告；所有按任务的结论只覆盖已标注任务的模型。"),
        "top10_tasks_by_total": {k: int(v) for k, v in task_totals.head(TOP_TASKS).items()},
    }
    with open(os.path.join(ANALYSIS, "summary_rq1.json"), "w", encoding="utf-8") as f:
        json.dump(rq1, f, ensure_ascii=False, indent=2)

    # ================= RQ2 =================
    with open(os.path.join(GOLD, "concentration_stats.json"), encoding="utf-8") as f:
        cs = json.load(f)

    lorenz = spark.read.parquet(os.path.join(GOLD, "lorenz_points")).toPandas()
    lorenz[["metric", "cum_model_pct", "cum_download_pct"]].to_csv(
        os.path.join(ANALYSIS, "lorenz_curve.csv"), index=False)
    print(f"[rq1/rq2] lorenz_curve.csv           rows={len(lorenz)}")

    # CCDF：P(X >= x)。由频数表推得：值 v 处，X >= v 的模型数 = n - 严格小于 v 的累计频数
    ccdf_rows = []
    for metric, sub in lorenz.groupby("metric"):
        sub = sub.sort_values("value")
        n = int(sub["freq"].sum())
        prev_cum = sub["cum_freq"] - sub["freq"]
        pos = sub[sub["value"] > 0]
        ccdf_rows.append(pd.DataFrame({
            "metric": metric,
            "value": pos["value"].to_numpy(),
            "ccdf": (n - prev_cum[pos.index].to_numpy()) / n}))
    ccdf = pd.concat(ccdf_rows, ignore_index=True)
    ccdf.to_csv(os.path.join(ANALYSIS, "ccdf.csv"), index=False)
    print(f"[rq1/rq2] ccdf.csv                   rows={len(ccdf)}")

    rq2 = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "spark",
        "snapshot_sha": SA.HUBSTATS_COMMIT,
        "n_models": int(n_models),
        "method": {
            "gini": "频数加权秩公式 G=2S/(n·total)-1，精确值，非采样近似",
            "topk": "按值降序取前 max(1,int(n·p)) 个模型；并列值整体同权，结果可复现",
            "lorenz": "频数聚合后的真实累计点，未抽样、未插值",
        },
        "downloads": cs["downloads"],
        "downloadsAllTime": cs["downloadsAllTime"],
        "metric_note": (
            "downloads 为近 30 天窗口口径，downloadsAllTime 为历史累计口径，"
            "两者相差约 27 倍，任何集中度结论都必须声明所用口径，不可混用。"),
        "powerlaw_note": (
            "本分析只描述长尾形态（Lorenz 偏离、CCDF 双对数近似直线），"
            "**未**做严格幂律拟合与模型比较，因此不宣称已证明幂律分布。"),
    }
    with open(os.path.join(ANALYSIS, "summary_rq2.json"), "w", encoding="utf-8") as f:
        json.dump(rq2, f, ensure_ascii=False, indent=2)

    print(json.dumps({"rq1": {k: rq1[k] for k in
                              ("n_models", "first_month", "last_month", "peak_month",
                               "peak_count", "partial_month", "unknown_task_share")},
                      "rq2_downloads": cs["downloads"],
                      "rq2_downloadsAllTime": cs["downloadsAllTime"]},
                     ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
