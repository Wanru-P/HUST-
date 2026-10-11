#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spark_etl.py — Bronze -> Silver -> Gold ETL（Hugging Face 开源模型生态分析）。

Phase 1A 修订：
  - 新增 Bronze 输入适配：同时支持
      (a) 新正式数据 hub-stats 的 models.parquet（createdAt/lastModified 为 timestamp，
          baseModels 为 struct<relation, models:[{_id,id}]>）；
      (b) 旧 Demo 的 HF API models.jsonl（createdAt 为 ISO 字符串，cardData.base_model）。
  - 统一经 schema_adapter 解析路径：HF_DATA_DIR + HF_ENGINE + HF_BRONZE_PATH，
    Spark 产物写 silver/gold，pandas 产物写 silver_local/gold_local，互不覆盖。
  - 保留旧列名与下游契约；只做**增量列**（base_relation / source / snapshot_sha）。

数据布局（引擎隔离）：
    <HF_DATA_DIR>/bronze/models.parquet    新正式原始数据（或 models.jsonl 旧数据）
    <HF_DATA_DIR>/silver/models            清洗后的模型主表（spark）
    <HF_DATA_DIR>/silver/model_tags         (model_id, tag) 长表
    <HF_DATA_DIR>/silver/model_base_edges   (src=base_model, dst=model_id, relation) 家族边表
    <HF_DATA_DIR>/gold/models_enriched      衍生特征宽表

运行：
    python spark_etl.py
    # 新正式数据（默认）：HF_DATA_DIR=<...>/data_hubstats  HF_ENGINE=spark
    # 旧 Demo：HF_DATA_DIR=<...>/data  HF_BRONZE_PATH=<...>/data/bronze/models.jsonl
"""
import json
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (LongType, StringType, StructType, StructField,
                               TimestampType, TimestampNTZType)
from pyspark.sql.window import Window

import schema_adapter as SA
import stats_utils as SU

# Phase 1A 用小规模起步；全量跑时用环境变量放大，不改代码。
MASTER = os.environ.get("HF_SPARK_MASTER", "local[4]")
DRIVER_MEMORY = os.environ.get("HF_SPARK_DRIVER_MEMORY", "4g")
SHUFFLE_PARTITIONS = os.environ.get("HF_SHUFFLE_PARTITIONS", "16")


def pick(df, name, cast_type):
    """列存在就 cast，不存在就给一个同类型的 NULL（防 schema 漂移导致 select 失败）。
    缺失分支必须显式 alias(name)，否则多个缺失列会被 Spark 自动命名而触发 COLUMN_ALREADY_EXISTS。"""
    if name in df.columns:
        return F.col(name).cast(cast_type)
    return F.lit(None).cast(cast_type).alias(name)


def read_bronze(spark, path):
    """读 Bronze，返回 (DataFrame, kind)。kind ∈ {'parquet','jsonl'}。

    jsonl 分支沿用旧逻辑：把 cardData 强制读成 JSON 字符串列（而非 struct），
    否则 struct cast string 会得到无键元组、get_json_object 取不到 $.base_model。
    """
    kind = SA.bronze_kind(path)
    if kind == "parquet":
        return spark.read.parquet(path), kind
    inferred = spark.read.json(path)
    new_fields = []
    for f in inferred.schema.fields:
        if f.name == "cardData":
            new_fields.append(StructField("cardData", StringType(), True))
        else:
            new_fields.append(f)
    return spark.read.schema(StructType(new_fields)).json(path), kind


def iso_string_col(df, name):
    """把时间列统一成 ISO-8601 UTC 字符串（与旧 Demo 一致）。

    必须同时处理 TimestampType 与 TimestampNTZType：
    hub-stats 的 parquet 里 createdAt 被读成 timestamp_ntz，它不是 TimestampType 的实例，
    漏判会走到 cast(StringType()) 分支，产出 "2026-09-23 01:01:55"（无 Z、空格分隔），
    破坏 ISO-Z 契约。jsonl 源本身已是字符串，原样透传。
    另需配合 spark.sql.session.timeZone=UTC，否则 "...Z" 会按本地时区渲染、月份被平移。
    """
    if name not in df.columns:
        return F.lit(None).cast(StringType()).alias(name)
    if isinstance(df.schema[name].dataType, (TimestampType, TimestampNTZType)):
        return F.date_format(F.col(name).cast("timestamp"), SA.ISO_Z_FORMAT).alias(name)
    return F.col(name).cast(StringType()).alias(name)


def base_models_col(raw, kind):
    """统一提取 base_models: array<string>。
    parquet: baseModels.models[].id（struct 数组）；jsonl: cardData.base_model（字符串或数组）。"""
    if kind == "parquet":
        return (F.when(F.col("baseModels").isNull(), F.lit(None).cast("array<string>"))
                 .otherwise(F.expr("transform(baseModels.models, x -> x.id)")).alias("base_models"))
    bm_json = F.get_json_object(F.col("cardData"), "$.base_model")
    return (F.when(bm_json.isNull(), F.lit(None).cast("array<string>"))
             .when(bm_json.startswith("["), F.from_json(bm_json, "array<string>"))
             .otherwise(F.array(F.regexp_replace(bm_json, r'^"|"$', ""))).alias("base_models"))


def base_relation_col(raw, kind):
    """保留 baseModels.relation（finetune / quantized / ...）。jsonl 源没有该字段 -> NULL。"""
    if kind == "parquet" and "baseModels" in raw.columns:
        return F.col("baseModels.relation").alias("base_relation")
    return F.lit(None).cast(StringType()).alias("base_relation")


def freq_table(df, value_col):
    """按 value_col 做**频数聚合**，返回 (freq_df, n_models, total_value)。

    Phase 1B 用它替换原来的「全局窗口排序」（原写法没有 partitionBy，
    会把 314 万行全挤进 1 个分区）。聚合后表的行数 = **唯一下载量取值数**
    （50,218 行样本实测 downloads 只有 1,352 个唯一值、downloadsAllTime 3,670 个），
    比模型数小三个数量级，因此这里的全局窗口只是对一张很小的表排序，不构成瓶颈。

    freq_df 列：value, freq, cum_freq, cum_value, start_rank, prev_cum_value
      start_rank     = 该取值在全局升序排名中占用的第一个秩（= 前序累计频数 + 1）
      prev_cum_value = 严格小于该取值的所有模型的下载量之和
    这两个列让"逐模型排名"可以退化成"按取值查表 + 组内序号"，无需全局排序。

    空值处理：旧 Demo jsonl 源没有 downloadsAllTime 列（pick() 给出类型化 NULL），
    这里 coalesce 成 0，与 local_etl.build_gold 的 fillna(0) 口径一致；
    新正式数据无空值，此处理不改变任何结果。
    """
    freq = (df.groupBy(F.coalesce(F.col(value_col), F.lit(0)).cast("long").alias("value"))
              .agg(F.count("*").alias("freq")))
    w = Window.orderBy("value")
    freq = (freq
            .withColumn("cum_freq", F.sum("freq").over(w))
            .withColumn("cum_value", F.sum(F.col("value") * F.col("freq")).over(w))
            .withColumn("start_rank", F.col("cum_freq") - F.col("freq") + 1)
            .withColumn("prev_cum_value",
                        F.col("cum_value") - F.col("value") * F.col("freq")))
    n_models = freq.agg(F.sum("freq")).first()[0] or 0
    total_value = freq.agg(F.sum(F.col("value") * F.col("freq"))).first()[0] or 0
    return freq, int(n_models), int(total_value)


def main():
    dirs = SA.resolve_dirs()
    bronze = SA.bronze_path()
    manifest = SA.read_manifest(dirs["root"])
    source = SA.HUBSTATS_DATASET if SA.bronze_kind(bronze) == "parquet" else "hf_api"
    snapshot_sha = manifest.get("commit", "") if manifest else ""
    ingested_at = manifest.get("verified_at", "") if manifest else ""

    print(f"[cfg] engine={dirs['engine']}  bronze={bronze}")
    print(f"[cfg] silver={dirs['silver']}  gold={dirs['gold']}")
    print(f"[cfg] master={MASTER}  driver={DRIVER_MEMORY}  shuffle={SHUFFLE_PARTITIONS}")
    print(f"[cfg] fs.file.impl={SA.SPARK_LOCAL_FS_CONFIGS.get('spark.hadoop.fs.file.impl', 'hadoop 默认')}")

    builder = (SparkSession.builder
               .appName("HF_ETL")
               .master(MASTER)
               .config("spark.driver.memory", DRIVER_MEMORY)
               .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
               .config("spark.sql.parquet.compression.codec", "snappy")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false"))
    for key, value in SA.SPARK_LOCAL_FS_CONFIGS.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()
    print(f"[time] {0.0:8.1f}s  SparkSession 就绪 (master={MASTER}, driver={DRIVER_MEMORY})")

    # ---- Bronze -> Silver: 模型主表 ----
    raw, kind = read_bronze(spark, bronze)
    print(f"[bronze] kind={kind}  字段: {raw.columns}")

    models = (raw
              .select(
                  pick(raw, "id", StringType()).alias("model_id"),
                  pick(raw, "author", StringType()).alias("author"),
                  iso_string_col(raw, "createdAt").alias("created_at"),
                  iso_string_col(raw, "lastModified").alias("last_modified"),
                  pick(raw, "downloads", LongType()),
                  pick(raw, "downloadsAllTime", LongType()),
                  pick(raw, "likes", LongType()),
                  pick(raw, "pipeline_tag", StringType()),
                  pick(raw, "library_name", StringType()),
                  pick(raw, "tags", "array<string>"),
                  F.lit(None).cast(StringType()).alias("gated"),   # 新源无 gated -> 显式类型化 NULL
                  base_models_col(raw, kind),
                  base_relation_col(raw, kind),
                  F.lit(ingested_at).cast(StringType()).alias("scraped_at"),
                  F.lit(source).cast(StringType()).alias("source"),
                  F.lit(snapshot_sha).cast(StringType()).alias("snapshot_sha"),
              )
              .dropna(subset=["model_id", "created_at"])
              .dropDuplicates(["model_id"]))

    # author 兜底：若源缺失，从 id 的命名空间派生
    models = models.withColumn(
        "author", F.when(F.col("author").isNull(), F.split(F.col("model_id"), "/").getItem(0))
                    .otherwise(F.col("author")))

    model_tags = (models
                  .select("model_id", F.explode_outer(F.col("tags")).alias("tag"))
                  .filter(F.col("tag").isNotNull()))

    edges = (models
             .select("model_id", F.explode_outer(F.col("base_models")).alias("base_model"), "base_relation")
             .filter(F.col("base_model").isNotNull())
             .select(F.col("base_model").alias("src"), F.col("model_id").alias("dst"),
                     F.coalesce(F.col("base_relation"), F.lit("base_model")).alias("relation")))

    # ---- Bronze -> Silver ----
    # 执行计划优化（Phase 1B）：原实现里 models 被 3 个 Silver 写 + 若干 Gold 写各重算一次，
    # 即每张表都从 Bronze 重扫一遍（10 张表 ≈ 10 次全量重算）。
    # 现在改为「Bronze 只读一次 -> 落 Silver -> 其余全部从已落盘的 Silver/Gold 读回」。
    # 这属于执行计划层面的优化，不改任何列名与语义。
    models.write.mode("overwrite").parquet(os.path.join(dirs["silver"], "models"))

    silver_models = spark.read.parquet(os.path.join(dirs["silver"], "models"))
    model_tags = (silver_models
                  .select("model_id", F.explode_outer(F.col("tags")).alias("tag"))
                  .filter(F.col("tag").isNotNull()))
    edges = (silver_models
             .select("model_id", F.explode_outer(F.col("base_models")).alias("base_model"),
                     "base_relation")
             .filter(F.col("base_model").isNotNull())
             .select(F.col("base_model").alias("src"), F.col("model_id").alias("dst"),
                     F.coalesce(F.col("base_relation"), F.lit("base_model")).alias("relation")))

    model_tags.write.mode("overwrite").parquet(os.path.join(dirs["silver"], "model_tags"))
    edges.write.mode("overwrite").parquet(os.path.join(dirs["silver"], "model_base_edges"))
    print(f"[silver] 写出 models / model_tags / model_base_edges -> {dirs['silver']}")
    print(f"[time] {time.time() - t0:8.1f}s  Silver 三表写出完成")

    # ---- Gold: §14 七张表 + §10 派生字段 ----
    children = (edges.groupBy("src").count()
                .select(F.col("src").alias("model_id"),
                        F.col("count").cast("long").alias("children_count")))

    base_gold = (silver_models
                 .withColumn("downloads", F.coalesce(F.col("downloads"), F.lit(0)).cast("long"))
                 .withColumn("downloadsAllTime",
                             F.coalesce(F.col("downloadsAllTime"), F.lit(0)).cast("long"))
                 .withColumn("likes", F.coalesce(F.col("likes"), F.lit(0)).cast("long"))
                 .withColumn("created_ts", F.to_timestamp("created_at"))
                 .withColumn("modified_ts", F.to_timestamp("last_modified"))
                 .withColumn("model_age_days", F.datediff(F.current_date(), F.col("created_ts")))
                 .withColumn("update_interval_days", F.datediff("modified_ts", "created_ts"))
                 .withColumn("downloads_log", F.log1p(F.col("downloads")))
                 .withColumn("likes_log", F.log1p(F.col("likes")))
                 .withColumn("tag_count", F.size(F.col("tags")))
                 .withColumn("year", F.year("created_ts"))
                 .withColumn("month_num", F.month("created_ts"))
                 .join(children, on="model_id", how="left")
                 .fillna({"children_count": 0})
                 .withColumn("family_size", F.col("children_count") + 1)
                 .withColumn("created_month", F.date_format("created_ts", "yyyy-MM"))
                 .withColumn("log_downloads", F.col("downloads_log")))
    base_gold.write.mode("overwrite").parquet(os.path.join(dirs["gold"], "models_enriched"))

    # 后续全部从已落盘的 models_enriched 读回（列裁剪后扫描成本远低于重算 base_gold）
    enriched = spark.read.parquet(os.path.join(dirs["gold"], "models_enriched"))
    n_models = enriched.count()
    print(f"[time] {time.time() - t0:8.1f}s  models_enriched 写出并回读完成 (n={n_models})")

    # 注意：必须用 Column 对象分组，不能写字符串 "month"。
    # base_gold 里已有整型列 month_num；早期版本把整型列命名为 month，
    # 导致 groupBy("month") 按名字解析到整型列（1~12），task_monthly_stats 行数从 1343 掉到 492。
    # 这里同时把 pipeline_tag/library_name 的空值补成 "unknown"，与 pandas 引擎的 fillna 对齐。
    month_col = F.col("created_month").alias("month")
    (enriched.groupBy(month_col, F.coalesce(F.col("pipeline_tag"), F.lit("unknown")).alias("task"))
        .agg(F.count("*").alias("new_models"))
        .write.mode("overwrite").parquet(os.path.join(dirs["gold"], "task_monthly_stats")))
    (enriched.groupBy(month_col, F.coalesce(F.col("library_name"), F.lit("unknown")).alias("library"))
        .agg(F.count("*").alias("new_models"))
        .write.mode("overwrite").parquet(os.path.join(dirs["gold"], "library_monthly_stats")))
    (enriched.select(month_col, F.explode_outer("tags").alias("tag"))
        .filter(F.col("tag").isNotNull())
        .groupBy("month", "tag").agg(F.count("*").alias("new_models"))
        .write.mode("overwrite").parquet(os.path.join(dirs["gold"], "tag_monthly_stats")))
    print(f"[time] {time.time() - t0:8.1f}s  Gold 三张月度统计表写出完成")

    # ---- 下载量集中度：频数聚合口径（Phase 1B 替换原全局窗口排序）----
    # 原写法 `Window.orderBy("downloads","model_id")` 没有 partitionBy，
    # Spark 会把全部模型移到单个分区做排序（日志里可见
    # "No Partition Defined for Window operation! Moving all data to a single partition"），
    # 314 万行只能由 1 个 core 串行处理。
    # 新写法：先按下载量取值聚合出频数表（唯一值数 << 模型数），
    # 逐模型 rank 退化为「按取值查表得到起始秩 + 组内按 model_id 排序的序号」，
    # 组内排序按 downloads 分区，可并行，且语义与旧实现**逐行等价**（test_distribution.py 已对拍）。
    freq_dl, _, total_dl = freq_table(enriched, "downloads")
    freq_at, _, total_at = freq_table(enriched, "downloadsAllTime")
    freq_dl.cache()
    freq_at.cache()

    dl = (enriched.select("model_id", "downloads", "downloadsAllTime", "downloads_log")
          .join(freq_dl.select(F.col("value").alias("_v"), "start_rank", "prev_cum_value"),
                F.col("downloads") == F.col("_v"), how="left")
          .withColumn("_off", F.row_number().over(
              Window.partitionBy("downloads").orderBy("model_id")) - 1)
          .withColumn("rank", F.col("start_rank") + F.col("_off"))
          .withColumn("cum_model_pct", F.col("rank") / F.lit(n_models))
          .withColumn("cum_download_pct",
                      (F.col("prev_cum_value") + (F.col("_off") + 1) * F.col("downloads"))
                      / F.lit(total_dl if total_dl else 1))
          .drop("_v", "start_rank", "prev_cum_value", "_off"))
    dl.write.mode("overwrite").parquet(os.path.join(dirs["gold"], "download_distribution"))
    print(f"[time] {time.time() - t0:8.1f}s  download_distribution 写出完成")

    # 新增 lorenz_points：频数聚合后的累计点（每个点都是真实累计值，
    # 点的"变少"来自把同一下载量的模型合并，而不是抽样/插值）。
    # 两种口径各自独立成 metric 分组，绝不混合。
    lorenz = None
    for metric, ft, tot in (("downloads", freq_dl, total_dl),
                            ("downloadsAllTime", freq_at, total_at)):
        part = ft.select(
            F.lit(metric).alias("metric"),
            F.col("value").cast("long").alias("value"),
            F.col("freq").cast("long").alias("freq"),
            F.col("cum_freq").cast("long").alias("cum_freq"),
            (F.col("cum_freq") / F.lit(n_models)).alias("cum_model_pct"),
            (F.col("cum_value") / F.lit(tot if tot else 1)).alias("cum_download_pct"))
        lorenz = part if lorenz is None else lorenz.unionByName(part)
    lorenz.write.mode("overwrite").parquet(os.path.join(dirs["gold"], "lorenz_points"))
    print(f"[time] {time.time() - t0:8.1f}s  lorenz_points 写出完成")

    freq_dl_rows = freq_dl.orderBy("value").select("value", "freq").collect()
    freq_at_rows = freq_at.orderBy("value").select("value", "freq").collect()
    stats = {
        "snapshot_sha": snapshot_sha,
        "source": source,
        "n_models": n_models,
        "downloads": SU.concentration_stats([r[0] for r in freq_dl_rows],
                                            [r[1] for r in freq_dl_rows]),
        "downloadsAllTime": SU.concentration_stats([r[0] for r in freq_at_rows],
                                                   [r[1] for r in freq_at_rows]),
    }
    with open(os.path.join(dirs["gold"], "concentration_stats.json"), "w",
              encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"[gold] 集中度统计（频数加权口径）: Gini(downloads)={stats['downloads']['gini']} "
          f"Gini(downloadsAllTime)={stats['downloadsAllTime']['gini']}")
    print(f"[gold] 唯一取值数: downloads={stats['downloads']['distinct_values']} "
          f"downloadsAllTime={stats['downloadsAllTime']['distinct_values']}")
    freq_dl.unpersist()
    freq_at.unpersist()

    (enriched.groupBy("family_size").agg(F.count("*").alias("num_models"))
        .orderBy("family_size")
        .write.mode("overwrite").parquet(os.path.join(dirs["gold"], "model_family_stats")))

    (enriched.select("model_id", "created_at", "last_modified",
                     "model_age_days", "update_interval_days")
        .write.mode("overwrite").parquet(os.path.join(dirs["gold"], "model_lifecycle")))

    (enriched.select("model_id", "model_age_days", "likes", "likes_log",
                     "pipeline_tag", F.col("library_name").alias("library"),
                     "tag_count", "children_count", "family_size",
                     "update_interval_days",
                     F.col("downloads_log").alias("label"))
        .write.mode("overwrite").parquet(os.path.join(dirs["gold"], "model_adoption_features")))

    print(f"[gold] 写出 models_enriched + 七表 + lorenz_points -> {dirs['gold']}")
    print(f"[gold] 行数: {n_models}")
    print(f"[time] {time.time() - t0:8.1f}s  全量 ETL 结束")

    spark.stop()


if __name__ == "__main__":
    main()
