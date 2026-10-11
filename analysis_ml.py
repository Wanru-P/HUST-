#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis_ml.py — RQ4：模型属性与「当前下载热度」之间的统计关联。

⚠️ 研究定位（必须与报告口径一致）
    本分析研究的是**当前公开快照下**模型属性与下载热度之间的**统计关联**，
    **不是**"预测未来是否会火"。原因：label 与 likes 都取自同一个时间点的快照，
    二者天然存在同期相关（contemporaneous correlation），
    因此模型的"预测能力"不能包装成前瞻性预测能力。

⚠️ 原实现的两个问题（本次修复）
    1. 在**全量数据**上 fit，又在**同一份数据**上 evaluate —— 训练集自评，指标虚高。
       现在：先划分 train/test，所有预处理（StringIndexer/OneHot/词表）只在 train 上 fit，
       指标只在 test 上计算。
    2. `update_interval_days` 被当作"更新频率"，但它的定义是
       lastModified - createdAt，即**维护跨度**（天），不是更新次数/频率。
       现在统一改名 `maintenance_span_days` 并如实说明。

⚠️ 训练规模控制
    用户明确要求"不要为了课程作业盲目对数百万条模型全部进行复杂 GBT 训练"。
    因此：全量 3,138,954 行 → 确定性哈希取 20%（crc32(model_id) % 5 == 0）作为建模样本，
    再在样本内 randomSplit(0.8/0.2, seed=42) 划分。两步都可复现，且样本内外不重叠。

特征口径（Spark 与 sklearn 完全一致，见 local_analysis.py::rq4_ml）
    数值特征：likes_log, model_age_days, tag_count, children_count, maintenance_span_days
    类别特征：task（pipeline_tag，缺失统一为 "unknown"）、library_top（Top-50 + other）
    编码：两类特征都做 one-hot；类别词表（Top-50 库）只在**训练集**上统计得出。
    目标：label = log1p(downloads)

输出（gold/analysis/）
    ml_model_comparison/              模型对比表（Spark CSV **目录**，含 part-*.csv）
    ml_feature_importance_randomforest/  RF 特征重要性（Spark CSV 目录）
    ml_feature_importance_gbt/           GBT 特征重要性（Spark CSV 目录）
    ml_correlations/                  特征相关性（Spark CSV 目录）
    ml_summary.json                   全部标量结论（下游报告/图只读这个文件）

⚠️ 上面四张表是 `df.write.csv(路径)` 写出的**目录**（多 part 分片 + _SUCCESS），
   不是单个 .csv 文件。下游若用 `pd.read_csv(目录)` 会失败并**静默**跳过，
   必须逐 part 合并（见 make_figures.py::read_spark_csv）。

用法：python analysis_ml.py
"""
import json
import os
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
GOLD = DIRS["gold"]
ANALYSIS = os.path.join(GOLD, "analysis")
os.makedirs(ANALYSIS, exist_ok=True)

# 建模样本：确定性哈希抽样率 1/SAMPLE_MODULUS（全量 3,138,954 -> 约 62.8 万）
SAMPLE_MODULUS = 5
SPLIT_SEED = 42
TRAIN_FRACTION = 0.8
TOP_LIBRARIES = 50

# 数值特征（Spark 列名 -> 对外统一名）
NUM_FEATURES = [
    ("likes_log", "likes_log"),
    ("model_age_days", "model_age_days"),
    ("tag_count", "tag_count"),
    ("children_count", "children_count"),
    ("update_interval_days", "maintenance_span_days"),   # 改名：维护跨度，非更新频率
]
# 类别特征（one-hot 前的源列名）
CAT_FEATURES = [("task", "task"), ("library_top", "library_top")]

RF_TREES = 60
RF_MAX_DEPTH = 12
GBT_ITERS = 40
GBT_MAX_DEPTH = 6


def evaluate(pred, label="label"):
    """在给定 DataFrame 上计算 RMSE / MAE / R²（只在 test 上调用）。"""
    out = {}
    for m in ("rmse", "mae", "r2"):
        out[m] = float(RegressionEvaluator(labelCol=label, metricName=m).evaluate(pred))
    return out


def avg_rank_table(df, col):
    """对取值聚合出频数表并给出平均秩（mid-rank），列为 [col, rx]。

    先聚合再定秩：唯一值数远小于行数，因此不需要对全表做全局排序。
    并列值取平均秩，是标准的 tie-corrected Spearman 口径。
    """
    w = Window.orderBy(col)
    return (df.groupBy(col).count()
            .withColumn("_cum", F.sum("count").over(w))
            .withColumn("rx", F.col("_cum") - (F.col("count") - 1) / F.lit(2.0))
            .select(col, "rx"))


def spearman(df, col, label="label"):
    """并列取平均秩的 Spearman 相关 = 秩化后的 Pearson。

    两个秩表都 join 回同一份原始两列表上，保证逐行对齐（不能用
    monotonically_increasing_id 在两个独立 DataFrame 上各自编号，那样不对齐）。
    """
    base = df.select(F.col(col).alias("_x"), F.col(label).alias("_y"))
    rx = avg_rank_table(base, "_x").withColumnRenamed("rx", "rank_x")
    ry = avg_rank_table(base, "_y").withColumnRenamed("rx", "rank_y")
    joined = base.join(rx, on="_x", how="left").join(ry, on="_y", how="left")
    return joined.stat.corr("rank_x", "rank_y")


def importance_layout(schema):
    """从 VectorAssembler 写进 schema 的 ml_attr 元数据还原每一维的属性名。

    长度天然等于特征维度，比用 `distinct().count()` 猜测可靠：
    StringIndexer / OneHotEncoder 在 handleInvalid="keep" 下会为
    "无效/未见过的取值"额外分配一个桶，展开维度会**大于** distinct 数。
    """
    meta = schema["features"].metadata or {}
    attrs = (meta.get("ml_attr", {}) or {}).get("attrs", {}) or {}
    pairs = []
    for group in ("numeric", "binary", "nominal"):
        for a in (attrs.get(group) or []):
            pairs.append((int(a["idx"]), str(a["name"])))
    pairs.sort()
    return [name for _, name in pairs]


def dim_source(attr_name):
    """把 one-hot 展开后的单维属性名归并回它所属的源特征列名。"""
    for _, cat in CAT_FEATURES:
        if attr_name.startswith(cat + "_ohe"):
            return cat
    for src, display in NUM_FEATURES:
        if attr_name == src:
            return display
    return attr_name


def add_task_lib(df, top_libs):
    """补出 RQ4 的 task / library_top 两列并做缺失兜底。

    task  : pipeline_tag 缺失统一记 "unknown"（不推断成任何具体任务）
    library_top : 只保留训练集里的 Top-N 库，其余归 "other"（避免 2708 维 one-hot）
    """
    return (df.withColumn("task", F.coalesce(F.col("pipeline_tag"), F.lit("unknown")))
              .withColumn("library_top",
                          F.when(F.col("library").isin(top_libs), F.col("library"))
                           .otherwise(F.lit("other")))
              .fillna({"update_interval_days": 0, "children_count": 0,
                       "tag_count": 0, "likes_log": 0.0, "model_age_days": 0})
              .dropna(subset=["label"]))


def main():
    spark = SS.build("HF_Analysis_RQ4")
    feats_path = os.path.join(GOLD, "model_adoption_features")
    full = spark.read.parquet(feats_path)

    # ---- 0) 原始行数 + 主键重复检查（不允许用重复模型 id 训练）----
    n_full = full.count()
    n_dup_ids = (full.groupBy("model_id").count().filter(F.col("count") > 1).count())
    print(f"[rq4] 特征表 {feats_path}  行数={n_full}  重复 model_id 组数={n_dup_ids}")

    # ---- 1) 确定性哈希建模样本（可复现，与 local 引擎同一抽样率）----
    sample = full.filter(F.pmod(F.crc32(F.col("model_id")), F.lit(SAMPLE_MODULUS)) == 0)
    n_sample = sample.count()

    # ---- 2) 先划分 train/test，再谈一切拟合 ----
    train_raw, test_raw = sample.randomSplit([TRAIN_FRACTION, 1.0 - TRAIN_FRACTION],
                                            seed=SPLIT_SEED)
    train_raw.cache()
    test_raw.cache()
    n_train = train_raw.count()
    n_test = test_raw.count()
    n_overlap = train_raw.select("model_id").intersect(test_raw.select("model_id")).count()
    print(f"[rq4] 建模样本={n_sample}  train={n_train}  test={n_test}  "
          f"train∩test={n_overlap}")

    # ---- 3) 类别词表只在训练集上统计（Top-50 库，其余归 other）----
    top_libs = [r["library"] for r in
                train_raw.groupBy("library").count().orderBy(F.desc("count"))
                .limit(TOP_LIBRARIES).collect()]

    train = add_task_lib(train_raw, top_libs)
    test = add_task_lib(test_raw, top_libs)

    # ---- 4) 相关分析（描述性，在建模样本上算，不参与模型选择）----
    corr_rows = []
    for src, name in NUM_FEATURES:
        p = sample.stat.corr(src, "label")
        s = spearman(sample, src)
        corr_rows.append((name, float(p) if p is not None else None,
                          float(s) if s is not None else None))
        print(f"  corr[{name}] pearson={p:.4f} spearman={s:.4f}")
    (spark.createDataFrame(corr_rows, ["feature", "pearson", "spearman"])
        .write.mode("overwrite").option("header", True)
        .csv(os.path.join(ANALYSIS, "ml_correlations")))
    print("[rq4] ml_correlations -> gold/analysis/ml_correlations/")

    # ---- 5) 预处理：只在 train 上 fit，然后 transform 两份 ----
    idx_stages, ohe_stages, ohe_cols = [], [], []
    for src, name in CAT_FEATURES:
        idx_stages.append(StringIndexer(inputCol=src, outputCol=f"{name}_idx",
                                        handleInvalid="keep"))
        ohe_stages.append(OneHotEncoder(inputCol=f"{name}_idx", outputCol=f"{name}_ohe",
                                        handleInvalid="keep"))
        ohe_cols.append(f"{name}_ohe")
    num_cols = [src for src, _ in NUM_FEATURES]
    assembler = VectorAssembler(inputCols=ohe_cols + num_cols, outputCol="features")
    pre = Pipeline(stages=idx_stages + ohe_stages + [assembler]).fit(train)
    train_p = pre.transform(train).cache()
    test_p = pre.transform(test).cache()
    print("[rq4] 预处理在训练集上 fit 完成（one-hot 词表来自 train）")

    # ---- 6) 简单基线：训练集 label 均值（常数预测）----
    mean_train = float(train_p.agg(F.avg("label")).first()[0])
    base_pred = test_p.withColumn("prediction", F.lit(mean_train))
    comparison = [("Baseline_mean", evaluate(base_pred))]
    print(f"[Baseline_mean] RMSE={comparison[0][1]['rmse']:.4f} "
          f"MAE={comparison[0][1]['mae']:.4f} R2={comparison[0][1]['r2']:.4f}")

    models = {
        # 注意：Spark MLlib 的参数名是 elasticNetParam（0.0 = 纯 L2 岭回归）。
        # 写成 elasticNet=0.0 会在 PySpark 4.2 直接 TypeError。
        "LinearRegression": LinearRegression(featuresCol="features", labelCol="label",
                                             regParam=0.1, elasticNetParam=0.0,
                                             maxIter=50, standardization=True),
        "RandomForest": RandomForestRegressor(featuresCol="features", labelCol="label",
                                             numTrees=RF_TREES, maxDepth=RF_MAX_DEPTH,
                                             maxBins=64, seed=SPLIT_SEED),
        "GBT": GBTRegressor(featuresCol="features", labelCol="label",
                            maxIter=GBT_ITERS, maxDepth=GBT_MAX_DEPTH,
                            maxBins=64, seed=SPLIT_SEED),
    }

    importances = {}
    for name, reg in models.items():
        fitted = reg.fit(train_p)
        m = evaluate(fitted.transform(test_p))
        comparison.append((name, m))
        print(f"[{name}] RMSE={m['rmse']:.4f} MAE={m['mae']:.4f} R2={m['r2']:.4f}")
        if name in ("RandomForest", "GBT"):
            importances[name] = fitted.featureImportances

    (spark.createDataFrame(
        [(n, m["rmse"], m["mae"], m["r2"], n_test) for n, m in comparison],
        ["model", "rmse", "mae", "r2", "n_test"])
        .write.mode("overwrite").option("header", True)
        .csv(os.path.join(ANALYSIS, "ml_model_comparison")))
    print("[rq4] ml_model_comparison -> gold/analysis/ml_model_comparison/")

    # ---- 7) 特征重要性：把 one-hot 分列的重要性按源列汇总 ----
    # 让重要性可读（否则 task_ohe 会展开成几十列）。
    #
    # ⚠️ 不能用 distinct().count() 去猜每一列展开后的维度：
    #    StringIndexer / OneHotEncoder 在 handleInvalid="keep" 时会为
    #    "无效/未见过的取值"额外分配一个桶，展开后的维度会**大于** distinct 数，
    #    猜出来的 layout 与 featureImportances 长度对不上（实测 111 vs 113）。
    # 正确做法：直接读 VectorAssembler 写进 schema 的 ml_attr 元数据，
    # 它逐维给出了属性名，长度天然等于特征维度。
    layout = importance_layout(train_p.schema)
    print(f"[rq4] 特征维度（来自 ml_attr 元数据）= {len(layout)}")
    dim_sources = [dim_source(n) for n in layout]

    agg_by_model = {}
    for name, vec in importances.items():
        values = [float(v) for v in vec]
        if len(dim_sources) != len(values):
            raise RuntimeError(
                f"特征维度不匹配: dim_sources={len(dim_sources)} "
                f"importances={len(values)}")
        agg = {}
        for src_name, v in zip(dim_sources, values):
            agg[src_name] = agg.get(src_name, 0.0) + v
        agg_by_model[name] = agg
        rows = sorted(agg.items(), key=lambda kv: -kv[1])
        (spark.createDataFrame([(k, v) for k, v in rows], ["feature", "importance"])
            .write.mode("overwrite").option("header", True)
            .csv(os.path.join(ANALYSIS, f"ml_feature_importance_{name.lower()}")))
        print(f"[rq4] ml_feature_importance_{name.lower()} -> "
              f"{[(k, round(v, 4)) for k, v in rows]}")

    # 直接用聚合后的字典排序，避免原写法（dict comprehension 按 zip 生成）
    # 在源列重复出现时把同名键覆盖掉、导致排名丢失列。
    rf_rank = sorted(agg_by_model["RandomForest"].items(), key=lambda kv: -kv[1])
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "spark",
        "snapshot_sha": SA.HUBSTATS_COMMIT,
        "research_positioning": (
            "研究对象是**当前快照**下模型属性与下载热度的统计关联，"
            "不是对未来是否会火的预测。label 与 likes 同取自一个时间点，"
            "存在同期相关，不能解释为前瞻性预测能力。"),
        "target": "label = log1p(downloads)，downloads 为近 30 天下载计数",
        "n_rows_full": int(n_full),
        "n_duplicate_model_id_groups": int(n_dup_ids),
        "sampling": {
            "method": f"确定性哈希 crc32(model_id) % {SAMPLE_MODULUS} == 0",
            "n_sample": int(n_sample),
            "sample_share": round(n_sample / n_full, 6),
            "reason": ("全量 314 万行直接训练 GBT 在本机代价过高；"
                       "用户明确要求不要盲目对全量做复杂 GBT 训练。"),
        },
        "split": {
            "method": f"randomSplit([{TRAIN_FRACTION}, {1 - TRAIN_FRACTION}], seed={SPLIT_SEED})",
            "seed": SPLIT_SEED,
            "n_train": int(n_train),
            "n_test": int(n_test),
            "train_test_overlap": int(n_overlap),
            "note": "先划分再拟合；所有预处理只在 train 上 fit，指标只在 test 上计算。",
        },
        "features": {
            "numeric": [n for _, n in NUM_FEATURES],
            "categorical": [n for _, n in CAT_FEATURES],
            "encoding": "两类特征均 one-hot；Top-50 库词表只在训练集统计",
            "top_libraries_from_train": top_libs,
            "rename_note": ("源列 update_interval_days = lastModified - createdAt，"
                            "含义是维护跨度（天），不是更新频率；对外统一名 "
                            "maintenance_span_days。"),
            "no_target_derived_inputs": True,
        },
        "correlations": [
            {"feature": r[0], "pearson": r[1], "spearman": r[2]} for r in corr_rows
        ],
        "correlation_note": (
            "相关系数在建模样本上计算，属描述性统计，不参与模型选择。"),
        "metrics_on_test": {n: m for n, m in comparison},
        "baseline_note": (
            "Baseline_mean 用训练集 label 均值做常数预测，作为‘不建模’的下界参照。"),
        "feature_importance_note": (
            "重要性是 one-hot 分列按源列汇总后的结果，只表示模型内部的统计贡献，"
            "不代表因果，也不代表模型质量。若 likes_log 占主导，只能说明"
            "‘同一快照内的点赞与下载高度同期相关’。"),
        "rf_importance_ranked": [{"feature": k, "importance": v} for k, v in rf_rank],
    }
    with open(os.path.join(ANALYSIS, "ml_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(json.dumps({"n_train": n_train, "n_test": n_test, "overlap": n_overlap,
                      "metrics": {n: m for n, m in comparison}},
                     ensure_ascii=False, indent=2))

    train_raw.unpersist()
    test_raw.unpersist()
    train_p.unpersist()
    test_p.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
