#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spark_smoke.py — Phase 1A 最小 Spark local 测试。

验证（全部真实执行）：
  1. 能起 SparkSession（local[4]）
  2. 能创建 DataFrame 并做聚合
  3. 能写 Parquet 到本地磁盘
  4. 能回读 Parquet 并校验行数/数值一致
  5. 记录 Java / Spark 版本与 HADOOP_HOME 状态

用法：
    python spark_smoke.py            # 默认 local[4]，写到 <project>/data_hubstats/_smoke/spark_parquet
"""
import json
import os
import shutil
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import schema_adapter as SA

MASTER = os.environ.get("HF_SMOKE_MASTER", "local[4]")


def main():
    out_dir = os.path.join(SA.data_root(), "_smoke", "spark_parquet")
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    report = {
        "master": MASTER,
        "java_home": os.environ.get("JAVA_HOME", ""),
        "hadoop_home": os.environ.get("HADOOP_HOME", ""),
        "pyspark_python": os.environ.get("PYSPARK_PYTHON", ""),
        "fs_impl": SA.SPARK_LOCAL_FS_CONFIGS.get(
            "spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem (hadoop 默认)"),
        "bare_fs_jar": SA.BARE_FS_JAR,
        "bare_fs_jar_exists": os.path.exists(SA.BARE_FS_JAR),
    }

    builder = (SparkSession.builder
               .appName("HF_Smoke_Phase1A")
               .master(MASTER)
               .config("spark.driver.memory", "2g")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false"))
    for key, value in SA.SPARK_LOCAL_FS_CONFIGS.items():
        builder = builder.config(key, value)

    t0 = time.time()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    report["spark_version"] = spark.version
    report["session_created_seconds"] = round(time.time() - t0, 2)

    # 1) 创建 DataFrame + 聚合
    df = spark.range(0, 1000).select(
        (F.col("id") % 7).cast("int").alias("grp"),
        (F.col("id") * 2).cast("long").alias("val"))
    agg = df.groupBy("grp").agg(F.count("*").alias("n"), F.sum("val").alias("s"))
    n_groups = agg.count()
    total = df.agg(F.sum("val")).first()[0]
    report["groups"] = n_groups
    report["sum_val"] = int(total)
    report["sum_expected"] = sum(2 * i for i in range(1000))

    # 2) 写 Parquet
    t1 = time.time()
    agg.write.mode("overwrite").parquet(out_dir)
    report["write_seconds"] = round(time.time() - t1, 2)

    # 3) 回读并校验
    t2 = time.time()
    back = spark.read.parquet(out_dir)
    report["read_rows"] = back.count()
    report["read_seconds"] = round(time.time() - t2, 2)
    report["roundtrip_ok"] = (
        report["read_rows"] == n_groups and report["sum_val"] == report["sum_expected"])
    report["parquet_files"] = sorted(os.listdir(out_dir))

    spark.stop()
    report["total_seconds"] = round(time.time() - t0, 2)

    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["roundtrip_ok"]:
        print("[FAIL] Spark 最小测试未通过", file=sys.stderr)
        sys.exit(1)
    print("[OK] Spark 最小测试通过（DataFrame / 聚合 / Parquet 写读）")


if __name__ == "__main__":
    main()
