#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spark_session.py — 全项目唯一的 SparkSession 构建入口（Phase 2 起）。

为什么必须统一：Phase 1A 踩到两个 Windows/时区坑，任何脚本漏掉都会静默出错：
  1. Windows 上没有 winutils.exe，必须把 fs.file.impl 指向纯 Java 的
     BareLocalFileSystem（jar 已由 setup_spark_fs.py 放进 <pyspark>/jars/）。
  2. 会话时区不锁 UTC，jsonl 源里 "...Z" 的时间会按本机 Asia/Shanghai 渲染，
     跨月记录被平移，与 pandas 路径口径不一致。

因此所有 Spark 脚本（ETL / RQ 分析 / Benchmark）都从这里建会话，不再各写一份。
另外这里顺手把 PYSPARK_PYTHON 固定为**当前解释器**：
本机 PATH 上的 `python` 是另一个 3.13，不设的话 worker 会用 3.13 而 driver 是 3.11，
直接抛 [PYTHON_VERSION_MISMATCH]。
"""
import os
import sys

from pyspark.sql import SparkSession

import schema_adapter as SA

# 必须在 getOrCreate() 之前设置：worker 进程通过它决定用哪个 Python
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

DEFAULT_MASTER = os.environ.get("HF_SPARK_MASTER", "local[6]")
DEFAULT_DRIVER_MEMORY = os.environ.get("HF_SPARK_DRIVER_MEMORY", "3g")
DEFAULT_SHUFFLE = os.environ.get("HF_SHUFFLE_PARTITIONS", "64")


def build(app_name, master=None, driver_memory=None, shuffle_partitions=None,
          enable_ui=False, extra_configs=None):
    """构建 SparkSession：本地文件系统 + UTC 时区 + 本地模式资源参数。

    enable_ui=True 时打开 Spark UI（默认关闭）。Benchmark 需要它：本地模式下
    StatusTracker 在 action 结束后取不到已完成 stage 的 taskMetrics，
    而 UI 的 REST 接口仍能给出每个 stage 的 shuffleRead/WriteBytes 与 executorRunTime。

    extra_configs 为可选的额外配置字典（如 benchmark 需要关闭 AQE）。
    """
    builder = (SparkSession.builder
               .appName(app_name)
               .master(master or DEFAULT_MASTER)
               .config("spark.driver.memory", driver_memory or DEFAULT_DRIVER_MEMORY)
               .config("spark.sql.shuffle.partitions", str(shuffle_partitions or DEFAULT_SHUFFLE))
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.parquet.compression.codec", "snappy")
               .config("spark.driver.maxResultSize", "512m")
               .config("spark.ui.enabled", "true" if enable_ui else "false"))
    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, str(value))
    for key, value in SA.SPARK_LOCAL_FS_CONFIGS.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
