#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""setup_spark_fs.py — 把额外的 JVM jar 安装进当前 PySpark 的 jars 目录。

需要安装两类 jar（清单见 schema_adapter.EXTRA_JARS）：
  1. 纯 Java 本地文件系统 hadoop-bare-naked-local-fs（Windows 上替代 winutils.exe）
  2. GraphFrames JVM 构件 graphframes-spark4_2.13 / graphframes-graphx-spark4_2.13

为什么必须放进 <pyspark>/jars/，而不能用 spark.jars：
    spark.jars / spark.driver.extraClassPath 都要到 SparkSubmit.prepareSubmitEnvironment
    阶段才生效；而该阶段解析 jar 路径本身就要先创建 FileSystem，于是反过来去加载
    fs.file.impl 指定的类 —— 此时该类尚未进入类路径，形成死锁。实测报错：
        java.lang.ClassNotFoundException:
            Class com.globalmentor.apache.hadoop.fs.BareLocalFileSystem not found
        at org.apache.spark.util.DependencyUtils$.resolveGlobPath
    只有 <pyspark>/jars/ 下的 jar 在 JVM 启动最初就在类路径上。

用法：
    python setup_spark_fs.py            # 安装全部（幂等）
    python setup_spark_fs.py --check    # 只检查是否已安装
"""
import argparse
import os
import shutil

import pyspark

import schema_adapter as SA


def jars_dir():
    """当前解释器所用 PySpark 的 jars 目录。"""
    return os.path.join(os.path.dirname(os.path.abspath(pyspark.__file__)), "jars")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="只检查，不复制")
    args = parser.parse_args()

    target_dir = jars_dir()
    print(f"[pyspark] jars 目录: {target_dir}")
    missing_src = [s for s in SA.EXTRA_JARS if not os.path.exists(s)]
    if missing_src:
        raise SystemExit("[FAIL] 源 jar 不存在：\n  " + "\n  ".join(missing_src))

    for src in SA.EXTRA_JARS:
        dst = os.path.join(target_dir, os.path.basename(src))
        installed = os.path.exists(dst) and os.path.getsize(dst) == os.path.getsize(src)
        if args.check:
            print(f"[check] installed={installed}  {os.path.basename(dst)}")
            continue
        if installed:
            print(f"[skip] 已安装且大小一致：{os.path.basename(dst)}")
            continue
        shutil.copy2(src, dst)
        print(f"[install] {os.path.basename(src)} -> {dst}")


if __name__ == "__main__":
    main()
