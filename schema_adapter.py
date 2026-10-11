#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
schema_adapter.py — 统一数据 Schema 适配层（Phase 1A）。

职责（只放纯 Python 常量与轻量工具，**不 import pyspark / pandas**）：
  1. 定义 Silver/Gold 的规范列名（与旧 Demo 交付保持兼容，只做增量列）。
  2. 统一路径解析：HF_DATA_DIR + HF_ENGINE + HF_BRONZE_PATH，实现
     「旧 Demo 数据」与「新正式数据」隔离、「Spark 引擎」与「pandas 引擎」结果隔离。
  3. 记录 hub-stats 快照的固定版本（commit / sha256 / size）与抽样参数。

被 spark_etl.py 与 local_etl.py 共同引用；两个 ETL 各自实现本引擎的转换逻辑，
但列名、路径、常量都来自这里，避免两套实现漂移。
"""
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# ---- hub-stats 快照固定版本（Phase 0 记录，download_hubstats.py 校验用）----
HUBSTATS_DATASET = "cfahlgren1/hub-stats"
HUBSTATS_FILE = "models.parquet"
HUBSTATS_COMMIT = "2c1e195090bbcbe1f940cfdcb4ac25b76e48da70"
HUBSTATS_SHA256 = "d86485a02a7aace1b8c4eda4a849dd153a702d5351ab5a57f3c2320176e251ef"
HUBSTATS_SIZE = 1596738497

# ---- 抽样：确定性哈希，绝不用 head(N) ----
# 3,138,959 行中取 crc32(model_id) % 63 == 0 的约 5 万行。
SAMPLE_MODULUS = 63

# ---- 时间统一格式（与旧 Demo 的 ISO-8601 UTC 字符串一致）----
ISO_Z_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

# ---- Windows 本地文件系统：绕开 winutils 的纯 Java 实现 ----
# 背景（已用 javap 反编译 PySpark 内置的 hadoop-client-api-3.5.0.jar 证实）：
#   RawLocalFileSystem.setPermission() 在 Windows 上只有两条分支：
#     (1) NativeIO.isAvailable() 为真 -> NativeIO$POSIX.chmod（需要 hadoop.dll）
#     (2) 否则 -> Shell.execCommand(Shell.getSetPermissionCommand(...))（需要 winutils.exe）
#   而 mkOneDirWithMode() 在 permission 为 null 时会强制填入 FsPermission.getDirDefault()，
#   core-default.xml 中也没有任何开关能关闭这次调用 —— 即"无法用配置绕过"。
# 因此改用 GlobalMentor 的纯 Java 实现（Apache-2.0，Maven Central，源码可逐行审计）：
#   其 setPermission 走 Files.getFileAttributeView(PosixFileAttributeView)，
#   Windows 上该 view 返回 null -> 空操作，完全不碰原生库与外部进程。
# 该方案被 Hadoop committer steveloughran 在 winutils 仓库 README 中明确推荐。
BARE_FS_CLASS = "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
BARE_FS_JAR = os.path.join(PROJECT_ROOT, "jars", "hadoop-bare-naked-local-fs-0.1.0.jar")
BARE_FS_ENABLED = os.environ.get("HF_BARE_FS", "1").strip().lower() not in ("0", "false", "no", "off")
# 注意：**不能**用 spark.jars / spark.driver.extraClassPath 传递该 jar。
# 实测：spark.jars 的路径解析发生在 SparkSubmit.prepareSubmitEnvironment 阶段，
# 而解析 glob 需要先创建 FileSystem，于是反过来去加载 fs.file.impl 指定的类，
# 此时该类还没进类路径 -> ClassNotFoundException，形成死锁。
# 唯一可靠做法是让 jar 在 JVM 启动最初就在类路径上，即放进 <pyspark>/jars/，
# 由 setup_spark_fs.py 完成（幂等）。
SPARK_LOCAL_FS_CONFIGS = {
    "spark.hadoop.fs.file.impl": BARE_FS_CLASS,
} if BARE_FS_ENABLED else {}

# ---- GraphFrames JVM 构件（Phase 1B 下载 + SHA-1 校验）----
# 版本矩阵核实结论（2026-10-11）：
#   io.graphframes:graphframes-spark4_2.13:0.12.3 的 compile 依赖只有
#     - scala-library 2.13.18        -> PySpark 4.2.0 自带同名同版本
#     - graphframes-graphx-spark4_2.13:0.12.3
#     - datasketches-java 6.2.0     -> PySpark 4.2.0 自带同名同版本
#   spark-sql / spark-mllib / slf4j 均为 provided，由自带 4.2.0 jar 满足
#   （PySpark 也自带 spark-graphx_2.13-4.2.0.jar）
# 所以只需额外补下面两个 jar，**不需要** Ivy 联网解析。
GRAPHFRAMES_VERSION = "0.12.3"
GRAPHFRAMES_JARS = [
    os.path.join(PROJECT_ROOT, "jars", f"graphframes-spark4_2.13-{GRAPHFRAMES_VERSION}.jar"),
    os.path.join(PROJECT_ROOT, "jars",
                 f"graphframes-graphx-spark4_2.13-{GRAPHFRAMES_VERSION}.jar"),
]

# 所有需要放进 <pyspark>/jars/ 的额外 jar（理由同 BARE_FS_JAR 上方注释）
EXTRA_JARS = [BARE_FS_JAR] + GRAPHFRAMES_JARS

# ---- Silver: models 表规范列（顺序即写出顺序）----
SILVER_MODELS_COLUMNS = [
    "model_id", "author", "created_at", "last_modified",
    "downloads", "downloadsAllTime", "likes",
    "pipeline_tag", "library_name", "tags",
    "gated", "base_models", "base_relation",
    "scraped_at", "source", "snapshot_sha",
]

# ---- Silver: 其余两表 ----
SILVER_TAGS_COLUMNS = ["model_id", "tag"]
SILVER_EDGES_COLUMNS = ["src", "dst", "relation"]

# ---- Gold 七表名（与旧 Demo 一致）----
GOLD_TABLES = [
    "task_monthly_stats", "library_monthly_stats", "tag_monthly_stats",
    "download_distribution", "model_family_stats", "model_lifecycle",
    "model_adoption_features",
]

# ---- Gold 新增表（Phase 1B，不改动上面七表的字段含义）----
# lorenz_points: 频数聚合后的 Lorenz 累计点，列 metric/value/freq/cum_freq/
#                cum_model_pct/cum_download_pct，metric ∈ {downloads, downloadsAllTime}
# concentration_stats.json: 频数加权口径的 Gini / Top-1·5·10% / 零值占比（两种口径）
GOLD_EXTRA_TABLES = ["lorenz_points"]
GOLD_EXTRA_FILES = ["concentration_stats.json"]


def data_root():
    """数据根目录。默认 <project>/data_hubstats（与旧 Demo 的 <project>/data 隔离）。"""
    return os.environ.get("HF_DATA_DIR", os.path.join(PROJECT_ROOT, "data_hubstats"))


def engine():
    """当前引擎：spark（默认）或 local。决定 Silver/Gold 子目录名，避免互相覆盖。"""
    return (os.environ.get("HF_ENGINE", "spark") or "spark").strip().lower()


def resolve_dirs():
    """返回本次运行使用的 root / bronze / silver / gold 目录。

    spark 引擎 -> silver, gold
    local 引擎 -> silver_local, gold_local
    这样两套引擎产物落在不同目录，互不覆盖。
    """
    root = data_root()
    eng = engine()
    silver = os.path.join(root, "silver" if eng == "spark" else "silver_local")
    gold = os.path.join(root, "gold" if eng == "spark" else "gold_local")
    return {
        "root": root,
        "bronze": os.path.join(root, "bronze"),
        "silver": silver,
        "gold": gold,
        "engine": eng,
    }


def bronze_path():
    """解析 Bronze 输入路径。

    优先级：
      1) 环境变量 HF_BRONZE_PATH（显式指定文件或目录）
      2) <root>/bronze/models.parquet（新正式数据）
      3) <root>/bronze/models.jsonl（旧 Demo 数据）
    """
    explicit = os.environ.get("HF_BRONZE_PATH")
    if explicit:
        return explicit
    root = data_root()
    pq = os.path.join(root, "bronze", "models.parquet")
    if os.path.exists(pq):
        return pq
    return os.path.join(root, "bronze", "models.jsonl")


def bronze_kind(path):
    """判断 Bronze 输入类型：'parquet' 或 'jsonl'。"""
    if path.endswith(".parquet"):
        return "parquet"
    if os.path.isdir(path):
        for name in os.listdir(path):
            if name.endswith(".parquet"):
                return "parquet"
        return "jsonl"
    return "jsonl"


def read_manifest(root=None):
    """读取 ingest_manifest.json（若存在），用于把来源/commit/时间写入 Gold 元数据。"""
    import json
    base = root or data_root()
    path = os.path.join(base, "bronze", "ingest_manifest.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
