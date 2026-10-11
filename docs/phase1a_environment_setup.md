# Phase 1A 交付文档（一）：本地 PySpark 环境搭建

> 工程：`hf_eco_analysis`　｜　阶段：Phase 1A　｜　日期：2026-10-11
> 本文件所有内容均为**真实执行结果**，无估算、无合成数据。

---

## 1. 结论摘要

| 事项 | 状态 | 关键证据 |
|---|---|---|
| 独立 conda 环境 `hf_eco`（Python 3.11） | ✅ 完成 | 未污染 base；`E:\Users\Admin\anaconda3\envs\hf_eco` |
| PySpark 4.2.0 + Windows 原生 local 模式 | ✅ 完成 | 无 WSL，`local[4]` 起得来 |
| DataFrame 创建 / 聚合 / Parquet 写入 / 回读 | ✅ 全部通过 | `roundtrip_ok=true` |
| Hadoop Windows 原生库（winutils）问题 | ✅ 已解决 | **改用纯 Java 文件系统，不引入任何 exe/dll** |
| 依赖版本锁定 | ✅ 完成 | 见 §3 完整 freeze |

**关键结论：本机 Windows 环境下不需要 `winutils.exe`，也不需要 `HADOOP_HOME`。**
经反编译确认 Hadoop 3.5.0 的 `RawLocalFileSystem` 在 Windows 上必然调用原生库或外部进程，
无法用配置绕过；改用 GlobalMentor 的纯 Java 实现 `BareLocalFileSystem` 后，Spark 本地读写完全正常。

---

## 2. conda 环境

```
环境名   : hf_eco
路径     : E:\Users\Admin\anaconda3\envs\hf_eco
解释器   : E:\Users\Admin\anaconda3\envs\hf_eco\python.exe
Python   : 3.11.17 | packaged by Anaconda, Inc. | (main, Oct  1 2026, 20:16:54) [MSC v.1942 64 bit (AMD64)]
```

**未污染 base**：所有包装在 `envs\hf_eco` 内，base 环境（`E:\Users\Admin\anaconda3`）未安装任何本次依赖。
不使用 `conda activate`，全部命令用解释器绝对路径调用，避免与其它环境（minimind / pytorch / torch_nightly）串味。

### Java

```
$ E:/Software/jdk-17/bin/java.exe -version
openjdk version "17" 2021-09-14
OpenJDK Runtime Environment (build 17+35-2724)
OpenJDK 64-Bit Server VM (build 17+35-2724, mixed mode, sharing)
```

运行时通过 `JAVA_HOME=E:/Software/jdk-17` 显式指定。

---

## 3. 实际安装的依赖及版本

`pip list --format=freeze` 全量输出（共 41 项）：

```
aiohappyeyeballs==2.7.1      frozenlist==1.8.0           py4j==0.10.9.9
aiohttp==3.14.5              fsspec==2026.9.0             pyarrow==26.0.0
aiosignal==1.4.0             graphframes-py==0.12.3       pyparsing==3.3.3
attrs==26.1.0                idna==3.20                   pyspark==4.2.0
certifi==2026.7.22           joblib==1.6.0                python-dateutil==2.9.0.post0
charset-normalizer==3.5.2    kiwisolver==1.5.1            requests==2.34.2
cloudpickle==3.1.2           matplotlib==3.11.2           scikit-learn==1.9.1
contourpy==1.3.3             multidict==7.1.0             scipy==1.17.1
cycler==0.12.1               narwhals==2.27.1             setuptools==84.0.0
fonttools==4.66.1            networkx==3.6.1              six==1.17.0
                             numpy==2.4.6                 threadpoolctl==3.7.0
                             packaging==26.3              typing_extensions==4.16.0
                             pandas==3.0.6                tzdata==2026.5
                             pillow==12.3.0               urllib3==2.8.0
                             pip==26.2.1                  wheel==0.47.0
                             propcache==0.5.4             yarl==1.25.1
                             psutil==7.2.2
```

核心版本（Phase 1A 的"实际锁定"）：

| 组件 | 版本 | 说明 |
|---|---|---|
| Python | 3.11.17 | conda 独立环境 |
| Java | OpenJDK 17 (build 17+35-2724) | Spark 4.x 要求 17+ |
| **pyspark** | **4.2.0** | |
| py4j | 0.10.9.9 | 随 PySpark 4.2.0 |
| pandas | 3.0.6 | 比旧 Demo 新，已在 `local_etl._iso_z` 做兼容加固 |
| numpy | 2.4.6 | |
| pyarrow | 26.0.0 | 与数据文件 `created_by: parquet-cpp-arrow version 26.0.0` 同源 |
| networkx | 3.6.1 | RQ3 图分析（pandas 路径） |
| scikit-learn | 1.9.1 | RQ4 预测 |
| matplotlib | 3.11.2 | 出图 |
| scipy | 1.17.1 | |
| **graphframes-py** | **0.12.3** | Python 包装器已装；**JVM 构件尚未下载**（见 §8） |
| fsspec | 2026.9.0 | 供 pyarrow 远程读取 |
| requests | 2.34.2 | 下载脚本 |

> ⚠️ 已知兼容性提示（非阻塞）：
> `pyspark.testing.utils` 会打印 `FutureWarning: PySpark does not yet fully support pandas >= 3.0.0`。
> 本工程不依赖 `pyspark.testing`，且 pandas 路径已单独验证，**不影响结果**。

### PySpark 内置 Hadoop 版本

PySpark 4.2.0 自带（`site-packages/pyspark/jars/`）：

```
hadoop-client-api-3.5.0.jar
hadoop-client-runtime-3.5.0.jar
parquet-hadoop-1.17.0.jar
dropwizard-metrics-hadoop-metrics2-reporter-0.1.2.jar
```

→ **Hadoop 3.5.0**，这是后面判断 winutils 版本匹配要求的依据。

---

## 4. Spark local 最小测试结果

脚本：`spark_smoke.py`（`local[4]`，driver 2g，shuffle 4）

实际输出（`data_hubstats/_smoke/spark_smoke_result.log`）：

```json
{
  "master": "local[4]",
  "java_home": "E:/Software/jdk-17",
  "hadoop_home": "",
  "pyspark_python": "E:/Users/Admin/anaconda3/envs/hf_eco/python.exe",
  "fs_impl": "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem",
  "bare_fs_jar_exists": true,
  "spark_version": "4.2.0",
  "session_created_seconds": 39.0,
  "groups": 7,
  "sum_val": 999000,
  "sum_expected": 999000,
  "write_seconds": 8.2,
  "read_rows": 7,
  "read_seconds": 1.62,
  "roundtrip_ok": true,
  "parquet_files": [
    "._SUCCESS.crc",
    ".part-00000-dadd3a5f-8e12-46a7-8d9f-de476834909d-c000.snappy.parquet.crc",
    "_SUCCESS",
    "part-00000-dadd3a5f-8e12-46a7-8d9f-de476834909d-c000.snappy.parquet"
  ],
  "total_seconds": 124.25
}
```

覆盖了要求的四项：**DataFrame 创建 ✅ / 聚合 ✅ / Parquet 写入 ✅ / 回读校验 ✅**。
注意 `hadoop_home` 为空字符串 —— **完全没有设置 `HADOOP_HOME`**。

仍会出现两条无害 WARN（不阻断执行，可忽略）：

```
WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException:
     HADOOP_HOME and hadoop.home.dir are unset.
WARN NativeCodeLoader: Unable to load native-hadoop library for your platform...
     using builtin-java classes where applicable
```

---

## 5. winutils 阻塞问题：完整记录与解决过程

### 5.1 第一步：记录完整报错 + 依赖版本（按要求执行）

依赖版本（复现上下文）：

```
OS        : Windows 11 Build 26200 (无 WSL)
Python    : 3.11.17
PySpark   : 4.2.0
Hadoop    : 3.5.0（PySpark 内置 hadoop-client-api-3.5.0.jar）
Java      : OpenJDK 17 (build 17+35-2724)
```

**完整报错**（`data_hubstats/_smoke/spark_failure_winutils.log`，节选关键栈）：

```
java.io.IOException: CreateProcess error=193, %1 不是有效的 Win32 应用程序。
    at java.base/java.lang.ProcessImpl.create(Native Method)
    at java.base/java.lang.ProcessImpl.<init>(ProcessImpl.java:494)
    at java.base/java.lang.ProcessBuilder.start(ProcessBuilder.java:1073)
    at org.apache.hadoop.util.Shell.runCommand(Shell.java:998)
    at org.apache.hadoop.util.Shell.run(Shell.java:960)
    at org.apache.hadoop.util.Shell$ShellCommandExecutor.execute(Shell.java:1285)
    at org.apache.hadoop.util.Shell.execCommand(Shell.java:1380)
    at org.apache.hadoop.util.Shell.execCommand(Shell.java:1362)
    at org.apache.hadoop.fs.RawLocalFileSystem.setPermission(RawLocalFileSystem.java:1179)
    at org.apache.hadoop.fs.RawLocalFileSystem.mkOneDirWithMode(RawLocalFileSystem.java:861)
    at org.apache.hadoop.fs.RawLocalFileSystem.mkdirsWithOptionalPermission(RawLocalFileSystem.java:901)
    at org.apache.hadoop.fs.RawLocalFileSystem.mkdirs(RawLocalFileSystem.java:873)
    at org.apache.hadoop.fs.ChecksumFileSystem.mkdirs(ChecksumFileSystem.java:1047)
    at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.setupJob(FileOutputCommitter.java:361)
    at org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.setupJob(HadoopMapReduceCommitProtocol.scala:180)
    at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:156)
```

现象：**读取正常，一写 Parquet 就崩**，崩在 `setPermission`。

（该报错是用一个 0 字节的 `winutils.exe` 占位文件触发的，`error=193` 即"不是有效的 Win32 应用程序"。
在完全没有 `HADOOP_HOME` 时，失败点更早：`FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`。）

### 5.2 第二步：反编译确认"能否绕过"

用 JDK 自带 `javap` 直接反编译 PySpark 内置的 `hadoop-client-api-3.5.0.jar`：

```
public void setPermission(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.permission.FsPermission)
    0: invokestatic  NativeIO.isAvailable:()Z          ← 分支 1
   18: invokestatic  NativeIO$POSIX.chmod:(Ljava/lang/String;I)V     ← 需要 hadoop.dll
   56: invokestatic  Shell.getSetPermissionCommand:(...)
   59: invokestatic  Shell.execCommand:(...)            ← 分支 2：需要 winutils.exe
```

`mkOneDirWithMode` 的反编译结果（关键三行）：

```
  0: aload_3
  1: ifnonnull  8
  4: invokestatic FsPermission.getDirDefault:()...    ← 权限为 null 时强制填默认值
  7: astore_3
...
102: aload_0
104: aload_3
105: invokevirtual setPermission:(...)                 ← 无条件调用
```

并检查了 `core-default.xml`（在 `hadoop-client-api-3.5.0.jar` 内）中所有含
`permission / native / winutil / umask` 的配置键：

```
fs.permissions.umask-mode      ← 只能改掩码，不能关闭调用
hadoop.security.auth_to_local
io.erasurecode.codec.native.enabled
io.seqfile.local.dir
```

**结论：Windows 上无法用配置绕过。** 要么给 `winutils.exe`，要么给 `hadoop.dll`（让 `NativeIO.isAvailable()` 为真），
两者都是 Windows 原生二进制。

### 5.3 第三步：查找可信、版本匹配的分发来源

| 来源 | 结论 |
|---|---|
| **Apache 官方** `archive.apache.org/.../hadoop-3.5.0/` | ❌ **没有任何 Windows 专用构件**。目录下只有 `hadoop-3.5.0.tar.gz` / `-aarch64.tar.gz` / `-src.tar.gz` / `-site.tar.gz`。官方不发布 winutils。 |
| **Maven Central** 搜索 `winutils` | ❌ `numFound = 0`，不存在任何 winutils 构件。 |
| `steveloughran/winutils`（Hadoop committer stevel 维护） | ⚠️ 最权威，但**已停止更新**，最高只到 hadoop-3.0.0。 |
| `cdarlint/winutils`（社区最常引用） | ⚠️ 最高只到 **hadoop-3.3.5/3.3.6**，与内置的 **3.5.0 不匹配**。 |
| `zehelh/winutils` | ⚠️ 声称有 hadoop-3.5.0，但为**低知名度第三方预编译二进制**，无法交叉验证。 |

按用户约束"**不允许自动运行未知来源的 exe/dll**，如无法确认来源，暂停并报告"，
上述 winutils 方案**全部不满足**要求 —— 要么版本不匹配，要么来源无法确认。

### 5.4 最终方案：改用纯 Java 文件系统（不引入任何 exe/dll）

在 `steveloughran/winutils` 的 README 中发现了 Hadoop committer 本人给出的推荐：

> **Status November 10, 2022: Bare Naked Local FS**
> Garret Wilson has implemented a filesystem which can be used as a replacement for the classic FS,
> **without the need for winutils**. If this works for you, **no need for winutils at all**!

采用构件：`com.globalmentor:hadoop-bare-naked-local-fs:0.1.0`

**来源可信度核查（全部真实执行）：**

| 核查项 | 结果 |
|---|---|
| Maven Central 元数据 | ✅ 唯一版本 0.1.0，发布于 2022-11-08 |
| 许可证 | ✅ Apache-2.0 |
| 被依赖情况 | ✅ Maven Central 上被 **268 个** 构件依赖 |
| 源码 | ✅ 同步发布 `-sources.jar`，可逐行审计 |
| 签名 | ✅ 同步发布 `.jar.asc`（GPG 分离签名） |
| 依赖 | ✅ `hadoop-common 3.3.4` 为 `provided` scope，jar 内**只有 3 个 class** |
| 文件大小 | 9,106 字节 |
| **SHA-1** | `cd03dc0f6e2b8d8957d97d421e95d9ceaa16b06b` —— 与 Maven Central 官方 `.sha1` **一致** |
| SHA-256 | `e0cc30fb0531eb0b59468dc0abf5b257533d2365b5e9f45e795edd707aa78c62` |

**源码审计**（`jars/_src/.../NakedLocalFileSystem.java`）：

```java
@Override
public void setPermission(final Path path, final FsPermission permission) throws IOException {
    final PosixFileAttributeView posixFileAttributeView =
        Files.getFileAttributeView(toNioPath(path), PosixFileAttributeView.class);
    if (posixFileAttributeView != null) {
        posixFileAttributeView.setPermissions(toNioPosixFilePermissions(permission));
    }
}
```

Windows 上 `Files.getFileAttributeView(..., PosixFileAttributeView.class)` 返回 `null` → **空操作**。
其余 `listStatus` / `getFileStatus` 全部走 Java NIO。`supportsSymlinks()` 返回 `false`。

**字节码审计**（对 jar 内 3 个 class 全部 `javap -c` 扫描可疑调用）：

| 检查项 | 结果 |
|---|---|
| `ProcessBuilder` / `Runtime.exec` | ❌ 未出现 |
| `URL` / `Socket` / `Http*`（网络） | ❌ 未出现 |
| `Class.forName`（反射） | ❌ 未出现 |
| `File.delete` | ❌ 未出现 |
| 唯一系统调用 | `System.getProperty("os.name")`（判断是否 Windows） |

**审计结论：该 jar 只做"把原生/shell 调用换成 Java NIO"，无任何额外行为。**

### 5.5 接入方式（有一个必须踩过的坑）

❌ **不能用 `spark.jars` 或 `spark.driver.extraClassPath` 传递该 jar。**
实测报错：

```
java.lang.RuntimeException: java.lang.ClassNotFoundException:
    Class com.globalmentor.apache.hadoop.fs.BareLocalFileSystem not found
    at org.apache.spark.util.DependencyUtils$.resolveGlobPath(DependencyUtils.scala:254)
    at org.apache.spark.deploy.SparkSubmit.prepareSubmitEnvironment(SparkSubmit.scala:399)
```

原因：`spark.jars` 的路径解析发生在 `SparkSubmit.prepareSubmitEnvironment` 阶段，
而解析 glob **本身就要先创建 `FileSystem`**，于是反过来去加载 `fs.file.impl` 指定的类 ——
此时该类还没进类路径，形成先有鸡后有蛋的死锁。

✅ **正确做法：把 jar 放进 `<pyspark>/jars/`**，它在 JVM 启动最初就在类路径上。
由 `setup_spark_fs.py` 完成（幂等）：

```bash
python setup_spark_fs.py           # 安装
python setup_spark_fs.py --check   # 只检查
```

已安装到：`E:\Users\Admin\anaconda3\envs\hf_eco\Lib\site-packages\pyspark\jars\hadoop-bare-naked-local-fs-0.1.0.jar`

Spark 侧只需一个配置（由 `schema_adapter.SPARK_LOCAL_FS_CONFIGS` 统一提供）：

```
spark.hadoop.fs.file.impl = com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
```

如需临时切回 Hadoop 原生实现：设环境变量 `HF_BARE_FS=0`。

### 5.6 遗留物说明

`_hadoop_home/` 目录是排查过程留下的**证据目录**，里面只有一个 0 字节占位文件
`bin/winutils.exe.placeholder-0bytes` 和说明文件 `NOTE.txt`。
**它不是可用的 winutils，不要把它设为 `HADOOP_HOME`。**

---

## 6. 统一运行方式

所有脚本通过 `schema_adapter.py` 解析路径与配置，用环境变量控制，不改代码：

| 环境变量 | 默认值 | 作用 |
|---|---|---|
| `HF_DATA_DIR` | `<project>/data_hubstats` | 数据根目录（与旧 Demo 的 `<project>/data` 隔离） |
| `HF_ENGINE` | `spark` | `spark` → `silver`/`gold`；`local` → `silver_local`/`gold_local` |
| `HF_BRONZE_PATH` | `<root>/bronze/models.parquet` | 显式指定 Bronze 输入（文件或目录） |
| `HF_BARE_FS` | `1` | `0` 则关闭纯 Java 文件系统，回退 Hadoop 原生实现 |
| `HF_SPARK_MASTER` | `local[4]` | Spark master |
| `HF_SPARK_DRIVER_MEMORY` | `4g` | driver 内存 |
| `HF_SHUFFLE_PARTITIONS` | `16` | shuffle 分区数 |

典型命令：

```bash
# 新正式数据（parquet），Spark 引擎
HF_DATA_DIR=<project>/data_hubstats HF_ENGINE=spark python spark_etl.py

# 新正式数据，pandas 引擎
HF_DATA_DIR=<project>/data_hubstats HF_ENGINE=local python local_etl.py

# 旧 Demo 数据（jsonl）回归
HF_DATA_DIR=<project>/data_hubstats/demo_regress \
HF_BRONZE_PATH=<project>/data/bronze/models.jsonl \
HF_ENGINE=local python local_etl.py

# 双引擎一致性校验
python verify_phase1a.py --root <project>/data_hubstats/sample_run
```

---

## 7. 本阶段新增/修改的文件

**新增：**

| 文件 | 作用 |
|---|---|
| `download_hubstats.py` | 从 hf-mirror 断点续传下载 + 全量 SHA-256 校验 + 写清单 |
| `schema_adapter.py` | 统一 Schema 契约与路径解析层（不 import pyspark/pandas） |
| `sample_bronze.py` | 确定性哈希抽样（`crc32(id) % 63 == 0`） |
| `spark_smoke.py` | Spark local 最小测试 |
| `setup_spark_fs.py` | 把纯 Java 文件系统 jar 装进 `<pyspark>/jars/` |
| `test_gini.py` | Gini 两种公式等价性验证 |
| `verify_phase1a.py` | Phase 1A 端到端验收校验 |
| `jars/hadoop-bare-naked-local-fs-0.1.0.jar`（+`.sha1`/`.asc`/`-sources.jar`） | 纯 Java 本地文件系统 |

**修改：**

| 文件 | 改动 |
|---|---|
| `spark_etl.py` | 接入 `schema_adapter`；新增 parquet/jsonl 双源 Bronze 适配；新增 `base_relation`/`source`/`snapshot_sha` 列；`gated` 显式类型化 NULL；新增 FS/时区/master 配置；**修复 3 处缺陷（见文档三 §6）** |
| `local_etl.py` | 接入 `schema_adapter`；新增 parquet 输入支持；产物按引擎隔离；保留 `GOLD`/`DATA_DIR`/`build_gold`/`load_bronze` 导出 |
| `verify_phase1a.py` | 新增 Gold 七表行数、月份格式、ISO-Z 格式三类校验（这些校验正是发现缺陷的原因） |

**未改动（严格保留）：** `analysis_evo.py`、`analysis_graph.py`、`analysis_ml.py`、`analysis_rq5.py`、
`benchmark.py`、`collect_hf.py`、`enrich_lastmodified.py`、`local_analysis.py`、`make_figures.py`、
`make_bench_figures.py`、`dashboard.html`、`report.md`、`README.md`、`requirements.txt`，以及
`data/` 下旧 Demo 的全部原始数据与结果（已用文件时间戳验证未被触碰）。

---

## 8. 未完成事项（Phase 1B 起）

1. **GraphFrames JVM 构件尚未下载。**
   Python 包装器 `graphframes-py 0.12.3` 已安装，但 JVM 侧构件还没取。
   已核实 Maven Central 上存在且版本对齐：
   `io.graphframes:graphframes-spark4_2.13:0.12.3`（latest，2026-10-05 更新，含 sources/javadoc）。
   下载后同样需要放进 `<pyspark>/jars/`（理由同 §5.5）。
2. `README.md` / `requirements.txt` 尚未同步 Phase 1A 的数据源与运行方式变更（本阶段按要求未改）。
3. `local_analysis.py` / `make_figures.py` / `benchmark.py` 尚未在新数据上跑过（Phase 1B）。
4. 全量 3,138,959 行任务未执行（按要求在 Phase 1A 结束后停止）。

---

## 9. 全量运行风险提示（提前列出）

| 风险 | 说明 | 建议 |
|---|---|---|
| **全局排序窗口单分区** | `download_distribution` 用 `Window.orderBy("downloads","model_id")` 且**无 partitionBy**，Spark 会告警 `No Partition Defined for Window operation! Moving all data to a single partition`。这是洛伦兹曲线/基尼系数的**语义要求**（必须是全局排名），无法靠加分区消除，但 314 万行会全部挤到 1 个分区。 | 用 `local[16]` + driver 8g；必要时改为"分桶排序 + 分段累计"重写 |
| **shuffle 分区数** | 当前默认 16，对 314 万行偏小，可能单分区过大 | 全量跑时按 16/32/64 扫描调优 |
| **pandas 路径内存** | `local_etl.py` 把整表读进内存；314 万行 × 11 列 + explode 出约 2000 万标签行，内存压力大 | 全量优先走 Spark；pandas 路径仅作抽样对拍 |
| **driver 内存** | 默认 4g，全量需上调 | `HF_SPARK_DRIVER_MEMORY=8g`（必要时 12g） |
| **首次 JVM 启动慢** | 本机冷启动 SparkSession 约 39 秒 | 批量任务合并到一次会话内，避免反复启停 |
| **`spark.sql.shuffle.partitions` 未按数据量自适应** | 抽样与全量用同一套配置会浪费或不足 | 全量跑前显式设置 |
