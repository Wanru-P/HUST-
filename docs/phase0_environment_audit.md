# Phase 0 · 环境审计报告（Windows 本地）

> 项目：Hugging Face 开源模型生态的演化、注意力集中与模型谱系分析
> 审计时间：2026-10-11（GMT+8）
> 审计方式：本机实测（命令输出为准），不采信 README 中原作者的 Linux/WSL 描述
> 结论先行：**具备运行条件，但需要 3 项安装/配置动作**（PySpark、Hadoop winutils、GraphFrames JAR 本地缓存）

---

## 1. 结论摘要

| 维度 | 实测结果 | 判定 |
|------|----------|------|
| 操作系统 | Windows 11 家庭版 中文版，Build 26200，64 位 | ✅ |
| WSL2 | **不可用**（无 LxssManager 服务、无发行版；且 wsl.exe 被本机安全策略禁用） | ⚠️ 不依赖 WSL |
| CPU | Intel Core Ultra 7 255HX，**20 物理核 / 20 逻辑处理器** @2.4GHz | ✅ |
| 物理内存 | **31.43 GB**（审计时可用 ≈ 6.76 GB） | ✅ 满足 8–12GB Driver |
| 磁盘 | C: 可用 **115.1 GB** / 397.5 GB；E: 可用 **244.5 GB** / 551.6 GB | ✅ |
| Java | **OpenJDK 17.0.17**（Temurin），`JAVA_HOME=E:\Software\jdk-17` | ✅ 满足 Spark 4.x |
| Python | Anaconda base **3.13.9**（`E:\Users\Admin\anaconda3`）；另有 `C:\Python314` | ✅ |
| PySpark | **未安装**（base + 5 个 conda 环境全部无） | ❌ 需安装 |
| py4j / graphframes-py | **未安装** | ❌ 需安装 |
| Hadoop winutils / hadoop.dll | **缺失**，`HADOOP_HOME` 未设置 | ❌ 需安装（Windows 写文件依赖） |
| 网络 · huggingface.co | **不可达**（直连超时；经沙箱代理 502） | ❌ |
| 网络 · hf-mirror.com | **可达**（HTTP 200） | ✅ 数据下载通道 |
| 网络 · Maven Central | **可达**（HTTP 200，0.7s） | ✅ GraphFrames JAR |
| 新数据源 models.parquet | 可读 Schema（HTTP Range），3,138,959 行 | ✅ |

**一句话结论**：本机硬件（20 核 / 32GB / 360GB+ 可用盘）完全够跑单机 `local` 模式全量实验；真正的阻塞是**软件栈缺失**（PySpark、winutils、graphframes-py）与**官方 HF 域名不通**（需走 hf-mirror）。

---

## 2. 操作系统与 WSL

- `Caption`: Microsoft Windows 11 家庭版 中文版
- `Version`: 10.0.26200，`BuildNumber`: 26200，`OSArchitecture`: 64 位
- WSL 检测（注意：本机安全策略**禁止直接执行 wsl.exe**，故通过服务/文件系统/注册表间接判定）：
  - `Get-Service LxssManager` → **找不到该服务**（WSL 服务未安装/未启用）
  - `\\wsl$` → **不可访问**（无任何已安装发行版）
  - `HKCU:\...\Lxss` 注册表键存在，但无发行版实例
  - `Microsoft-Windows-Subsystem-Linux` / `VirtualMachinePlatform` 可选功能查询需要管理员权限，无法在当前权限下确认状态

**判定**：不要把项目建立在 WSL 之上。原 README 里的 `data_sci`（`/home/gyin/miniconda3/envs/data_sci`）是原作者的 WSL 环境，**本机不存在**。全部实验改用 **Windows 原生 PySpark local 模式**。

---

## 3. CPU / 内存 / 磁盘

| 项目 | 实测值 | 对实验的含义 |
|------|--------|--------------|
| CPU 型号 | Intel(R) Core(TM) Ultra 7 255HX | — |
| 物理核 | 20 | — |
| 逻辑处理器 | 20（无超线程） | `local[16]` 左右较稳，留核给 OS/JVM |
| 基础频率 | 2400 MHz | — |
| 物理内存 | 31.43 GB | Driver 8GB 起步合理；12GB 需先关掉占用 |
| 审计时可用内存 | 6.76 GB | ⚠️ 当前后台程序占用较多，正式跑批前建议清理 |
| C: 盘 | 115.1 GB 可用 / 397.5 GB | 可放数据集(1.5GB)+中间结果 |
| E: 盘 | 244.5 GB 可用 / 551.6 GB | 建议把 `HF_DATA_DIR` 与 Spark 临时目录放 E: |

**并行度建议（单机 local）**
- `local[16]`（20 核留 4 核给 OS/驱动）
- `spark.sql.shuffle.partitions`：起步 **16**（≈核数），扫描区间 16/32/64
- `spark.driver.memory=8g` 起步，若 OOM 再上 `12g`；`spark.driver.maxResultSize=2g`

---

## 4. Java / Python / PySpark / GraphFrames

### 4.1 Java
- `java -version` → **openjdk 17.0.17 (Temurin)**，`JAVA_HOME=E:\Software\jdk-17`
- Spark 4.x 要求 Java 17/21 → **兼容**。建议显式设置 `PYSPARK_PYTHON` 与 `JAVA_HOME`。

### 4.2 Python
- Anaconda base：**3.13.9**（`E:\Users\Admin\anaconda3\python.exe`）
- 已装（本地等价路径所需，全部齐备）：
  `pandas 2.3.3`、`pyarrow 21.0.0`、`networkx 3.6.1`、`scikit-learn 1.7.2`、`matplotlib 3.10.6`、`numpy 2.3.5`、`requests 2.32.5`、`fsspec 2025.12.0`、`aiohttp 3.13.2`、`pypdf 6.7.3`
- 另有 `C:\Python314`（py launcher 默认）与托管 Python `3.13.14`
- 5 个 conda 环境（minimind / my_pytorch / new_pytorch / pytorch / torch_nightly）**均无 pyspark**

### 4.3 PySpark / GraphFrames（缺口）
- **PySpark 未安装**（base + 全部 conda 环境）；`py4j`、`graphframes-py` 同样缺失
- GraphFrames JVM JAR 计划经 `spark.jars.packages` 从 Maven 拉取（Maven 可达）；本机无预置 JAR 缓存
- 兼容性判断（待 Phase 3 实测确认）：
  - Spark 4.x → Python 侧 `graphframes-py>=0.12`，JVM 侧 `io.graphframes:graphframes-spark4_2.13:0.12.2`
  - 注意 Spark 4 的 `StatusTracker` 只有 `getActiveStageIds()/getStageInfo()`，无 `getStageInfos()`（benchmark.py 已适配）

### 4.4 Windows 专属依赖（**关键风险**）
- `winutils.exe` / `hadoop.dll` **不存在**，`HADOOP_HOME` **未设置**
- Spark on Windows 在**写本地文件**（写 parquet/json、`spark.sql.warehouse.dir`）时通常需要 winutils
- 处理方案（Phase 3 前完成）：下载与 Spark 4.x 对应 Hadoop 版本（3.4.x）的 `winutils.exe`+`hadoop.dll` 放入 `%HADOOP_HOME%\bin`，并设置 `HADOOP_HOME`、把 `%HADOOP_HOME%\bin` 加入 `PATH`；或使用第三方 `winutils` 包

---

## 5. 网络连通性（实测）

| 目标 | 结果 | 说明 |
|------|------|------|
| `huggingface.co` | ❌ 直连超时(20s)；经沙箱代理返回 502 | 官方域名不可用 |
| `huggingface.co/api/models` | ❌ 同上 | 旧采集脚本 `collect_hf.py` 默认走此域名，**会失败** |
| `hf-mirror.com` | ✅ HTTP 200（1.9s） | 项目 README 已列为备用镜像；作为**唯一可行下载通道** |
| `repo1.maven.org`（Maven Central） | ✅ HTTP 200（0.7s） | GraphFrames / Ivy 依赖可拉取 |
| 本机 Clash `127.0.0.1:7897` | ❌ 从当前沙箱不可达（探测返回 502） | README 假设的 Clash 通道在沙箱内不可用 |

环境变量（当前会话）：`HTTPS_PROXY=HTTP_PROXY=http://127.0.0.1:62571`（`WORKBUDDY_PROXY_SOURCE=system`）。

**对采集的影响**：
- `collect_hf.py` 与 `enrich_lastmodified.py` 依赖官方域名，**当前网络下无法运行**；Phase 1 数据接入应改为下载 `cfahlgren1/hub-stats` 快照（经 `hf-mirror.com`）。
- `hf-mirror.com` 是 Hugging Face 的公开镜像（项目 README 已认可），非未知镜像；如用户另有指定通道，请在验收时说明。

---

## 6. 新数据源连通性（轻量验证）

- 数据集：`cfahlgren1/hub-stats`
- 快照 revision：`main`，commit sha = `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`，`lastModified = 2026-10-10T13:38:15Z`
- 目标文件 `models.parquet`：**1,596,738,497 字节 ≈ 1.49 GiB**，支持 HTTP Range（`Accept-Ranges: bytes`）
- 通过 `hf-mirror.com` 的 `/resolve/main/models.parquet` 可 302 → CDN 并成功读取
- 已用 **HTTP Range 仅读取 Parquet footer/row-group**，未把整文件载入内存（详见 `phase0_code_audit.md` §5）

---

## 7. 是否具备运行完整项目的条件

**结论：硬件具备，软件需补齐。**

| 能力 | 现状 | 需要做的事 |
|------|------|-----------|
| 本地等价路径（pandas/networkx/sklearn/matplotlib） | ✅ 依赖齐全，可直接跑 | 无 |
| PySpark 单机 local 全量 ETL + RQ1/2/4/5 | ❌ | 安装 pyspark 4.x（建议独立 venv/conda env） |
| GraphFrames RQ3 | ❌ | 装 graphframes-py + 首次联网拉 JAR（Maven 可达） |
| Spark 写本地文件 | ❌ | 安装 winutils/hadoop.dll 并配置 `HADOOP_HOME` |
| 采集/补采（官方 API） | ❌ | 改用 hub-stats 快照；或配置可用代理 |
| 数据下载 | ✅ | 经 `hf-mirror.com` |

**审计期未做任何修改**：未安装任何依赖、未改动任何脚本、未覆盖任何数据/结果（Phase 0 只读）。
