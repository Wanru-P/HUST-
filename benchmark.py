#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
benchmark.py — Phase 3：真实数据 Spark 扩展性实验（+ 单独标注的合成数据压力测试）。

两种模式，结论**严格分开**，绝不互相引用：
  --mode real   （默认）真实 hub-stats `models.parquet`，确定性哈希 10/25/50/100% 嵌套子集
  --mode synth  合成数据压力测试。**只用于受控压力测试**，
                任何关于 Hugging Face 生态的结论都不得引用合成数据结果。

⚠️ 原实现的问题（本次修复）
  1. 合成数据的取值在**建计划时**就求值了：`F.lit(ptags[rnd.randint(...)])` 在 Python 侧
     只调用一次 random，于是整列是同一个常量。现在改为
     `F.element_at(数组字面量, pmod(id, k) + 1)`，取值真正逐行变化。
  2. `time_op` / `sampler` 是嵌套函数，违反工程约束。现在全部提到模块级。
  3. 文档里写的是 WSL + data_sci 环境，与本机实际环境不符。现在统一走 `spark_session.py`。
  4. shuffle/CPU 指标被写成 0：本地模式下 StatusTracker 在 action 结束后取不到
     已完成 stage 的 taskMetrics，原实现就填 0 —— 那等于谎称 groupBy/join 没有 shuffle。
     现在改为：读不到就记**空值**，并改从 Spark UI REST 取真实值；同时关闭 AQE，
     因为 AQE 会把已物化的 shuffle stage 标成 SKIPPED，使 per-op shuffle 量被系统性低估。
  5. Parquet 扫描算子测不出列裁剪：`count()` 会被**聚合下推到 Parquet 元数据层**，
     根本不解码数据列（实测全 20 列 count 0.289s、6 列 count 0.600s、
     而真正解码全 20 列要 4.857s）。原实现拿 `count()` 比"全列 vs 列裁剪"，
     两者耗时几乎相同，得出的"列式存储列裁剪优势"会是假的。
     现在扫描类算子统一改用 `decode_barrier()`（`count(每列)`，不可下推），
     列裁剪效果因此真实可测（4.857s vs 0.600s，约 8.1 倍）。
  6. 内存指标口径错误：原实现累加机器上**所有** `java.exe` 的 RSS，
     会把用户自己的 Gradle/Kotlin 守护进程（本机实测约 4.7 GiB）算进来，
     得到的数字与 Spark 无关。现在改为精确定位 Spark driver JVM 进程号，
     只采该进程的 RSS（local 模式下 executor 与 driver 同进程）。
  7. 规模扫描混入固定成本：直接对 `raw.filter(哈希谓词)` 计时时，
     由于函数谓词无法下推、过滤列以外的列仍为整批行解码，
     每个规模档都白付一次全表扫描成本（实测 10% 档 5.411s vs 100% 档 5.096s）。
     现在改为先把嵌套哈希子集**物化**成独立 Parquet 文件，再在子集上计时，
     数据规模因此成为唯一自变量。

⚠️ 缓存口径（必须写清楚）
  - 计时区域内**不使用** `cache()/persist()`，因此不存在"拿缓存命中结果和冷读结果直接比"的情况。
  - Windows 无法清空操作系统文件缓存（没有 root/特权接口），所以同一文件第二次读起
     OS page cache 一定是热的。本脚本因此：
      * 每个 (scale, partitions, op) 先做 1 次**不计时** warmup；
      * 再重复 repeats 次并报告**中位数**；
      * 把"本进程首次接触该文件的扫描耗时"单独记为 `parquet_scan_first_touch`，
        并在 `os_cache_state` 里标注是 `first_touch` 还是 `warm`。

⚠️ 命名纪律
  本机是 local[N] 单机多核，**不是**多节点横向扩展。报告里只能写
  "本地并行度（核数）对耗时的影响"，不得写成"集群横向扩展"。

运行：
    python benchmark.py --mode real --out <csv>
    python benchmark.py --mode synth --scales 200000,1000000,3000000 --out <csv>
"""
import argparse
import csv
import json
import os
import platform
import re
import threading
import time
import urllib.request
from datetime import datetime, timezone

import psutil
from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
BRONZE_PARQUET = os.path.join(DIRS["bronze"], "models.parquet")
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
os.makedirs(ANALYSIS, exist_ok=True)

# 规模扫描固定用同一档 shuffle 分区，保证"只变数据量、不变其他条件"
SCALE_SWEEP_SHUFFLE = 64
DEFAULT_PARTITION_GRID = [16, 32, 64]
PARTITION_SWEEP_OPS = ["explode_tags", "groupby", "join", "graph_edges", "etl_pipeline"]
# 可用内存低于该值即认为该次测量受内存压力影响，在 note 里自动标注（GiB）
MEM_PRESSURE_GIB = 0.8

PROJECTED_COLUMNS = ["id", "createdAt", "downloads", "pipeline_tag",
                     "library_name", "tags"]

OP_NAMES = ["parquet_scan_first_touch", "parquet_scan_full", "parquet_scan_projected",
            "parquet_scan_scaled", "filter", "explode_tags", "groupby", "join",
            "graph_edges", "etl_pipeline"]

CSV_COLUMNS = [
    "run_tag", "mode", "data_kind", "scale_label", "scale_pct", "n_rows",
    "op", "is_warmup", "repeat_index", "seconds", "rows_out",
    "throughput_rows_per_s", "executor_run_ms", "shuffle_read_bytes",
    "shuffle_write_bytes", "input_partitions", "shuffle_partitions",
    "jvm_rss_peak_mib", "system_avail_min_gib", "mem_pressure", "spark_master",
    "driver_memory", "spark_cache", "os_cache_state", "note",
]

# ---- 模块级运行态（避免在函数内嵌套定义函数/闭包）----
_RUN_CFG = {}
_MEM_SAMPLES = []
_MEM_STOP = threading.Event()
_SEEN_STAGES = set()
_OUT_ROWS = []
_MEM_LOCK = threading.Lock()
_JVM_PID = None


# ============================ 内存采样 ============================
def set_jvm_pid(spark):
    """解析 Spark driver JVM 的进程号。

    为什么要精确定位：local 模式下 executor 与 driver 在**同一个 JVM**里，
    这个 JVM 的 RSS 才是本实验真正要观测的内存。
    若像原实现那样累加机器上所有 `java.exe` 的 RSS，会把用户自己的
    Gradle/Kotlin 守护进程（本机实测 4.7 GiB）也算进来，
    得到的数字与 Spark 无关 —— 那是错误指标，不如不给。

    RuntimeMXBean.getName() 形如 "12345@HOSTNAME"，取 @ 前的数字即 PID。
    """
    global _JVM_PID
    try:
        jvm = spark.sparkContext._jvm
        name = str(jvm.java.lang.management.ManagementFactory
                   .getRuntimeMXBean().getName())
        _JVM_PID = int(name.split("@")[0])
    except Exception:
        _JVM_PID = None
    return _JVM_PID


def java_rss_bytes():
    """Spark driver JVM 的 RSS（字节）。取不到返回 None。"""
    if _JVM_PID is None:
        return None
    try:
        return psutil.Process(_JVM_PID).memory_info().rss
    except Exception:
        return None


def mem_sampler():
    """后台采样线程体：记录 (时刻, Spark JVM RSS, 系统可用内存)。

    只读单个进程 + 一次系统内存查询，单轮耗时在毫秒级；
    原实现每次遍历全机进程表（本机 300+ 进程），单轮要数秒，
    导致采样点过稀、很多算子窗口内一个样本都没有。
    间隔取 0.05s：最快的算子（join/filter）只要 0.15s 左右，
    间隔太大时该算子的计时窗口内一个采样点都落不进来，RSS 只能记「不可用」。
    """
    while not _MEM_STOP.is_set():
        try:
            snap = (time.time(), java_rss_bytes(), psutil.virtual_memory().available)
            with _MEM_LOCK:
                _MEM_SAMPLES.append(snap)
        except Exception:
            pass
        time.sleep(0.05)


def start_mem_sampler():
    th = threading.Thread(target=mem_sampler, daemon=True)
    th.start()
    return th


def peak_in_window(t0, t1):
    """窗口内的 Spark JVM RSS 峰值（MiB）与系统可用内存最小值（GiB）。取不到就 None。

    注意 RSS 与可用内存**分别**判断：RSS 全部取不到时给 None，
    不会因为可用内存取到了就把 RSS 写成 0。
    """
    with _MEM_LOCK:
        win = [s for s in _MEM_SAMPLES if t0 <= s[0] <= t1]
    if not win:
        return None, None
    rss_vals = [s[1] for s in win if s[1] is not None]
    avail_vals = [s[2] for s in win if s[2] is not None]
    rss = round(max(rss_vals) / 2 ** 20, 1) if rss_vals else None
    avail = round(min(avail_vals) / 2 ** 30, 3) if avail_vals else None
    return rss, avail


def collect_stage_metrics(tracker, stage_ids):
    """从 StatusTracker 累加 stage 指标（CPU / Shuffle）。

    ⚠️ 本地模式下 action 同步返回后，已完成的 stage 往往已不在 StatusTracker 里，
    `getStageInfo()` 返回 None，于是这里什么也读不到。
    **读不到必须返回 None（不可用），绝不能返回 0** ——
    返回 0 等于断言"没有发生 shuffle"，而 groupBy/join 显然会 shuffle，那是编造数据。

    返回 (executor_run_ms, shuffle_read_bytes, shuffle_write_bytes)，
    任一环节读不到就整体返回 (None, None, None)。
    """
    cpu_ms, sr, sw = 0.0, 0, 0
    seen = False
    for sid in sorted(stage_ids):
        if sid in _SEEN_STAGES:
            continue
        _SEEN_STAGES.add(sid)
        try:
            info = tracker.getStageInfo(sid)
            if info is None:
                continue
            tm = info.taskMetrics()
            if tm is None:
                continue
            seen = True
            cpu_ns = tm.executorCpuTime() or 0
            run_ms = tm.executorRunTime() or 0
            cpu_ms += (cpu_ns / 1e6) if cpu_ns > 0 else run_ms
            try:
                sr += int(tm.shuffleReadMetrics().totalBytesRead() or 0)
            except Exception:
                pass
            try:
                sw += int(tm.shuffleWriteMetrics().bytesWritten() or 0)
            except Exception:
                pass
        except Exception:
            pass
    if not seen:
        return None, None, None
    return cpu_ms, sr, sw


# ---------- Spark UI REST：本地模式下真正能拿到 shuffle/CPU 的途径 ----------
def rest_get(url, timeout=5.0):
    """极简 HTTP GET -> JSON；任何失败都返回 None，不抛异常。"""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def local_ui_url(ui_url):
    """把 Spark UI 地址的主机名换成 127.0.0.1。

    `sparkContext.uiWebUrl` 返回的是机器主机名（如 http://LAPTOP-XXX:4040），
    本机 DNS/代理环境下 urllib 解析该主机名会失败，导致 REST 全部拿不到。
    UI 默认绑定 0.0.0.0，因此换成 127.0.0.1 是等价的。
    """
    if not ui_url:
        return None
    return re.sub(r"//[^/:]+", "//127.0.0.1", ui_url)


def rest_app_id(ui_url):
    """取当前应用的 app id（Spark UI REST 需要）。"""
    if not ui_url:
        return None
    apps = rest_get(ui_url.rstrip("/") + "/api/v1/applications")
    if isinstance(apps, list) and apps:
        return apps[0].get("id")
    return None


def rest_stage_max_id(ui_url, app_id):
    """当前已注册 stage 的最大 stageId；一个 stage 都没有时返回 -1（取不到返回 None）。

    必须返回 **-1 而不是 0**：stageId 从 0 开始编号，
    若用 0 作基线，`stageId > 0` 会把编号为 0 的那个 stage 漏掉，
    导致第一次测量的 shuffle 量被系统性低估。
    """
    if not (ui_url and app_id):
        return None
    stages = rest_get(ui_url.rstrip("/") + f"/api/v1/applications/{app_id}/stages")
    if not isinstance(stages, list):
        return None
    ids = [int(s.get("stageId")) for s in stages if s.get("stageId") is not None]
    return max(ids) if ids else -1


def rest_delta_metrics(ui_url, app_id, after_stage_id):
    """只统计 stageId > after_stage_id 的 stage，即**本次算子新产生**的 stage。

    返回 (executor_run_ms, shuffle_read_bytes, shuffle_write_bytes)；
    取不到返回 (None, None, None)。

    只要出现**非 COMPLETE** 的 stage（如 AQE 导致的 SKIPPED），就整体判为不可用：
    SKIPPED 的 stage 指标恒为 0，把它当 0 加进去会系统性低估 shuffle 量，
    那等于编造一个偏小的数字，不如如实标记为不可用。
    """
    if not (ui_url and app_id) or after_stage_id is None:
        return None, None, None
    stages = rest_get(ui_url.rstrip("/") + f"/api/v1/applications/{app_id}/stages")
    if not isinstance(stages, list):
        return None, None, None
    run_ms, sr, sw = 0, 0, 0
    seen = False
    incomplete = 0
    for s in stages:
        sid = s.get("stageId")
        if sid is None or int(sid) <= int(after_stage_id):
            continue
        seen = True
        if (s.get("status") or "").upper() != "COMPLETE":
            incomplete += 1
            continue
        run_ms += int(s.get("executorRunTime") or 0)
        sr += int(s.get("shuffleReadBytes") or 0)
        sw += int(s.get("shuffleWriteBytes") or 0)
    if not seen or incomplete:
        return None, None, None
    return run_ms, sr, sw


def stage_sampler(tracker, stage_ids, stop):
    """后台采集 active stage id，避免动作结束后 stage 不再 active 而取不到指标。"""
    while not stop.is_set():
        try:
            for sid in tracker.getActiveStageIds():
                stage_ids.add(sid)
        except Exception:
            pass
        time.sleep(0.03)


# ============================ 各算子实现 ============================
# 统一签名 (raw, sub)：raw = 未过滤的读取结果；sub = 按规模过滤后的结果。
def decode_barrier(columns):
    """构造**不可被 Parquet 聚合下推**的表达式，强制 Spark 真正逐行解码指定列。

    为什么不能直接用 count()（本机实测，3,138,959 行，中位耗时）：
        raw.count()                    0.520s
        raw.select(*全部列).count()     0.289s   <- 列数翻倍反而更快，纯属噪声
        raw.agg(count(6 个投影列))      0.600s
        raw.agg(count(全部 20 列))      4.857s
    前两者说明 count(*) 走 Parquet **元数据下推**，完全不解码数据列。
    若拿 count() 比较"全表扫描 vs 列裁剪"，两者耗时几乎相同（0.52s vs 0.29s），
    据此得出"列式存储的列裁剪优势"就是**编造结论**。

    count(col) 不在 Spark 支持的 Parquet 聚合下推列表内，会真正逐行解码；
    它只统计非空个数，几乎不引入额外 CPU（对比 to_json(struct(...))：
    全列 19.4s，其中大半是 JSON 序列化开销，会把解码信号稀释掉）。
    实测列裁剪效果因此清晰可测：全 20 列 4.857s vs 6 投影列 0.600s（约 8.1 倍）。
    """
    return [F.count(F.col(c)).alias(f"nn_{i}") for i, c in enumerate(columns)]


def scan_and_count(df, columns):
    """在给定列集合上强制解码后返回行数（解码量随 columns 变化，便于对比列裁剪）。"""
    row = df.agg(F.count(F.lit(1)).alias("n"), *decode_barrier(columns)).first()
    return int(row["n"])


def op_parquet_scan_first_touch(raw, sub):
    """本进程首次读取该文件：强制解码全部列后计数（Windows 下唯一的冷态近似）。"""
    return scan_and_count(raw, raw.columns)


def op_parquet_scan_full(raw, sub):
    """全表扫描：强制解码**全部 20 列**后计数（全列读取基准）。"""
    return scan_and_count(raw, raw.columns)


def op_parquet_scan_projected(raw, sub):
    """列裁剪扫描：只解码 PROJECTED_COLUMNS（6 列）后计数（列裁剪基准）。"""
    return scan_and_count(raw, PROJECTED_COLUMNS)


def op_parquet_scan_scaled(raw, sub):
    """按规模读取：在确定性哈希子集上强制解码全部列后计数。"""
    return scan_and_count(sub, sub.columns)


def op_filter(raw, sub):
    return sub.filter(F.col("downloads") > 1000).count()


def op_explode_tags(raw, sub):
    return sub.select(F.explode("tags").alias("tag")).count()


def op_groupby(raw, sub):
    return (sub.groupBy("pipeline_tag")
            .agg(F.count("*").alias("n_models"),
                 F.sum("downloads").alias("total_downloads"))
            .count())


def op_join(raw, sub):
    dim = sub.groupBy("pipeline_tag").agg(F.count("*").alias("n_models"))
    return sub.join(dim, on="pipeline_tag", how="left").count()


def op_graph_edges(raw, sub):
    """图边构建：baseModels(struct) -> (父模型 id, 子模型 id, 关系) 三元组。

    ⚠️ Bronze 原始列类型是
        struct<relation: string, models: array<struct<_id: string, id: string>>>
    `baseModels` 本身是 struct 而不是 array，因此**不能**直接 explode("baseModels")
    （会报 DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE）。正确做法：
    先取 struct 字段 baseModels.relation，再 explode 内层数组 baseModels.models。
    """
    exploded = (sub.select(F.col("id").alias("dst"),
                           F.col("baseModels.relation").alias("relation"),
                           F.explode("baseModels.models").alias("m")))
    return (exploded.select(F.col("m.id").alias("src"), "dst", "relation")
            .filter(F.col("src").isNotNull()).count())


def op_etl_pipeline(raw, sub):
    """复合 ETL：投影 -> 过滤 -> 展开标签 -> 按 (任务, 标签, 月份) 聚合。"""
    staged = (sub.select("id", "pipeline_tag", "tags", "createdAt")
              .filter(F.col("pipeline_tag").isNotNull()))
    flat = (staged.select(F.explode("tags").alias("tag"), "pipeline_tag",
                          F.date_format(F.col("createdAt"), "yyyy-MM").alias("month")))
    return (flat.groupBy("pipeline_tag", "tag", "month")
            .agg(F.count("*").alias("new_models")).count())


OPS = {
    "parquet_scan_first_touch": op_parquet_scan_first_touch,
    "parquet_scan_full": op_parquet_scan_full,
    "parquet_scan_projected": op_parquet_scan_projected,
    "parquet_scan_scaled": op_parquet_scan_scaled,
    "filter": op_filter,
    "explode_tags": op_explode_tags,
    "groupby": op_groupby,
    "join": op_join,
    "graph_edges": op_graph_edges,
    "etl_pipeline": op_etl_pipeline,
}


# ============================ 计时与落盘 ============================
def time_op(spark, op_name, raw, sub, scale_label, scale_pct, n_rows,
            is_warmup, repeat_index, os_cache_state, note=""):
    tracker = spark.sparkContext.statusTracker()
    stage_ids = set()
    stop = threading.Event()
    th = threading.Thread(target=stage_sampler, args=(tracker, stage_ids, stop), daemon=True)
    th.start()
    # REST 基线在**计时区间之外**取，不影响被测耗时
    before_stage = rest_stage_max_id(_RUN_CFG.get("ui_url"), _RUN_CFG.get("app_id"))
    t0 = time.time()
    rows_out = OPS[op_name](raw, sub)
    dt = time.time() - t0
    t1 = time.time()
    stop.set()
    th.join(timeout=2)
    try:
        for sid in tracker.getActiveStageIds():
            stage_ids.add(sid)
    except Exception:
        pass
    cpu_ms, sr, sw = collect_stage_metrics(tracker, stage_ids)
    if cpu_ms is None:
        # StatusTracker 读不到（本地模式常见）-> 退回 Spark UI REST
        cpu_ms, sr, sw = rest_delta_metrics(_RUN_CFG.get("ui_url"),
                                            _RUN_CFG.get("app_id"), before_stage)
        if cpu_ms is None and before_stage is not None:
            # UI 注册 stage 有极短延迟，重试一次
            time.sleep(0.4)
            cpu_ms, sr, sw = rest_delta_metrics(_RUN_CFG.get("ui_url"),
                                                _RUN_CFG.get("app_id"), before_stage)
    rss_peak, avail_min = peak_in_window(t0, t1)
    # 内存压力标记：可用内存过低时该次耗时会受换页影响。
    # 用**独立的布尔列**而不是写进 note —— note 是汇总的分组键，
    # 若把"最低 0.004 GiB / 0.032 GiB"这种带数值的文本写进去，
    # 同一算子的 3 次重复会被拆成 3 个只有 1 次重复的组，中位数失去意义。
    mem_pressure = bool(avail_min is not None and avail_min < MEM_PRESSURE_GIB)

    row = {
        "run_tag": _RUN_CFG["run_tag"],
        "mode": _RUN_CFG["mode"],
        "data_kind": _RUN_CFG["data_kind"],
        "scale_label": scale_label,
        "scale_pct": scale_pct,
        "n_rows": int(n_rows),
        "op": op_name,
        "is_warmup": bool(is_warmup),
        "repeat_index": repeat_index,
        "seconds": round(dt, 3),
        "rows_out": int(rows_out),
        "throughput_rows_per_s": (round(rows_out / dt, 1) if dt > 0 else None),
        # None = 取不到，**不是 0**（0 会谎称"没有 shuffle"）
        "executor_run_ms": (round(cpu_ms, 1) if cpu_ms is not None else None),
        "shuffle_read_bytes": (int(sr) if sr is not None else None),
        "shuffle_write_bytes": (int(sw) if sw is not None else None),
        "input_partitions": _RUN_CFG.get("input_partitions"),
        "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
        "jvm_rss_peak_mib": rss_peak,
        "system_avail_min_gib": avail_min,
        "mem_pressure": mem_pressure,
        "spark_master": _RUN_CFG["spark_master"],
        "driver_memory": _RUN_CFG["driver_memory"],
        "spark_cache": "none（计时区域内不使用 cache/persist）",
        "os_cache_state": os_cache_state,
        "note": note,
    }
    _OUT_ROWS.append(row)
    print(f"[bench] {_RUN_CFG['run_tag']:<22} scale={scale_label:>8} "
          f"parts={row['shuffle_partitions']:>3} op={op_name:24s} "
          f"rows={rows_out:>10,} t={dt:7.3f}s "
          f"cpu={row['executor_run_ms'] if row['executor_run_ms'] is not None else 'NA'}ms "
          f"shuffle={(row['shuffle_read_bytes'] or 0) + (row['shuffle_write_bytes'] or 0) if row['shuffle_read_bytes'] is not None else 'NA'} "
          f"rss={rss_peak} MiB"
          + ("  [warmup]" if is_warmup else ""), flush=True)
    return row


def run_op_block(spark, op_name, raw, sub, scale_label, scale_pct, n_rows,
                 repeats, warmup, os_cache_state, note=""):
    """1 次不计时 warmup + repeats 次计时，返回计时行列表。"""
    if warmup:
        time_op(spark, op_name, raw, sub, scale_label, scale_pct, n_rows,
                True, -1, os_cache_state, note)
    out = []
    for i in range(repeats):
        out.append(time_op(spark, op_name, raw, sub, scale_label, scale_pct, n_rows,
                           False, i, os_cache_state, note))
    return out


# ============================ 数据准备 ============================
def scale_condition(pct):
    """确定性哈希选样：crc32(id) % 100 < pct。

    同一 hash 阈值意味着 10% ⊂ 25% ⊂ 50% ⊂ 100%（嵌套子集，不是独立抽样），
    因此各档之间的差异只来自数据量，不来自样本构成变化。
    与 Python 侧 `zlib.crc32(id) % 100` 完全等价（已由 test_spark_contracts 交叉验证）。
    """
    return F.pmod(F.crc32(F.col("id")), F.lit(100)) < F.lit(int(pct))


BENCH_SUBSET_DIRNAME = "bench_scale_subsets"


def materialize_scale_subsets(raw, scales):
    """把确定性哈希子集**物化**成独立 Parquet 文件，使数据规模成为唯一自变量。

    为什么必须物化（实测证据，见本文件顶部第 5 条）：Spark 的 Parquet 向量化读取
    是"整批解码所需列、再应用过滤"。`pmod(crc32(id),100) < 10` 属于函数谓词，
    Spark 的 record-level filter 只认顶层列的简单比较，识别不了它，
    于是过滤列以外的列**仍为整批行解码**、过滤之后才丢弃。
    实测在 10% 子集上强制解码全部 20 列要 5.411s，与 100% 全表的 5.096s 几乎相同。
    若直接对 filter 结果计时，每个规模档都会混进一次全表扫描成本，
    "运行时间随数据规模变化"的结论会严重失真。

    物化后各档使用**同一套计算逻辑**，只有数据量不同；且子集之间嵌套
    （10% ⊂ 25% ⊂ 50% ⊂ 100%），采样方式完全一致。
    """
    root = os.path.join(ANALYSIS, BENCH_SUBSET_DIRNAME)
    paths = {}
    for pct in scales:
        p = os.path.join(root, f"scale_{pct}pct")
        if os.path.exists(os.path.join(p, "_SUCCESS")):
            print(f"[bench] 复用已物化子集 {pct}% -> {p}", flush=True)
        else:
            raw.filter(scale_condition(pct)).write.mode("overwrite").parquet(p)
            print(f"[bench] 已物化子集 {pct}% -> {p}", flush=True)
        paths[pct] = p
    return paths


def subset_size_bytes(path):
    """物化子集目录下所有 part 文件的总字节数。"""
    total = 0
    for name in os.listdir(path):
        fp = os.path.join(path, name)
        if os.path.isfile(fp) and not name.startswith("_"):
            total += os.path.getsize(fp)
    return total


def str_array(xs):
    """把 Python 字符串列表转成 Spark 数组字面量（供 element_at 按下标取值）。"""
    return F.array([F.lit(x) for x in xs])


def pick_str(xs, modulus, offset):
    """逐行取值：element_at(数组, pmod(id*modulus + offset, len) + 1)。

    关键：下标是**列表达式**而不是 Python 侧算好的常量，
    所以整列取值会随 id 变化（原实现用 rnd.randint 在建计划时求值，导致整列常量）。
    """
    idx = F.pmod(F.col("id") * F.lit(modulus) + F.lit(offset), F.lit(len(xs)))
    return F.element_at(str_array(xs), (idx + 1).cast("int"))


def synthesize(spark, n, path):
    """生成 N 条**逐行变化**的合成记录（parquet，保留 timestamp/struct 类型）。

    只用于受控压力测试，与真实 HF 数据无关，输出里 data_kind = synthetic_stress_test。
    """
    authors = ["org", "user", "lab", "bigcorp"]
    libs = ["transformers", "diffusers", "safetensors", "peft", "gguf", "other"]
    ptags = ["text-generation", "text-classification", "text-to-image",
             "image-text-to-text", "automatic-speech-recognition", "other"]
    tags_pool = ["pytorch", "transformers", "en", "zh", "license:mit",
                 "text-generation", "safetensors", "conversational", "diffusers"]
    bases = ["Qwen/Qwen3-27B", "Qwen/Qwen-Image-2.1", "MiniMaxAI/MiniMax-H3",
             "black-forest-labs/FLUX.1-dev", "google/gemma-3-2b"]
    bm_struct = "struct<relation:string,models:array<struct<_id:string,id:string>>>"

    df = (spark.range(0, n, numPartitions=max(1, min(64, n // 50000 + 1)))
          .select(
              F.concat(F.lit("org/model-"), F.col("id").cast("string")).alias("id"),
              pick_str(authors, 1, 0).alias("author"),
              F.expr("timestampadd(HOUR, CAST(pmod(id, 20000) AS INT), "
                     "TIMESTAMP '2024-01-01 00:00:00')").alias("createdAt"),
              F.expr("timestampadd(HOUR, CAST(pmod(id, 20000) AS INT) + 240, "
                     "TIMESTAMP '2024-01-01 00:00:00')").alias("lastModified"),
              F.pmod(F.col("id") * F.lit(7919), F.lit(1000000)).cast("long")
               .alias("downloads"),
              F.pmod(F.col("id") * F.lit(104729), F.lit(50000000)).cast("long")
               .alias("downloadsAllTime"),
              F.pmod(F.col("id") * F.lit(31), F.lit(5000)).cast("long").alias("likes"),
              pick_str(ptags, 1, 0).alias("pipeline_tag"),
              pick_str(libs, 1, 0).alias("library_name"),
              F.array(pick_str(tags_pool, 7, 1), pick_str(tags_pool, 13, 5),
                      pick_str(tags_pool, 29, 3)).alias("tags"),
              F.when(F.pmod(F.col("id"), F.lit(11)) == 0,
                     F.struct(F.lit("finetune").alias("relation"),
                              F.array(F.struct(
                                  F.lit(None).cast("string").alias("_id"),
                                  pick_str(bases, 1, 0).alias("id"))).alias("models")))
               .otherwise(F.lit(None).cast(bm_struct)).alias("baseModels")))
    df.write.mode("overwrite").parquet(path)
    print(f"[synth] 写出 {n:,} 条逐行变化的合成记录 -> {path}", flush=True)
    return path


# ============================ 主流程 ============================
def write_csv(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[bench] 明细 -> {path}  ({len(rows)} 行)")


def median_or_none(vals):
    """去掉 None（不可用）后取中位数；全为 None 则返回 None。

    绝不能把 None 当 0 参与统计，否则"不可用"会被伪装成"值为 0"。
    """
    xs = sorted(v for v in vals if v is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def write_summary(rows, path):
    """按 (run_tag, mode, scale_label, op, shuffle_partitions, note) 汇总：中位数/最小/最大。

    ⚠️ `run_tag` 必须进分组键：P1（规模扫描，local[6]）、P2（并行度扫描，
    local[2]/[4]/[6]，同为 real_50pct 规模档）、P3（合成）会产出**同名规模档**，
    若不带 run_tag，不同 master/内存配置的结果会被合并进同一组，
    中位数就变成了跨配置的混合值，规模-耗时曲线也会出现同一个 n_rows 上的多个点。
    """
    timed = [r for r in rows if not r["is_warmup"]]
    groups = {}
    for r in timed:
        key = (r["run_tag"], r["mode"], r["data_kind"], r["scale_label"], r["op"],
               r["shuffle_partitions"], r["mem_pressure"], r["note"])
        groups.setdefault(key, []).append(r)
    out = []
    for key, rs in sorted(groups.items()):
        secs = sorted(r["seconds"] for r in rs)
        mid = secs[len(secs) // 2] if len(secs) % 2 else \
            (secs[len(secs) // 2 - 1] + secs[len(secs) // 2]) / 2.0
        out.append({
            "run_tag": key[0], "mode": key[1], "data_kind": key[2],
            "scale_label": key[3], "op": key[4], "shuffle_partitions": key[5],
            "mem_pressure": key[6], "note": key[7],
            "n_rows": rs[0]["n_rows"], "rows_out": rs[0]["rows_out"],
            "repeats": len(rs),
            "seconds_median": round(mid, 3),
            "seconds_min": round(min(secs), 3),
            "seconds_max": round(max(secs), 3),
            "executor_run_ms_median": median_or_none([r["executor_run_ms"] for r in rs]),
            "shuffle_read_bytes_median": median_or_none([r["shuffle_read_bytes"] for r in rs]),
            "shuffle_write_bytes_median": median_or_none([r["shuffle_write_bytes"] for r in rs]),
            "jvm_rss_peak_mib": median_or_none([r["jvm_rss_peak_mib"] for r in rs]),
            "system_avail_min_gib": median_or_none([r["system_avail_min_gib"] for r in rs]),
            "spark_master": rs[0]["spark_master"],
            "driver_memory": rs[0]["driver_memory"],
        })
    cols = list(out[0].keys()) if out else []
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in out:
            w.writerow(r)
    n_na = sum(1 for r in out if r["shuffle_read_bytes_median"] is None)
    n_mp = sum(1 for r in out if r["mem_pressure"])
    print(f"[bench] 汇总(中位数) -> {path}  ({len(out)} 行；"
          f"其中 {n_na} 行 shuffle 指标不可用，{n_mp} 行受内存压力影响已单独分组)")
    return out


def main():
    ap = argparse.ArgumentParser(description="Phase 3 Spark 扩展性实验")
    ap.add_argument("--mode", choices=["real", "synth"], default="real")
    ap.add_argument("--scales", default=None,
                    help="real: 百分比 10,25,50,100；synth: 行数 200000,1000000")
    ap.add_argument("--partitions", default=",".join(str(p) for p in DEFAULT_PARTITION_GRID),
                    help="shuffle 分区扫描网格")
    ap.add_argument("--partition-sweep-scales", default="50,100",
                    help="在哪些规模上做分区扫描（real 模式为百分比）")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--ops", default=None, help="逗号分隔，限制参与扫描的算子")
    ap.add_argument("--tag", default=None, help="run_tag，用于区分不同 master/资源配置")
    ap.add_argument("--out", default=os.path.join(ANALYSIS, "benchmark_detail.csv"))
    ap.add_argument("--out-summary", default=os.path.join(ANALYSIS, "benchmark_summary.csv"))
    args = ap.parse_args()

    if args.mode == "real":
        scales = [int(x) for x in (args.scales or "10,25,50,100").split(",")]
        scale_labels = [f"real_{p}pct" for p in scales]
    else:
        scales = [int(x) for x in (args.scales or "200000,1000000,3000000").split(",")]
        scale_labels = [f"synth_{n//1000}k" for n in scales]
    part_grid = [int(x) for x in args.partitions.split(",")]
    sweep_scales = [int(x) for x in args.partition_sweep_scales.split(",")]
    scan_ops = [x.strip() for x in args.ops.split(",")] if args.ops else None

    run_tag = args.tag or f"{args.mode}_{SS.DEFAULT_MASTER}_dm{SS.DEFAULT_DRIVER_MEMORY}"
    _RUN_CFG.update({
        "run_tag": run_tag,
        "mode": args.mode,
        "data_kind": ("hubstats_models_parquet" if args.mode == "real"
                      else "synthetic_stress_test"),
        "spark_master": SS.DEFAULT_MASTER,
        "driver_memory": SS.DEFAULT_DRIVER_MEMORY,
        "input_partitions": None,
        "ui_url": None,
        "app_id": None,
    })

    # 打开 Spark UI：本地模式下 StatusTracker 取不到已完成 stage 的指标，
    # 但 UI 的 REST 接口能给出 shuffleRead/WriteBytes 与 executorRunTime。
    # 关闭 AQE：AQE 会跳过已物化的 shuffle stage（status=SKIPPED），
    # 使"本次算子新产生的 stage"不完整，per-op 的 shuffle 量会被系统性低估。
    # 该配置对所有实验组一致，不引入组间偏差。
    spark = SS.build(f"HF_Benchmark_{args.mode}", enable_ui=True,
                     extra_configs={"spark.sql.adaptive.enabled": "false"})
    _RUN_CFG["ui_url"] = local_ui_url(spark.sparkContext.uiWebUrl)
    _RUN_CFG["app_id"] = rest_app_id(_RUN_CFG["ui_url"])
    jvm_pid = set_jvm_pid(spark)
    start_mem_sampler()
    _RUN_CFG["avail_start_gib"] = round(psutil.virtual_memory().available / 2 ** 30, 3)
    print(f"[bench] mode={args.mode} master={SS.DEFAULT_MASTER} "
          f"driver={SS.DEFAULT_DRIVER_MEMORY} python={platform.python_version()} "
          f"ui={_RUN_CFG['ui_url']} app_id={_RUN_CFG['app_id']} jvm_pid={jvm_pid}", flush=True)
    if jvm_pid is None:
        print("[bench] ⚠️ 未能解析 Spark JVM PID：jvm_rss_peak_mib 将记为「不可用」(空)。",
              flush=True)
    if _RUN_CFG["app_id"] is None:
        print("[bench] ⚠️ Spark UI REST 不可用：shuffle/CPU 指标将记为「不可用」(空)，"
              "而不是 0。核心指标（墙钟执行时间）不受影响。", flush=True)

    if args.mode == "real":
        if not os.path.exists(BRONZE_PARQUET):
            raise SystemExit(f"缺少真实 Bronze: {BRONZE_PARQUET}")
        spark.conf.set("spark.sql.shuffle.partitions", SCALE_SWEEP_SHUFFLE)
        raw = spark.read.parquet(BRONZE_PARQUET)
        n_total = raw.count()
        _RUN_CFG["input_partitions"] = raw.rdd.getNumPartitions()
        print(f"[bench] 真实 Bronze 行数={n_total:,} "
              f"读取分区数={_RUN_CFG['input_partitions']} "
              f"大小={os.path.getsize(BRONZE_PARQUET):,} bytes", flush=True)

        # 首次接触该文件的扫描耗时单独记录（本进程唯一一次 OS cache 冷态近似）
        # 注意 1：上面的 raw.count() 只读 Parquet 页脚与行组元数据，不触数据页，
        #         因此不会把列数据预热，first_touch 仍可视为冷态近似。
        # 注意 2：必须放在**物化子集之前**，否则写子集的过程会把原文件读进 OS 缓存。
        run_op_block(spark, "parquet_scan_first_touch", raw, raw, "real_cold_touch", 100,
                     n_total, args.repeats, 0, "first_touch",
                     "本进程首次读该文件（强制解码全部列）；Windows 无法清 OS page cache，这是冷态近似")

        base_ops = [o for o in OP_NAMES if o != "parquet_scan_first_touch"]
        if scan_ops:
            base_ops = [o for o in base_ops if o in scan_ops]

        # 物化各规模子集：使数据规模成为唯一自变量（见 materialize_scale_subsets 注释）
        subset_paths = materialize_scale_subsets(raw, scales)
        _RUN_CFG["subset_paths"] = {str(k): v for k, v in subset_paths.items()}
        _RUN_CFG["subset_sizes_bytes"] = {str(k): subset_size_bytes(v)
                                          for k, v in subset_paths.items()}

        for pct, label in zip(scales, scale_labels):
            sub = spark.read.parquet(subset_paths[pct])
            n_sub = sub.count()
            _RUN_CFG["input_partitions"] = sub.rdd.getNumPartitions()
            print(f"[bench] --- scale={label} n_rows={n_sub:,} "
                  f"parts={_RUN_CFG['input_partitions']} "
                  f"size={_RUN_CFG['subset_sizes_bytes'][str(pct)]:,} bytes ---", flush=True)
            for op in base_ops:
                run_op_block(spark, op, sub, sub, label, pct, n_sub,
                             args.repeats, args.warmup, "warm",
                             "在物化子集上计时，数据规模为唯一自变量")

        # 分区扫描：只变 shuffle 分区数，其他条件全部固定。
        # 跳过 p == SCALE_SWEEP_SHUFFLE：该分区数在规模扫描里已经测过，
        # 重复测一次会在同一 (op, shuffle_partitions) 上产生两行、污染分区曲线。
        for pct in sweep_scales:
            label = f"real_{pct}pct"
            sub = spark.read.parquet(subset_paths[pct])
            n_sub = sub.count()
            _RUN_CFG["input_partitions"] = sub.rdd.getNumPartitions()
            for p in part_grid:
                if p == SCALE_SWEEP_SHUFFLE:
                    continue
                spark.conf.set("spark.sql.shuffle.partitions", p)
                for op in PARTITION_SWEEP_OPS:
                    run_op_block(spark, op, sub, sub, label, pct, n_sub,
                                 args.repeats, args.warmup, "warm",
                                 f"partition_sweep={p}")
            spark.conf.set("spark.sql.shuffle.partitions", SCALE_SWEEP_SHUFFLE)
    else:
        spark.conf.set("spark.sql.shuffle.partitions", SCALE_SWEEP_SHUFFLE)
        import tempfile
        tmp = os.path.join(tempfile.gettempdir(), "hf_bench_synth")
        for n, label in zip(scales, scale_labels):
            path = synthesize(spark, n, os.path.join(tmp, f"n{n}"))
            raw = spark.read.parquet(path)
            n_rows = raw.count()
            _RUN_CFG["input_partitions"] = raw.rdd.getNumPartitions()
            print(f"[bench] --- scale={label} n_rows={n_rows:,} "
                  f"parts={_RUN_CFG['input_partitions']} ---", flush=True)
            for op in OP_NAMES:
                run_op_block(spark, op, raw, raw, label, 100, n_rows,
                             args.repeats, args.warmup, "warm",
                             "合成数据，仅用于受控压力测试")

    write_csv(_OUT_ROWS, args.out)
    summary = write_summary(_OUT_ROWS, args.out_summary)
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_tag": run_tag,
        "mode": args.mode,
        "data_kind": _RUN_CFG["data_kind"],
        "spark_master": SS.DEFAULT_MASTER,
        "driver_memory": SS.DEFAULT_DRIVER_MEMORY,
        "physical_cores": os.cpu_count(),
        "system_ram_gib": round(psutil.virtual_memory().total / 2 ** 30, 2),
        "system_avail_start_gib": _RUN_CFG.get("avail_start_gib"),
        "system_avail_min_gib_run": round(
            min((s[2] for s in _MEM_SAMPLES if s[2] is not None), default=0) / 2 ** 30, 3)
        if any(s[2] is not None for s in _MEM_SAMPLES) else None,
        "scale_sweep_shuffle_partitions": SCALE_SWEEP_SHUFFLE,
        "partition_grid": part_grid,
        "repeats": args.repeats,
        "warmup": args.warmup,
        "n_detail_rows": len(_OUT_ROWS),
        "n_summary_rows": len(summary),
        "cpu_time_note": ("executor_run_ms 优先取 Spark UI REST 的 executorRunTime"
                          "（各 stage 之和）；本地模式下 StatusTracker 在 action 结束后"
                          "往往取不到已完成 stage 的 taskMetrics，此时改走 UI REST。"),
        "metric_availability_note": (
            "shuffle_read_bytes / shuffle_write_bytes / executor_run_ms 若在某次测量中"
            "确实取不到，CSV 中记为**空值**并在汇总中排除，**绝不写 0** —— "
            "写 0 等于谎称该算子没有发生 shuffle。汇总里的 None 表示不可用。"),
        "ui_rest_used": bool(_RUN_CFG.get("app_id")),
        "jvm_pid": jvm_pid,
        "rss_caliber_note": (
            "jvm_rss_peak_mib 是 **Spark driver JVM 单个进程**的 RSS 峰值"
            "（local 模式下 executor 与 driver 同进程）。不累加机器上其它 java 进程，"
            "否则会把用户自己的 Gradle/Kotlin 守护进程算进来，得到与 Spark 无关的数字。"),
        "mem_avail_note": (
            "system_avail_min_gib 是该算子计时窗口内系统可用物理内存的最小值，"
            "反映并发压力，不代表 Spark 自身占用。"),
        "mem_sample_interval_s": 0.05,
        "mem_pressure_threshold_gib": MEM_PRESSURE_GIB,
        "mem_pressure_note": (
            f"每次测量的计时窗口内，若系统可用物理内存最低值 < {MEM_PRESSURE_GIB} GiB，"
            "该行 mem_pressure 记为 true，并在汇总里**单独成组**（不与正常测量混算中位数）。"
            "内存压力会导致换页，使耗时偏大且不可复现，这类数据不得直接与其它档比较。"),
        "mem_sample_note": (
            "内存采样线程每 0.05s 记录一次 Spark JVM 的 RSS 与系统可用内存。"
            "若某算子的计时窗口内一个采样点都没有（极短算子可能发生），"
            "jvm_rss_peak_mib / system_avail_min_gib 记为**空值（不可用）**，不填 0。"),
        "scaling_note": ("本机为 local[N] 单机多核，实验只能说明本地并行度与"
                         "资源对耗时的影响，**不得**表述为多节点横向扩展。"),
        "os_cache_note": ("Windows 无可用接口清空 OS page cache，除 "
                          "parquet_scan_first_touch 外所有计时都在 OS 缓存已热的状态下取得。"),
        "cache_note": "计时区域内不使用 Spark cache/persist，不存在缓存命中与冷读混比。",
        "scale_subset_note": (
            "real 模式的规模扫描**不使用惰性 filter**，而是先把 crc32 哈希子集"
            "（10% ⊂ 25% ⊂ 50% ⊂ 100%，嵌套）物化成独立 Parquet 文件再计时。"
            "原因：Spark 向量化读取是整批解码所需列后再过滤，函数谓词无法下推，"
            "实测 10% 子集上强制解码全部 20 列仍要 5.411s，与 100% 全表的 5.096s 几乎相同；"
            "若直接对 filter 结果计时，每个规模档都会混进一次全表扫描成本，"
            "规模-耗时关系会失真。物化后各档计算逻辑一致，只有数据量不同。"
            "物化子集位于 data_hubstats/gold/analysis/bench_scale_subsets/，可复用。"),
        "subset_paths": _RUN_CFG.get("subset_paths"),
        "subset_sizes_bytes": _RUN_CFG.get("subset_sizes_bytes"),
        "parquet_scan_note": (
            "扫描类算子（parquet_scan_*）统一使用 decode_barrier()：`count(每列)`。"
            "因为 Spark 对 Parquet 的 count(*) 会**聚合下推到元数据层**，完全不解码数据列"
            "（实测 3,138,959 行：全 20 列 count 0.289s、6 列 count 0.600s，"
            "而真正解码全 20 列 4.857s）。若用 count() 比较全表扫描与列裁剪，"
            "两者耗时几乎相同，会得出虚假的「无列裁剪收益」结论。"
            "parquet_scan_full 解码全部 20 列，parquet_scan_projected 只解码 6 个投影列，"
            "二者聚合形式完全一致，差异只来自列集合，因此列裁剪效果可真实对比。"),
        "synth_note": ("synth 模式数据由 synthesize() 逐行生成（取值随 id 变化），"
                       "仅用于受控压力测试，不得用于任何 HF 生态结论。"),
    }
    meta_path = os.path.join(ANALYSIS, f"benchmark_meta_{run_tag}.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"[bench] 元信息 -> {meta_path}")
    _MEM_STOP.set()
    spark.stop()


if __name__ == "__main__":
    main()
