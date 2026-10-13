# 结果清单（results_manifest）

生成时间：2026-10-13T11:59:55.299302+00:00  
数据快照 commit：`2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`  
指标条数：237

> 本文件是**唯一**的结果基准。报告、Dashboard、图注、LaTeX 素材、PPT 素材中的所有数字都必须能在此表找到，且不得与表中值冲突。

| 指标 | 值 | 出处 | 备注 |
| --- | --- | --- | --- |
| `bench.mem_pressure_threshold_gib` | 0.8 | `benchmark_meta_cores_local2.json:mem_pressure_threshold_gib` |  |
| `bench.mem_sample_interval_s` | 0.05 | `benchmark_meta_cores_local2.json:mem_sample_interval_s` |  |
| `bench.real.cores_masters` | ["local[2]", "local[4]", "local[6]"] | `benchmark_all_summary.csv` |  |
| `bench.real.cores_measurements` | [{"run_tag": "cores_local2", "mode": "real", "data_kind": "hubstats_models_parquet", "scale_label": "real_50pct", "op": "etl_pipeline", "shuffle_partitions": "16", "mem_pressure": "False", "note": … | `benchmark_all_summary.csv（并行度扫描组）` |  |
| `bench.real.driver_memory` | ["3g"] | `benchmark_all_summary.csv:driver_memory` |  |
| `bench.real.masters` | ["local[6]"] | `benchmark_all_summary.csv` |  |
| `bench.real.measurements` | [{"run_tag": "cores_local2", "mode": "real", "data_kind": "hubstats_models_parquet", "scale_label": "real_50pct", "op": "etl_pipeline", "shuffle_partitions": "16", "mem_pressure": "False", "note": … | `benchmark_all_summary.csv（全量明细）` |  |
| `bench.real.measurements_all` | [{"run_tag": "cores_local2", "mode": "real", "data_kind": "hubstats_models_parquet", "scale_label": "real_50pct", "op": "etl_pipeline", "shuffle_partitions": "16", "mem_pressure": "False", "note": … | `benchmark_all_summary.csv（含内存压力组，仅供审计）` |  |
| `bench.real.mem_pressure_groups` | 0 | `benchmark_all_summary.csv:mem_pressure` |  |
| `bench.real.n_measurements` | 107 | `benchmark_all_summary.csv` |  |
| `bench.real.ops` | ["etl_pipeline", "explode_tags", "filter", "graph_edges", "groupby", "join", "parquet_scan_first_touch", "parquet_scan_full", "parquet_scan_projected", "parquet_scan_scaled"] | `benchmark_all_summary.csv` |  |
| `bench.real.rows` | [314455, 785597, 1569149, 3138959] | `benchmark_all_summary.csv` |  |
| `bench.real.scales` | ["real_100pct", "real_10pct", "real_25pct", "real_50pct", "real_cold_touch"] | `benchmark_all_summary.csv` |  |
| `bench.real.sweep_measurements` | [{"run_tag": "real_local6_dm3g", "mode": "real", "data_kind": "hubstats_models_parquet", "scale_label": "real_100pct", "op": "etl_pipeline", "shuffle_partitions": "16", "mem_pressure": "False", "no… | `benchmark_all_summary.csv（规模扫描组，按 run_tag 隔离）` |  |
| `bench.real.sweep_tag` | real_local6_dm3g | `benchmark_all_summary.csv:run_tag` |  |
| `bench.rss_caliber_note` | jvm_rss_peak_mib 是 **Spark driver JVM 单个进程**的 RSS 峰值（local 模式下 executor 与 driver 同进程）。不累加机器上其它 java 进程，否则会把用户自己的 Gradle/Kotlin 守护进程算进来，得到与 Spark 无关的数字。 | `benchmark_meta_cores_local2.json:rss_caliber_note` |  |
| `bench.run_meta` | {"generated_at": "2026-10-11T16:44:30+0800", "runs": [{"tag": "cores_local2", "exit_code": 0, "seconds": 260.3, "log": "benchmark_p2_cores_local2.log", "env": {"HF_SPARK_MASTER": "local[2]", "HF_SP… | `benchmark_runs.json` |  |
| `bench.synth.measurements` | [{"run_tag": "synth_local6_dm3g", "mode": "synth", "data_kind": "synthetic_stress_test", "scale_label": "synth_1000k", "op": "etl_pipeline", "shuffle_partitions": "64", "mem_pressure": "False", "no… | `benchmark_all_summary.csv（全量明细）` |  |
| `bench.synth.n_measurements` | 30 | `benchmark_all_summary.csv` |  |
| `data.bronze_rows` | 3138959 | `data_quality_report.json:n_bronze_source_rows` | 源 parquet 物理行数（含源数据自带的重复 id） |
| `data.conservation` | {"task_monthly_stats": {"sum_new_models": 3138954, "equals_n_models": true}, "library_monthly_stats": {"sum_new_models": 3138954, "equals_n_models": true}, "tag_monthly_stats": {"sum_new_models": 2… | `data_quality_report.json:conservation` | 聚合守恒：各月度表求和 == 主表行数 |
| `data.created_at_iso_z_ok` | true | `data_quality_report.json:created_at_iso_z_ok` |  |
| `data.created_max` | 2026-10-10T13:13:07.000Z | `data_quality_report.json:created_at_max` | 快照中最新创建时间 |
| `data.created_min` | 2022-03-02T23:29:04.000Z | `data_quality_report.json:created_at_min` |  |
| `data.created_parse_failures` | 0 | `data_quality_report.json:created_at_parse_fail` |  |
| `data.distinct_children` | 887032 | `data_quality_report.json:n_edge_dst` | 不同衍生（子）模型数 |
| `data.distinct_external_parents` | 3620 | `data_quality_report.json:n_distinct_external_parents` |  |
| `data.distinct_library_names` | 2708 | `data_quality_report.json:n_library_values` |  |
| `data.distinct_parents` | 88333 | `data_quality_report.json:n_edge_src` | 不同底座（父）模型数 |
| `data.distinct_pipeline_tags` | 57 | `data_quality_report.json:n_pipeline_tag_values` |  |
| `data.downloadsAllTime_sum` | 75949397771 | `data_quality_report.json:downloadsAllTime_sum` | downloadsAllTime 合计 |
| `data.downloads_sum` | 2788919780 | `data_quality_report.json:downloads_sum` | downloads 合计（近 30 天口径） |
| `data.dup_model_id` | 0 | `data_quality_report.json:dup_model_id` | 去重后主键重复数（应为 0） |
| `data.edge_relation_counts` | {"adapter": 329707, "merge": 48028, "finetune": 354320, "quantized": 187784} | `data_quality_report.json:edge_relation_counts` |  |
| `data.edge_rows` | 919839 | `data_quality_report.json:n_edges` | 显式底座关系边数 |
| `data.external_parent_edges` | 14838 | `data_quality_report.json:n_edges_with_external_parent` | 父模型不在快照内的边（已保留，未丢弃） |
| `data.gated_all_null` | true | `data_quality_report.json:gated_all_null` | gated 是否全为 NULL |
| `data.gold_bytes` | 697384662 | `full_etl_run_report.json:gold_bytes` |  |
| `data.gold_enriched_rows` | 3138954 | `data_quality_report.json:n_models_enriched` |  |
| `data.gold_rows` | {"task_monthly_stats": 2587, "library_monthly_stats": 7915, "tag_monthly_stats": 861394, "download_distribution": 3138954, "model_family_stats": 575, "model_lifecycle": 3138954, "model_adoption_fea… | `data_quality_report.json:gold_rows` | Gold 各表行数（表名 -> 行数） |
| `data.gold_rows.download_distribution` | 3138954 | `data_quality_report.json:gold_rows.download_distribution` |  |
| `data.gold_rows.library_monthly_stats` | 7915 | `data_quality_report.json:gold_rows.library_monthly_stats` |  |
| `data.gold_rows.lorenz_points` | 62763 | `data_quality_report.json:gold_rows.lorenz_points` |  |
| `data.gold_rows.model_adoption_features` | 3138954 | `data_quality_report.json:gold_rows.model_adoption_features` |  |
| `data.gold_rows.model_family_stats` | 575 | `data_quality_report.json:gold_rows.model_family_stats` |  |
| `data.gold_rows.model_lifecycle` | 3138954 | `data_quality_report.json:gold_rows.model_lifecycle` |  |
| `data.gold_rows.tag_monthly_stats` | 861394 | `data_quality_report.json:gold_rows.tag_monthly_stats` |  |
| `data.gold_rows.task_monthly_stats` | 2587 | `data_quality_report.json:gold_rows.task_monthly_stats` |  |
| `data.hard_failures` | [] | `data_quality_report.json:hard_fail` | 硬性失败项数（应为 0） |
| `data.missing_rates` | {"model_id": 0.0, "author": 0.0, "created_at": 0.0, "last_modified": 0.0, "downloads": 0.0, "downloadsAllTime": 0.0, "likes": 0.0, "pipeline_tag": 0.65555, "library_name": 0.509585, "tags": 0.0, "g… | `data_quality_report.json:missing_rate` |  |
| `data.per_year` | {"2022": 100392, "2023": 318206, "2024": 740836, "2025": 1103162, "2026": 876358} | `data_quality_report.json:models_per_year` |  |
| `data.quality_ok` | true | `data_quality_report.json:ok` |  |
| `data.silver_bytes` | 458545882 | `full_etl_run_report.json:silver_bytes` |  |
| `data.silver_models_rows` | 3138954 | `data_quality_report.json:n_models_silver` | 去重后的 Silver 主表行数 |
| `data.silver_rows` | {"models": 3138954, "model_tags": 20625635, "model_base_edges": 919839} | `data_quality_report.json:silver_rows` |  |
| `data.source_duplicate_ids` | 5 | `data_quality_report.json:n_bronze_duplicate_ids` | 源数据本身重复出现的 id 个数 |
| `data.tag_rows` | 20625635 | `data_quality_report.json:n_tag_rows` | (model_id, tag) 长表行数 |
| `data.unique_model_ids` | 3138954 | `data_quality_report.json:n_unique_model_id` |  |
| `data.unique_tags` | 424435 | `data_quality_report.json:n_unique_tags` |  |
| `deliverables.figure_count` | 12 | `figure_index.json` |  |
| `deliverables.figures` | [{"figure": "Figure 1", "file": "fig01_monthly_evolution.png", "title": "月度新增模型数（快照可见模型的创建时间分布）", "data_sources": ["gold/analysis/monthly_new_models.csv", "gold/analysis/summary_rq1.json"], "note":… | `figure_index.json:figures` |  |
| `deliverables.figures_ext` | [{"figure": "Figure 13", "file": "fig13_family_structure_compare.png", "title": "三种典型家族结构对比（星/链/合并）", "data_sources": ["gold/analysis/family_cases.json", "silver/model_base_edges", "gold/analysis/f… | `figures_ext_index.json:figures` |  |
| `deliverables.figures_ext_count` | 3 | `figures_ext_index.json` |  |
| `env.HF_ENGINE` | spark | `full_etl_run_report.json:config` |  |
| `env.HF_SHUFFLE_PARTITIONS` | 64 | `full_etl_run_report.json:config` |  |
| `env.HF_SPARK_DRIVER_MEMORY` | 3g | `full_etl_run_report.json:config` |  |
| `env.HF_SPARK_MASTER` | local[6] | `full_etl_run_report.json:config` |  |
| `env.JAVA_HOME` | E:\Software\jdk-17 | `full_etl_run_report.json:config` |  |
| `env.PYSPARK_PYTHON` | E:\Users\Admin\anaconda3\envs\hf_eco\python.exe | `full_etl_run_report.json:config` |  |
| `env.PYTHONUNBUFFERED` | 1 | `full_etl_run_report.json:config` |  |
| `env.etl_elapsed_seconds` | 142.8 | `full_etl_run_report.json:elapsed_seconds` |  |
| `env.etl_exit_code` | 0 | `full_etl_run_report.json:exit_code` |  |
| `env.etl_step_timings` | ["[time]      0.0s  SparkSession 就绪 (master=local[6], driver=3g)", "[time]     45.3s  Silver 三表写出完成", "[time]     73.5s  models_enriched 写出并回读完成 (n=3138954)", "[time]     86.5s  Gold 三张月度统计表写出完成", … | `full_etl_run_report.json:step_timings` |  |
| `env.min_system_available_gib` | 3.87 | `full_etl_run_report.json:min_system_available_gib` |  |
| `env.peak_jvm_rss_gib` | 3.67 | `full_etl_run_report.json:peak_jvm_rss_gib` |  |
| `env.quality_check_config` | {"master": "local[6]", "driver_memory": "4g", "shuffle_partitions": "64"} | `data_quality_report.json:config` |  |
| `exec.phase2` | {"generated_at": "2026-10-11T16:50:50+0800", "config": {"HF_SPARK_MASTER": "local[6]", "HF_SPARK_DRIVER_MEMORY": "4g", "HF_SHUFFLE_PARTITIONS": "64", "HF_ENGINE": "spark", "PYTHONUNBUFFERED": "1"},… | `phase2_runs.json` |  |
| `final.gf_unfinished` | [] | `graphframes_result.json:steps` |  |
| `final.phase2_failed` | [] | `phase2_runs.json` |  |
| `final.status` | PROJECT READY FOR REPORT AND PRESENTATION | `build_manifest.py:自动判定（依据 data_quality_report / phase2_runs / benchmark / graphframes_result）` |  |
| `final.status_reason` | 全部模块完成，GraphFrames 迭代算法亦在本机跑完，无未完成项。 | `build_manifest.py:自动判定` |  |
| `meta.bronze_file_bytes` | 1596738497 | `schema_adapter.py` |  |
| `meta.snapshot_commit` | 2c1e195090bbcbe1f940cfdcb4ac25b76e48da70 | `schema_adapter.py` |  |
| `meta.snapshot_sha256` | d86485a02a7aace1b8c4eda4a849dd153a702d5351ab5a57f3c2320176e251ef | `schema_adapter.py` |  |
| `rq1.date_floor_note` | 2022-03-02 单日即集中 29,573 个模型，占首月 32,621 个的 90.7%；而该日只有 5 个不同时间戳，其中 2022-03-02T23:29:05.000Z 一个时间戳就占 21,869 个（73.9%）。以上是**已观测到的现象**：同一时间戳在最早一天高度聚集，与逐日发布过程不符，因此不能解读为「自然发布高峰」。仅凭本快照**无法确认根因**（既可能是平台早期元数… | `summary_rq1.json:date_floor_note` |  |
| `rq1.date_floor_share` | 0.906563 | `summary_rq1.json:date_floor_share` |  |
| `rq1.earliest_day` | 2022-03-02 | `summary_rq1.json:earliest_day` |  |
| `rq1.earliest_day_count` | 29573 | `summary_rq1.json:earliest_day_count` |  |
| `rq1.earliest_day_distinct_timestamps` | 5 | `summary_rq1.json:earliest_day_distinct_timestamps` |  |
| `rq1.earliest_day_modal_count` | 21869 | `summary_rq1.json:earliest_day_modal_count` |  |
| `rq1.earliest_day_modal_share` | 0.739492 | `summary_rq1.json:earliest_day_modal_share` |  |
| `rq1.earliest_day_modal_timestamp` | 2022-03-02T23:29:05.000Z | `summary_rq1.json:earliest_day_modal_timestamp` |  |
| `rq1.first_month` | 2022-03 | `summary_rq1.json:first_month` |  |
| `rq1.first_month_count` | 32621 | `summary_rq1.json:first_month_count` |  |
| `rq1.last_month` | 2026-10 | `summary_rq1.json:last_month` |  |
| `rq1.n_models` | 3138954 | `summary_rq1.json:n_models` |  |
| `rq1.n_months` | 56 | `summary_rq1.json:n_months` |  |
| `rq1.partial_month` | 2026-10 | `summary_rq1.json:partial_month` |  |
| `rq1.peak_count` | 125030 | `summary_rq1.json:peak_count` |  |
| `rq1.peak_month` | 2024-09 | `summary_rq1.json:peak_month` |  |
| `rq1.top10_tasks_by_total` | {"unknown": 2057742, "text-generation": 422224, "text-classification": 123924, "text-to-image": 111444, "reinforcement-learning": 78390, "image-text-to-text": 41815, "automatic-speech-recognition":… | `summary_rq1.json:top10_tasks_by_total` |  |
| `rq1.unknown_task_models` | 2057742 | `summary_rq1.json:unknown_task_models` |  |
| `rq1.unknown_task_share` | 0.65555 | `summary_rq1.json:unknown_task_share` |  |
| `rq2.downloads.distinct_values` | 14894 | `concentration_stats.json:downloads.distinct_values` |  |
| `rq2.downloads.gini` | 0.995355 | `concentration_stats.json:downloads.gini` |  |
| `rq2.downloads.n_models` | 3138954 | `concentration_stats.json:downloads.n_models` |  |
| `rq2.downloads.top_10pct_share` | 0.994817 | `concentration_stats.json:downloads.top_10pct_share` |  |
| `rq2.downloads.top_1pct_share` | 0.971272 | `concentration_stats.json:downloads.top_1pct_share` |  |
| `rq2.downloads.top_5pct_share` | 0.990904 | `concentration_stats.json:downloads.top_5pct_share` |  |
| `rq2.downloads.total_downloads` | 2788919780 | `concentration_stats.json:downloads.total_downloads` |  |
| `rq2.downloads.zero_value_models` | 1366793 | `concentration_stats.json:downloads.zero_value_models` |  |
| `rq2.downloads.zero_value_share` | 0.435429 | `concentration_stats.json:downloads.zero_value_share` |  |
| `rq2.downloadsAllTime.distinct_values` | 47869 | `concentration_stats.json:downloadsAllTime.distinct_values` |  |
| `rq2.downloadsAllTime.gini` | 0.998117 | `concentration_stats.json:downloadsAllTime.gini` |  |
| `rq2.downloadsAllTime.n_models` | 3138954 | `concentration_stats.json:downloadsAllTime.n_models` |  |
| `rq2.downloadsAllTime.top_10pct_share` | 0.998409 | `concentration_stats.json:downloadsAllTime.top_10pct_share` |  |
| `rq2.downloadsAllTime.top_1pct_share` | 0.987017 | `concentration_stats.json:downloadsAllTime.top_1pct_share` |  |
| `rq2.downloadsAllTime.top_5pct_share` | 0.996536 | `concentration_stats.json:downloadsAllTime.top_5pct_share` |  |
| `rq2.downloadsAllTime.total_downloads` | 75949397771 | `concentration_stats.json:downloadsAllTime.total_downloads` |  |
| `rq2.downloadsAllTime.zero_value_models` | 1317858 | `concentration_stats.json:downloadsAllTime.zero_value_models` |  |
| `rq2.downloadsAllTime.zero_value_share` | 0.41984 | `concentration_stats.json:downloadsAllTime.zero_value_share` |  |
| `rq2.meta.method` | {"gini": "频数加权秩公式 G=2S/(n·total)-1，精确值，非采样近似", "topk": "按值降序取前 max(1,int(n·p)) 个模型；并列值整体同权，结果可复现", "lorenz": "频数聚合后的真实累计点，未抽样、未插值"} | `summary_rq2.json:method` |  |
| `rq2.meta.metric_note` | downloads 为近 30 天窗口口径，downloadsAllTime 为历史累计口径，两者相差约 27 倍，任何集中度结论都必须声明所用口径，不可混用。 | `summary_rq2.json:metric_note` |  |
| `rq2.meta.powerlaw_note` | 本分析只描述长尾形态（Lorenz 偏离、CCDF 双对数近似直线），**未**做严格幂律拟合与模型比较，因此不宣称已证明幂律分布。 | `summary_rq2.json:powerlaw_note` |  |
| `rq2_groups.calibers` | ["downloads", "downloadsAllTime"] | `concentration_groups.json:calibers` |  |
| `rq2_groups.created_year.downloads.n_groups` | 5 | `concentration_groups.json:groupings.created_year.downloads.n_groups` |  |
| `rq2_groups.created_year.downloads.n_groups_ge_1000` | 5 | `concentration_groups.json:groupings.created_year.downloads.n_groups_ge_1000` |  |
| `rq2_groups.created_year.downloads.per_group` | {"2022": {"n_models": 100392, "total_downloads": 924709435, "distinct_values": 2980, "zero_value_models": 36336, "zero_value_share": 0.361941, "gini": 0.998291, "top_1pct_share": 0.992198, "top_5pc… | `concentration_groups.json:groupings.created_year.downloads.per_group` |  |
| `rq2_groups.created_year.downloads.theil_decomposition` | {"theil_total": 7.259273051615285, "theil_within": 6.736795341941718, "theil_between": 0.5224777096735665, "between_share": 0.0719738334621961, "groups": [{"group": "2022", "n": 64056, "n_zero": 36… | `concentration_groups.json:groupings.created_year.downloads.theil_decomposition` | Theil T 的组间/组内分解（between_share 为组间占比） |
| `rq2_groups.created_year.downloadsAllTime.n_groups` | 5 | `concentration_groups.json:groupings.created_year.downloadsAllTime.n_groups` |  |
| `rq2_groups.created_year.downloadsAllTime.n_groups_ge_1000` | 5 | `concentration_groups.json:groupings.created_year.downloadsAllTime.n_groups_ge_1000` |  |
| `rq2_groups.created_year.downloadsAllTime.per_group` | {"2022": {"n_models": 100392, "total_downloads": 43152858003, "distinct_values": 11338, "zero_value_models": 34020, "zero_value_share": 0.338872, "gini": 0.997256, "top_1pct_share": 0.976618, "top_… | `concentration_groups.json:groupings.created_year.downloadsAllTime.per_group` |  |
| `rq2_groups.created_year.downloadsAllTime.theil_decomposition` | {"theil_total": 7.656842418189406, "theil_within": 6.313836200810419, "theil_between": 1.3430062173789872, "between_share": 0.17539948506561592, "groups": [{"group": "2022", "n": 66372, "n_zero": 3… | `concentration_groups.json:groupings.created_year.downloadsAllTime.theil_decomposition` | Theil T 的组间/组内分解（between_share 为组间占比） |
| `rq2_groups.has_lineage.downloads.n_groups` | 2 | `concentration_groups.json:groupings.has_lineage.downloads.n_groups` |  |
| `rq2_groups.has_lineage.downloads.n_groups_ge_1000` | 2 | `concentration_groups.json:groupings.has_lineage.downloads.n_groups_ge_1000` |  |
| `rq2_groups.has_lineage.downloads.per_group` | {"no": {"n_models": 2251922, "total_downloads": 1564952093, "distinct_values": 9544, "zero_value_models": 1259462, "zero_value_share": 0.559283, "gini": 0.997095, "top_1pct_share": 0.985332, "top_5… | `concentration_groups.json:groupings.has_lineage.downloads.per_group` |  |
| `rq2_groups.has_lineage.downloads.theil_decomposition` | {"theil_total": 7.259273051615283, "theil_within": 7.259270578509938, "theil_between": 2.473105345135191e-06, "between_share": 3.406822318916483e-07, "groups": [{"group": "no", "n": 992460, "n_zero… | `concentration_groups.json:groupings.has_lineage.downloads.theil_decomposition` | Theil T 的组间/组内分解（between_share 为组间占比） |
| `rq2_groups.has_lineage.downloadsAllTime.n_groups` | 2 | `concentration_groups.json:groupings.has_lineage.downloadsAllTime.n_groups` |  |
| `rq2_groups.has_lineage.downloadsAllTime.n_groups_ge_1000` | 2 | `concentration_groups.json:groupings.has_lineage.downloadsAllTime.n_groups_ge_1000` |  |
| `rq2_groups.has_lineage.downloadsAllTime.per_group` | {"no": {"n_models": 2251922, "total_downloads": 59957801038, "distinct_values": 33930, "zero_value_models": 1219229, "zero_value_share": 0.541417, "gini": 0.998932, "top_1pct_share": 0.993694, "top… | `concentration_groups.json:groupings.has_lineage.downloadsAllTime.per_group` |  |
| `rq2_groups.has_lineage.downloadsAllTime.theil_decomposition` | {"theil_total": 7.656842418189413, "theil_within": 7.54743378524089, "theil_between": 0.10940863294852302, "between_share": 0.014289001519557783, "groups": [{"group": "no", "n": 1032693, "n_zero": … | `concentration_groups.json:groupings.has_lineage.downloadsAllTime.theil_decomposition` | Theil T 的组间/组内分解（between_share 为组间占比） |
| `rq2_groups.n_models_total` | 3138954 | `concentration_groups.json:n_models_total` |  |
| `rq2_groups.task.downloads.n_groups` | 57 | `concentration_groups.json:groupings.task.downloads.n_groups` |  |
| `rq2_groups.task.downloads.n_groups_ge_1000` | 34 | `concentration_groups.json:groupings.task.downloads.n_groups_ge_1000` |  |
| `rq2_groups.task.downloads.per_group` | {"any-to-any": {"n_models": 11658, "total_downloads": 27808339, "distinct_values": 549, "zero_value_models": 9421, "zero_value_share": 0.808115, "gini": 0.997215, "top_1pct_share": 0.988054, "top_5… | `concentration_groups.json:groupings.task.downloads.per_group` |  |
| `rq2_groups.task.downloads.theil_decomposition` | {"theil_total": 7.259273051615281, "theil_within": 6.24795027816288, "theil_between": 1.0113227734524015, "between_share": 0.13931460715992344, "groups": [{"group": "text-generation", "n": 402394, … | `concentration_groups.json:groupings.task.downloads.theil_decomposition` | Theil T 的组间/组内分解（between_share 为组间占比） |
| `rq2_groups.task.downloadsAllTime.n_groups` | 57 | `concentration_groups.json:groupings.task.downloadsAllTime.n_groups` |  |
| `rq2_groups.task.downloadsAllTime.n_groups_ge_1000` | 34 | `concentration_groups.json:groupings.task.downloadsAllTime.n_groups_ge_1000` |  |
| `rq2_groups.task.downloadsAllTime.per_group` | {"any-to-any": {"n_models": 11658, "total_downloads": 213905347, "distinct_values": 997, "zero_value_models": 9374, "zero_value_share": 0.804083, "gini": 0.996911, "top_1pct_share": 0.983613, "top_… | `concentration_groups.json:groupings.task.downloadsAllTime.per_group` |  |
| `rq2_groups.task.downloadsAllTime.theil_decomposition` | {"theil_total": 7.656842418189409, "theil_within": 6.504018463975249, "theil_between": 1.1528239542141596, "between_share": 0.1505612746418209, "groups": [{"group": "fill-mask", "n": 18264, "n_zero… | `concentration_groups.json:groupings.task.downloadsAllTime.theil_decomposition` | Theil T 的组间/组内分解（between_share 为组间占比） |
| `rq3.avg_children_count` | 10.4133 | `graph_stats.json:avg_children_count` |  |
| `rq3.avg_descendant_count_khop` | 17.0759 | `graph_stats.json:avg_descendant_count_khop` |  |
| `rq3.engine` | spark_native | `graph_stats.json:engine` |  |
| `rq3.external_edge_share` | 0.016131 | `graph_stats.json:external_edge_share` |  |
| `rq3.max_children_count` | 43419 | `graph_stats.json:max_children_count` |  |
| `rq3.max_descendant_count_khop` | 43682 | `graph_stats.json:max_descendant_count_khop` |  |
| `rq3.n_base_plus_derived` | 975365 | `graph_stats.json:n_base_plus_derived` |  |
| `rq3.n_distinct_base_models` | 88333 | `graph_stats.json:n_distinct_base_models` |  |
| `rq3.n_distinct_derived_models` | 887032 | `graph_stats.json:n_distinct_derived_models` |  |
| `rq3.n_distinct_external_parents` | 3620 | `graph_stats.json:n_distinct_external_parents` |  |
| `rq3.n_edges` | 919839 | `graph_stats.json:n_edges` |  |
| `rq3.n_edges_with_external_parent` | 14838 | `graph_stats.json:n_edges_with_external_parent` |  |
| `rq3.n_models_total` | 3138954 | `graph_stats.json:n_models_total` |  |
| `rq3.n_models_without_base` | 2251922 | `graph_stats.json:n_models_without_base` |  |
| `rq3.n_nodes_both_base_and_derived` | 44299 | `graph_stats.json:n_nodes_both_base_and_derived` |  |
| `rq3.n_nodes_involved` | 931066 | `graph_stats.json:n_nodes_involved` |  |
| `rq3.node_count_note` | n_nodes_involved 是 (src ∪ dst) 去重后的节点数，与 graphframes_result.json 的 n_vertices_in_graph 应完全相等；n_base_plus_derived 是直接相加，会重复计算同时扮演两种角色的模型。 | `graph_stats.json:node_count_note` |  |
| `rq3.reach_hops` | 3 | `graph_stats.json:reach_hops` |  |
| `rq3.relation_counts` | {"finetune": 354320, "adapter": 329707, "quantized": 187784, "merge": 48028} | `graph_stats.json:relation_counts` |  |
| `rq3.seconds` | 146.8 | `graph_stats.json:seconds` |  |
| `rq3.top10_bases_by_children` | [{"base_model": "black-forest-labs/FLUX.1-dev", "children_count": 43419, "downloads": 661619, "downloadsAllTime": 34112217}, {"base_model": "Qwen/Qwen1.5-0.5B", "children_count": 32534, "downloads"… | `graph_stats.json:top10_bases_by_children` |  |
| `rq3_family.case_selection_rule` | {"star": "children_count 最大的底座", "chain": "children_count>=100 中 subtree_height 最大者", "merge": "被判为 merge 且 children_count>=20 中 merge 出边数最多者"} | `family_cases.json:case_selection_rule` |  |
| `rq3_family.cases` | [{"role": "star", "base_model": "black-forest-labs/FLUX.1-dev", "pipeline_tag": "text-to-image", "created_at": "2024-07-31T21:13:44.000Z", "children_count": 43419, "descendant_count_khop": 43682, "… | `family_cases.json:cases` | 三个代表性家族：star / chain / merge |
| `rq3_family.children_count_downloads_correlation` | {"pearson": 0.05809199389423941, "spearman": 0.2021036285860482} | `family_cases.json:children_count_downloads_correlation` | 同一快照内 children_count 与下载量的秩相关；不作因果解释 |
| `rq3_family.engine` | spark_native + python_graph | `family_ext_stats.json:engine` |  |
| `rq3_family.max_children_count` | 43419 | `family_ext_stats.json:max_children_count` |  |
| `rq3_family.max_root_depth` | 151 | `family_ext_stats.json:max_root_depth` |  |
| `rq3_family.max_subtree_height` | 151 | `family_ext_stats.json:max_subtree_height` |  |
| `rq3_family.mean_subtree_height_all_bases` | 2.3529 | `family_ext_stats.json:mean_subtree_height_all_bases` |  |
| `rq3_family.multi_parent_rows` | [{"parents_count": "1", "n_models": "871985"}, {"parents_count": "2", "n_models": "8747"}, {"parents_count": "3", "n_models": "2555"}, {"parents_count": "4", "n_models": "1621"}, {"parents_count": … | `family_multi_parent.csv` |  |
| `rq3_family.multi_parent_share_of_derived` | 0.016963 | `family_ext_stats.json:multi_parent_share_of_derived` |  |
| `rq3_family.n_distinct_base_models` | 88333 | `family_ext_stats.json:n_distinct_base_models` |  |
| `rq3_family.n_distinct_derived_models` | 887032 | `family_ext_stats.json:n_distinct_derived_models` |  |
| `rq3_family.n_edges` | 919839 | `family_ext_stats.json:n_edges` |  |
| `rq3_family.n_merge_edges` | 48028 | `family_ext_stats.json:n_merge_edges` |  |
| `rq3_family.n_models_total` | 3138954 | `family_ext_stats.json:n_models_total` |  |
| `rq3_family.n_multi_parent_models` | 15047 | `family_ext_stats.json:n_multi_parent_models` |  |
| `rq3_family.n_nodes_involved` | 931066 | `family_ext_stats.json:n_nodes_involved` |  |
| `rq3_family.relation_counts` | {"adapter": 329707, "merge": 48028, "finetune": 354320, "quantized": 187784} | `family_ext_stats.json:relation_counts` |  |
| `rq3_family.relation_shares` | {"adapter": 0.35844, "merge": 0.052213, "finetune": 0.385198, "quantized": 0.204149} | `family_ext_stats.json:relation_shares` |  |
| `rq3_family.role_note` | 星型底座以'被大量浅层复用'为主，链型底座以'被逐层继续衍生'为主，合并型底座以'被多父合并'为主；这些是**当前快照的结构差异**，不是因果机制，也不代表模型质量。 | `family_cases.json:role_note` |  |
| `rq3_family.scc_summary` | {"n_nodes": 931066, "n_strongly_connected_components": 931052, "n_scc_size_gt1": 10, "max_scc_size": 3, "n_nodes_in_cycles": 24, "cycle_node_share": 2.578e-05, "n_self_loop_edges": 2060, "condensat… | `family_scc_summary.json` |  |
| `rq3_family.scope_note` | 结论限定在**显式继承网络**内；base_models 缺失率 71.74%，无显式底座的模型不在图内。深度/高度为凝聚 DAG 上的最长路径，不等同于可达后代数。 | `family_ext_stats.json:scope_note` |  |
| `rq3_family.seconds` | 194.3 | `family_ext_stats.json:seconds` |  |
| `rq3_family.structure_class_counts_all_bases` | {"mixed": 78381, "merge": 8939, "star": 647, "chain": 366} | `family_ext_stats.json:structure_class_counts_all_bases` |  |
| `rq3_family.structure_class_counts_by_tier` | {"all": {"mixed": 78381, "merge": 8939, "star": 647, "chain": 366}, "ge10": {"mixed": 3482, "merge": 1595, "star": 647, "chain": 257}, "ge100": {"star": 647, "chain": 169, "merge": 43, "mixed": 1}} | `family_ext_stats.json:structure_class_counts_by_tier` |  |
| `rq3_family.task_structure_rows` | [{"task": "text-generation", "n_bases": "47521", "total_children": "522066", "median_children": "2.0", "avg_height": "3.1324887944277267", "merge_share": "0.09072839259062516", "multiparent_share":… | `family_task_structure.csv` | 按 pipeline_tag 汇总的家族结构（底座数/衍生数/中位出度/平均高度/merge 占比/多父占比） |
| `rq3_family.top_bases_by_children` | [{"base_model": "black-forest-labs/FLUX.1-dev", "children_count": 43419, "subtree_height": 5, "structure": "star"}, {"base_model": "Qwen/Qwen1.5-0.5B", "children_count": 32534, "subtree_height": 3,… | `family_ext_stats.json:top_bases_by_children` |  |
| `rq3_gf.budget_seconds` | 2400 | `graphframes_result.json:budget_seconds` |  |
| `rq3_gf.isolated_models_not_in_graph` | 2207888 | `graphframes_result.json:isolated_models_not_in_graph` |  |
| `rq3_gf.n_edges` | 919839 | `graphframes_result.json:n_edges` |  |
| `rq3_gf.n_models_total` | 3138954 | `graphframes_result.json:n_models_total` |  |
| `rq3_gf.n_vertices_in_graph` | 931066 | `graphframes_result.json:n_vertices_in_graph` |  |
| `rq3_gf.status` | completed | `graphframes_result.json:status` |  |
| `rq3_gf.step.connected_components` | {"seconds": 352.9, "components_in_graph": 42890, "components_total": 2250778} | `graphframes_result.json:steps.connected_components` |  |
| `rq3_gf.step.degrees` | {"seconds": 3.9, "rows": 931066} | `graphframes_result.json:steps.degrees` |  |
| `rq3_gf.step.label_propagation` | {"seconds": 215.0, "n_communities": 126431} | `graphframes_result.json:steps.label_propagation` |  |
| `rq3_gf.step.merge_node_metrics` | {"seconds": 13.6, "rows": 931066} | `graphframes_result.json:steps.merge_node_metrics` |  |
| `rq3_gf.step.pagerank_forward` | {"seconds": 375.0} | `graphframes_result.json:steps.pagerank_forward` |  |
| `rq3_gf.step.pagerank_reverse` | {"seconds": 348.9} | `graphframes_result.json:steps.pagerank_reverse` |  |
| `rq3_gf.total_seconds` | 59.1 | `graphframes_result.json:total_seconds` |  |
| `rq3_gf.viz_subgraph` | {"component": 1, "component_size": 417584, "n_nodes_shown": 1500, "n_links_shown": 2124, "truncated": true} | `graphframes_result.json:viz_subgraph` |  |
| `rq4.correlations` | [{"feature": "likes_log", "pearson": 0.38847635977700046, "spearman": 0.27052267505417615}, {"feature": "model_age_days", "pearson": -0.0068121865848762364, "spearman": 0.004530479834124941}, {"fea… | `ml_summary.json:correlations` |  |
| `rq4.feature_importance_note` | 重要性是 one-hot 分列按源列汇总后的结果，只表示模型内部的统计贡献，不代表因果，也不代表模型质量。若 likes_log 占主导，只能说明‘同一快照内的点赞与下载高度同期相关’。 | `ml_summary.json:feature_importance_note` |  |
| `rq4.features` | {"numeric": ["likes_log", "model_age_days", "tag_count", "children_count", "maintenance_span_days"], "categorical": ["task", "library_top"], "encoding": "两类特征均 one-hot；Top-50 库词表只在训练集统计", "top_libr… | `ml_summary.json:features` |  |
| `rq4.metrics_on_test` | {"Baseline_mean": {"rmse": 1.7466664323431675, "mae": 1.403250302481077, "r2": -5.279920219258827e-06}, "LinearRegression": {"rmse": 1.3328008239128513, "mae": 0.9501018464612001, "r2": 0.417745504… | `ml_summary.json:metrics_on_test` |  |
| `rq4.n_duplicate_model_id_groups` | 0 | `ml_summary.json:n_duplicate_model_id_groups` |  |
| `rq4.n_rows_full` | 3138954 | `ml_summary.json:n_rows_full` |  |
| `rq4.research_positioning` | 研究对象是**当前快照**下模型属性与下载热度的统计关联，不是对未来是否会火的预测。label 与 likes 同取自一个时间点，存在同期相关，不能解释为前瞻性预测能力。 | `ml_summary.json:research_positioning` |  |
| `rq4.rf_importance_ranked` | [{"feature": "tag_count", "importance": 0.4129934556193599}, {"feature": "library_top", "importance": 0.19444540038234442}, {"feature": "likes_log", "importance": 0.15142080137937414}, {"feature": … | `ml_summary.json:rf_importance_ranked` |  |
| `rq4.sampling` | {"method": "确定性哈希 crc32(model_id) % 5 == 0", "n_sample": 626269, "sample_share": 0.199515, "reason": "全量 314 万行直接训练 GBT 在本机代价过高；用户明确要求不要盲目对全量做复杂 GBT 训练。"} | `ml_summary.json:sampling` |  |
| `rq4.split` | {"method": "randomSplit([0.8, 0.19999999999999996], seed=42)", "seed": 42, "n_train": 501049, "n_test": 125220, "train_test_overlap": 0, "note": "先划分再拟合；所有预处理只在 train 上 fit，指标只在 test 上计算。"} | `ml_summary.json:split` |  |
| `rq4.target` | label = log1p(downloads)，downloads 为近 30 天下载计数 | `ml_summary.json:target` |  |
| `rq5.caliber_note` | downloads（近 30 天）与 downloadsAllTime（历史累计）各自独立成列，不混合、不相加。 | `rq5_summary.json:caliber_note` |  |
| `rq5.family_note` | family_size = children_count + 1 只是 1 跳直系家族规模；avg_direct_family_size 与 avg_children_count 同源，均不等于连通分量规模或可达后代数（后者见 graph_stats.json）。 | `rq5_summary.json:family_note` |  |
| `rq5.highest_base_relation_coverage` | [{"task": "text-ranking", "value": 0.84308131}, {"task": "visual-document-retrieval", "value": 0.81382979}, {"task": "text-to-image", "value": 0.71342558}, {"task": "video-text-to-text", "value": 0… | `rq5_summary.json:highest_base_relation_coverage` |  |
| `rq5.min_distinct_values_for_gini` | 20 | `rq5_summary.json:min_distinct_values_for_gini` |  |
| `rq5.min_group_for_comparison` | 200 | `rq5_summary.json:min_group_for_comparison` |  |
| `rq5.n_models_total` | 3138954 | `rq5_summary.json:n_models_total` |  |
| `rq5.n_tasks_comparable` | 50 | `rq5_summary.json:n_tasks_comparable` |  |
| `rq5.n_tasks_labeled` | 56 | `rq5_summary.json:n_tasks_labeled` |  |
| `rq5.threshold_note` | n_models < 200 或唯一取值数 < 20 的任务组 comparable=False，其 Gini 由极少数模型决定，不可与大任务组直接比较；unknown 组（任务标签缺失的占位组）恒为 comparable=False，不参与比较。 | `rq5_summary.json:threshold_note` |  |
| `rq5.top10_by_downloads_total` | [{"task": "text-generation", "value": 500535196}, {"task": "sentence-similarity", "value": 438924283}, {"task": "image-text-to-text", "value": 299808409}, {"task": "feature-extraction", "value": 16… | `rq5_summary.json:top10_by_downloads_total` |  |
| `rq5.top10_by_gini_downloads` | [{"task": "sentence-similarity", "value": 0.997863}, {"task": "any-to-any", "value": 0.997215}, {"task": "fill-mask", "value": 0.997183}, {"task": "feature-extraction", "value": 0.9967}, {"task": "… | `rq5_summary.json:top10_by_gini_downloads` |  |
| `rq5.top10_by_n_models` | [{"task": "text-generation", "value": 422224}, {"task": "text-classification", "value": 123924}, {"task": "text-to-image", "value": 111444}, {"task": "reinforcement-learning", "value": 78390}, {"ta… | `rq5_summary.json:top10_by_n_models` |  |
| `rq5.unknown_note` | pipeline_tag 缺失的模型统一记为 unknown，单独报告；所有跨任务比较只在已标注任务之间进行，结论只覆盖已标注部分。 | `rq5_summary.json:unknown_note` |  |
| `rq5.unknown_task` | {"task": "unknown", "is_unknown": true, "comparable": false, "n_models": 2057742, "n_models_share": 0.65555022, "downloads_total": 366100442, "downloadsAllTime_total": 8043862697, "downloads_median… | `rq5_summary.json:unknown_task` |  |
