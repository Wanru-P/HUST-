# 三个代表性家族案例（Task B）

> 生成时间：2026-10-13T12:01:38.271341+00:00  
> 数据快照：`2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`  
> 本文件由 `write_ext_docs.py` 自动生成，**所有数字取自结果文件，未手抄**。

---

## 1. 选例规则

| 角色 | 选例规则 |
| --- | --- |
| star 星型 | `children_count` 最大的底座 |
| chain 链型 | `children_count ≥ 100` 中 `subtree_height` 最大者 |
| merge 合并型 | 被判为 `merge` 且 `children_count ≥ 20` 中 merge 出边最多者 |

> 选例规则写死在 `analysis_family_cases.py`，可复现；三个案例分属不同结构，互不重叠。

---

## 2. 三个案例总览

| 案例 | 角色 | 任务 | 创建时间 | 直接衍生 | 最长链 | 结构 |
| --- | --- | --- | --- | --- | --- | --- |
| black-forest-labs/FLUX.1-dev | star | text-to-image | 2024-07-31 | 43,419 | 5 | star |
| meta-llama/Meta-Llama-3-8B | chain | text-generation | 2024-04-17 | 1,724 | 151 | chain |
| Laxhar/noobai-XL-1.1 | merge | text-to-image | 2024-11-21 | 539 | 2 | merge |

## 3.1 black-forest-labs/FLUX.1-dev（star）

- 任务：`text-to-image`；创建于 2024-07-31T21:13:44.000Z；
- 直接衍生 **43,419** 个，≤3 跳可达后代 **43,682** 个，最长衍生链 **5** 跳；
- 关系构成：finetune 613 / adapter 42,702 / quantized 94 / merge 10；
- 底座自身 `downloadsAllTime` = **34,112,217**；全部直接衍生的 `downloadsAllTime` 合计 = **46,643,574**；底座/衍生比 = **0.731**；
- 衍生相对底座的创建时间：中位滞后 **223.1** 天（最早 -612.8 天，最晚 800.4 天）；
- 在底座**之后**创建的衍生模型 43,408 个（占 99.97%）。

衍生热度最高的子模型：

| 子模型 | 关系 | downloadsAllTime |
| --- | --- | --- |
| ByteDance/Hyper-SD | adapter | 4,642,736 |
| city96/FLUX.1-dev-gguf | quantized | 3,503,765 |
| Shakker-Labs/FLUX.1-dev-LoRA-AntiBlur | adapter | 3,415,546 |
| XLabs-AI/flux-RealismLora | adapter | 2,863,695 |
| xey/sldr_flux_nsfw_v2-studio | adapter | 1,053,480 |
| Shakker-Labs/AWPortrait-FL | finetune | 1,043,788 |
| alimama-creative/FLUX.1-Turbo-Alpha | finetune | 810,470 |
| InstantX/FLUX.1-dev-Controlnet-Union | finetune | 712,860 |

## 3.2 meta-llama/Meta-Llama-3-8B（chain）

- 任务：`text-generation`；创建于 2024-04-17T09:35:16.000Z；
- 直接衍生 **1,724** 个，≤3 跳可达后代 **8,784** 个，最长衍生链 **151** 跳；
- 关系构成：finetune 613 / adapter 738 / quantized 284 / merge 89；
- 底座自身 `downloadsAllTime` = **47,200,733**；全部直接衍生的 `downloadsAllTime` 合计 = **14,253,888**；底座/衍生比 = **3.311**；
- 衍生相对底座的创建时间：中位滞后 **142.6** 天（最早 -153.6 天，最晚 902.1 天）；
- 在底座**之后**创建的衍生模型 1,722 个（占 99.88%）。

衍生热度最高的子模型：

| 子模型 | 关系 | downloadsAllTime |
| --- | --- | --- |
| unsloth/llama-3-8b-bnb-4bit | quantized | 4,970,415 |
| MLP-KTLim/llama-3-Korean-Bllossom-8B | finetune | 2,161,222 |
| DeepMount00/Llama-3-8b-Ita | finetune | 1,134,703 |
| aaditya/Llama3-OpenBioLLM-8B | finetune | 628,967 |
| openchat/openchat-3.6-8b-20240522 | finetune | 494,667 |
| dphn/dolphin-2.9-llama3-8b | finetune | 417,902 |
| winninghealth/WiNGPT2-Llama-3-8B-Chat | finetune | 352,592 |
| jondurbin/bagel-8b-v1.0 | finetune | 262,240 |

## 3.3 Laxhar/noobai-XL-1.1（merge）

- 任务：`text-to-image`；创建于 2024-11-21T13:43:47.000Z；
- 直接衍生 **539** 个，≤3 跳可达后代 **548** 个，最长衍生链 **2** 跳；
- 关系构成：finetune 224 / adapter 8 / quantized 3 / merge 304；
- 底座自身 `downloadsAllTime` = **982,138**；全部直接衍生的 `downloadsAllTime` 合计 = **5,963,852**；底座/衍生比 = **0.165**；
- 衍生相对底座的创建时间：中位滞后 **176.6** 天（最早 -0.8 天，最晚 679.4 天）；
- 在底座**之后**创建的衍生模型 538 个（占 99.81%）。

衍生热度最高的子模型：

| 子模型 | 关系 | downloadsAllTime |
| --- | --- | --- |
| John6666/nova-furry-xl-il-v120-sdxl | merge | 1,774,916 |
| John6666/janku-v5-nsfw-trained-noobai-rou-wei-illustrious-xl-v50-sdxl | merge | 1,772,935 |
| John6666/prefect-illustrious-xl-v3-sdxl | merge | 1,745,220 |
| John6666/nova-anime-xl-il-v80-sdxl | merge | 181,579 |
| John6666/mala-illustriousxl-v20-sdxl | merge | 80,153 |
| John6666/nova-reality-xl-illustrious-v40-sdxl | merge | 75,265 |
| John6666/smooth-mix-noobai-illustrious-pony-illustrious2-noobai-v2-sdxl | merge | 49,932 |
| John6666/ntr-mix-illustrious-xl-noob-xl-v777-sdxl | finetune | 26,825 |

---

## 4. 底座规模与下载量的关系

- `children_count` 与 `downloads` 的 Pearson 相关 = **0.0581**，Spearman 秩相关 = **0.2021**。

秩相关为正但很弱：被大量复用与下载热度之间存在**微弱的同向关系**，但远不足以解释下载量的差异。

---

## 5. 必须声明的限制

1. 三个案例都取自**同一个快照**，「创建时间差」是快照内可见模型之间的差，**不是完整历史**；
2. 底座热度 vs 衍生热度只是**同期描述**，**不能**推出「衍生带动了底座」或「底座带火了衍生」这类因果；
3. 下载量不代表模型质量；
4. 案例是**结构性代表**，不是「最成功的模型」榜单。

---

## 6. 产出文件

| 文件 | 内容 |
| --- | --- |
| `family_cases.json` | 三个案例的完整指标（含 top_children） |
| `family_cases_summary.csv` | 案例汇总长表 |
| `family_cases_children.csv` | 各案例的直接子节点明细 |
| `family_cases_relations.csv` | 各案例的关系类型计数 |

