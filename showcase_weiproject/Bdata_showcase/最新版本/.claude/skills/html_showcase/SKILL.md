---
name: html_showcase
description: 把答辩/汇报/课程项目的素材（文字稿、数据结论、配图）封装成横板（16:9 横向翻页）演示网页。采用「固定画布 + 等比缩放」架构保证内容不被裁切，Claude 风格配色与字体，包含多种布局模式（bento/时间线/hero 大图/统计带/选项卡等）与动画交互。当用户要求"生成展示网页 / PPT 式网页 / 答辩演示网页 / 横板网页 / 把素材做成网页"时使用。
---

# 展示网页制作 Skill（html_showcase）

## 一、技能定位

- 把「文字稿 + 数据结论 + 图片」做成一页页**横板（横向翻页）阅读**的网页，用于答辩、结题汇报、项目展示。
- 目标受众是**计算机专业学生 / 评审老师**，要求**专业 + 有设计感 + 信息详尽**。
- 视觉参照 **Claude 网页**：米白暖色底、陶土色强调、衬线标题、无衬线正文。
- 必须有**动画与交互**、**首页与尾页突出主题**、**内容精确**、**多种布局**、**每页饱满不裁切**。

## 二、核心架构：固定 16:9 画布 + 等比缩放（关键）

> ⚠️ 不要用「流体 100vw×100vh + clamp()」方案。它在不同屏幕比例下会让页面下部内容被裁切、需滚动，数字还可能被卡片裁剪。本项目统一用**固定画布 + 整体 scale 缩放**，像 PPT 一样稳定。

设计画布 **1280 × 720**（16:9），用 JS 等比缩放到视口内居中，两侧自动留边：

```css
#deck{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:var(--bg);overflow:hidden}
#stage{position:relative;width:1280px;height:720px;flex:none;transform-origin:center center;overflow:hidden;background:var(--bg);box-shadow:0 18px 60px rgba(31,30,29,.16);border:1px solid var(--line)}
#track{display:flex;height:100%;transition:transform .72s cubic-bezier(.77,0,.18,1);will-change:transform}
.slide{flex:0 0 100%;width:1280px;height:720px;position:relative;overflow:hidden;background:var(--bg)}
.slide-inner{position:relative;z-index:2;height:100%;padding:38px 50px 20px;display:flex;flex-direction:column}
```

```js
function fit(){
  var s = Math.min(window.innerWidth / 1280, window.innerHeight / 720);
  stage.style.transform = 'scale(' + s + ')';
}
function goTo(i){ track.style.transform = 'translateX(' + (-i * 1280) + 'px)'; }
window.addEventListener('resize', function(){ fit(); goTo(current); });
```

**要点**：
- 舞台内字号/间距/图片高度**一律用 px**（画布是固定的，再用 vw/vh 会双重缩放）。
- 顶部进度条、页码、底部圆点、翻页按钮、灯箱固定在**视口边缘**（`position:fixed`），不随画布缩放。
- 灯箱（lightbox）内部可用 vw/vh（它固定在视口，不参与舞台缩放）。

## 三、设计系统（Claude 风格）

### 配色（CSS 变量）
```css
:root{
  --bg:#faf9f5; --bg2:#f3efe6; --card:#ffffff; --card2:#f7f4ee;
  --ink:#1f1e1d; --ink2:#5b564d; --ink3:#8f897d;
  --line:#e7e2d7; --line2:#efece4;
  --accent:#d97757; --accentDark:#b9553a; --accentSoft:#f5e5dd;
  --green:#4f7d5c; --blue:#5b7da8; --gold:#c09a4b; /* 数据条配色 */
}
```

### 字体栈
```css
--serif:"Noto Serif SC","Source Han Serif SC","STZhongsong","STSong","SimSun",Georgia,serif; /* 标题/大数字 */
--sans:"Noto Sans SC","Source Han Sans SC","PingFang SC","Microsoft YaHei",system-ui,sans-serif; /* 正文 */
--mono:"IBM Plex Mono","SF Mono","JetBrains Mono",Consolas,monospace; /* 标签/数字 */
```
> 通过 Google Fonts 引入 Noto Serif/Sans SC，离线自动回退系统字体。

### 参考字号（1280×720 画布下）
页面标题 `.s-title` 34px；正文 `.card p`/`.tick li` 16px；卡片标签 `.lbl` 12.5px；图注 13px；大数字 `.stat .v` 36px。

## 四、工作流程

1. **扫描素材目录**：列出所有文件，区分文本稿（.txt/.md）与图片（.png/.jpg/.gif）。
2. **提取内容**：抽取「题目/成员/研究问题(RQ)/数据/方法/问题与解决/结论与发现/动机/未来」；建立「图片文件名 → 归属页 → 图注」映射。
3. **核实图片**：每个 `src` 与磁盘文件名**完全一致**（含中文、大小写）；顺便读取图片宽高比（PNG 用 `struct` 读 IHDR 第 16–24 字节），判断它是宽幅还是方图。
4. **确定页面结构**：通常 18–28 页；**过密页拆成两页**（如 RQ 页拆「方法/口径」与「结论/发现」），但**不改主要内容**。
5. **搭建骨架**：单文件 HTML，含 `<style>` 与 `<script>`，图片用相对路径。
6. **应用设计系统**：先写 CSS 变量与组件类，再写页面。
7. **写内容与布局**：每页用不同布局（见第六、七节），图片尽量大、内容居中、页面饱满。
8. **添加交互动画**：翻页、数字滚动、数据条、灯箱、选项卡、粒子背景、入场 reveal。
9. **质量校验**：跑校验脚本（见第十一节），0 错误后再交付。
10. **交付说明**：告知路径、打开方式、需替换的占位符。

## 五、页面结构模板（28 页示例）

1. 封面（主题大字 + 粒子网络背景 + 成员/班级/日期占位）
2. 目录（可点击跳转的编号列表）
3. 前置知识
4. 选题 / 研究问题概览
5–6. 数据与环境（可拆「分层/4V」与「架构图」两页）
7–8. 测试环境（可拆「运行环境」与「性能实验」两页）
9–10. 数据集特征（可拆「来源/字段」与「陷阱/缺失口径」两页）
11.【章节过渡页】研究问题
12–21. RQ1–RQ5（每个 RQ 拆「方法/口径」与「结论/发现」两页）
22.【章节过渡页】稳健性·总结·展望
23–24. 稳健性检查 / 总体结论
25. 创新点 · 收获
26. 挑战 · 投入
27. 未来打算
28. 尾页（Q&A + 一句话结论 + 粒子背景）

> RQ「方法页」= 研究问题/数据/方法 + 问题与解决 + 主图；RQ「结论页」= 结论 + 图网格 + 底部统计带。

## 六、布局模式库（避免「矩形拼接」）

| 类名 | 作用 |
|---|---|
| `.cols` | 两等栏 |
| `.cols-12` / `.cols-21` | 1:1.2 / 1.2:1 不等栏 |
| `.cols-13` | 1:1.55（**图片占主导**时用，让图更大） |
| `.cols-3` / `.cols-4` / `.cols-5` | 三/四/五列卡片 |
| `.stats` / `.stats.c2` / `.stats.c3` | 底部统计带（大数字，**必须全宽**，见第九节） |
| `.bars` + `.bar-row` | 动画数据条（`--w` 写真实百分比） |
| `.pipe` | 流程色块条 |
| `.agenda` | 目录列表 |
| `.divider` | 章节过渡页（超大轮廓数字） |
| `.tabs` + `.tabpane` | 选项卡（多案例切换） |
| `.bento` + `.tile[.wide/.tall]` | 不规则磁贴网格（打破等宽矩形） |
| `.timeline` | 纵向时间线（带竖线+圆点） |
| `.statline` | 分段横向统计条（竖分隔线） |
| `.hero-num` | 巨型数字 |
| `.feat-item` | 编号特性卡（大序号 + 标题 + 描述） |
| `.challenges` + `.challenge` | 彩色左框挑战网格 |

**原则**：不同页面交替使用不同模式，避免全篇都是「左文右图」或「上下堆卡片」。

## 七、图片规格与排布（图片要够大）

### 三级图片
```css
.fig img{width:100%;object-fit:contain;max-height:215px}  /* 普通图 */
.fig.big img{max-height:350px}                             /* 大图 */
.fig.hero img{max-height:510px}                            /* 主图，接近满高 */
```
- 主图用 `.hero`（页面唯一主视觉）；次要图用 `.big` 或 `.fig` 缩略点击放大。
- 图片外框 `padding` 尽量小（5px），让图占框架更大比例。

### 宽幅图 vs 方图（按宽高比决定排布）
- **读 PNG 宽高比**：`ratio = w/h`。
- 宽幅横图（ratio ≈ 1.7–2.5）是**宽度受限**的：并排会很小，**上下堆叠（`grid-template-columns:1fr`）占满整列宽度**才能放大。
- 方图/近方图（ratio < 1.6）是**高度受限**的：加大 `max-height`（用 `.big`）即可放大。
- 两张图要突出时，改「左右并排」为「上下堆叠」；四张图用 2×2 网格。

## 八、交互与动画清单

1. **翻页**：`#track{transition:transform .72s cubic-bezier(.77,0,.18,1)}`，JS `translateX(-i*1280px)`。
2. **导航**：方向键/空格/滚轮（防抖）/上一页下一页按钮/底部圆点（当前页拉长高亮）/顶部进度条+页码。
3. **入场 reveal**：`.reveal{opacity:0;transform:translateY(22px)}`，`.slide.on .reveal` 显现，`.d1`–`.d7` 错峰。
4. **数字滚动 count-up**：`[data-count]` 进入页面从 0 动画到目标（整数 `toLocaleString`，小数 `toFixed`）。
5. **动画数据条**：`.bar-fill{width:0}`，`.slide.on .bar-fill{width:var(--w)}`。
6. **灯箱 lightbox**：点图放大 + 图注，Esc/点击关闭。
7. **选项卡 tabs**：多案例切换。
8. **粒子网络 canvas**：封面/尾页背景，节点漂移 + 距离连线。
9. **图片悬停**：上浮 + 放大镜提示。

## 九、排版与留白规范（血泪经验）

1. **卡片紧贴内容，不要用 `flex:1` 拉伸**：`.card` 设 `flex:1` 会把短卡片拉高、内部出现大块空白。让卡片自然高度、内容紧贴。
2. **列内内容垂直居中**：`.col{display:flex;flex-direction:column;justify-content:center}`。**不要用 `space-between`**（会把卡片顶到上下两端、中间留大空档）。
3. **数字防遮挡**：4 个大数字的统计带**必须放全宽底部**（`stats` 是 slide-inner 的直接子元素），**不要塞进窄列**——窄列里 `0.995355` 这类长数字会被卡片 `overflow:hidden` 裁掉。
4. **每页饱满**：图片尽量大、字号够大、卡片 padding 收紧（14px 18px）；内容不饱时优先「放大图片/字号」而非拉伸卡片。
5. **过密即拆页**：一页塞不下「文字 + 多图 + 统计带」时，拆成两页（文字页 + 图片页），每页一屏正好放下。
6. **统计带紧凑**：`.stat` padding 10px 14px、`.stats` padding-top 10px，把垂直空间让给图片。

## 十、数据可视化规范（内容精确）

- 所有数字只来自同一份结果清单，**不写图里没有的数字**。
- 数据条 `--w` = 真实数值 × 100%（0.413 → 41.3%），右侧标注精确值。
- 图注带**限制条件**（「未抽样未插值」「50 个可比需带阈值」「直系规模仅 1 跳」）。
- 缺失值（任务 65.55%、血缘 71.74%）显式报告，不推断不填补。

## 十一、质量校验清单（交付前必跑）

用 Python（内置 `html.parser`）校验，确认：
- `<section>` 数量 = 预期页数；
- 所有 `<img src>` 文件**真实存在**（0 缺失）；
- HTML 标签**全部配平**（0 unclosed / 0 mismatch）；
- 无残留占位符（`__CSS__`、`__SLIDES__`）、无废弃函数（`SW`；`fit()` 与缩放架构配套保留）。

```python
import re, os
from html.parser import HTMLParser
d = r'<素材目录>'
s = open(os.path.join(d,'index.html'),encoding='utf-8').read()
VOID={'area','base','br','col','embed','hr','img','input','link','meta','param','source','track','wbr'}
class P(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True); self.stack=[]; self.errs=[]
    def handle_starttag(self,t,a):
        if t not in VOID: self.stack.append(t)
    def handle_endtag(self,t):
        if t in VOID: return
        if self.stack and self.stack[-1]==t: self.stack.pop()
        else: self.errs.append(t)
p=P(); p.feed(s); p.close()
print('slides:', s.count('<section'))
print('unclosed:', p.stack, 'errors:', p.errs)
srcs = re.findall(r'src="([^"]+)"', s)
print('missing imgs:', [x for x in srcs if not os.path.exists(os.path.join(d,x))])
```

## 十二、常见坑与规避

- **内容被裁切/需滚动**：不要用流体 `100vw×100vh + clamp`；用固定 1280×720 画布 + `transform:scale()`，画布内全部 px。
- **数字被卡片裁掉**：长数字（小数/大整数）不要放窄列统计带，放全宽底部。
- **卡片内部大块空白**：不要给卡片 `flex:1` 拉伸；让卡片紧贴内容、列内 `justify-content:center`。
- **中间留白**：列内对齐用 `center`，别用 `space-between`。
- **宽幅横图显小**：先读宽高比；横图改上下堆叠占满整列，别并排。
- **方图显小**：加大 `max-height`（`.big`/`.hero`）。
- **竖长图撑爆版面**：`max-height` + `object-fit:contain`。
- **中文文件名编码**：校验图片存在性用 Python（UTF-8），别用 PowerShell 5.1 读 UTF-8 无 BOM 文件。
- **占位符**：封面/尾页的班级、成员、指导教师、日期用 `＿＿＿＿` 占位，交付时提醒替换。


