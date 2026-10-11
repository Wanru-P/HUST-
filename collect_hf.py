#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
collect_hf.py — 拉取 Hugging Face Hub 模型元数据，落盘为 Bronze JSONL。

相比原始骨架的修正：
  1. 分页用官方 `Link` 响应头里的 `rel="next"` 游标（HF 没有 `x-next-cursor` 头，
     原始骨架的读法拿不到下一页，会陷入死循环/提前结束）。requests 会自动把
     Link 头解析进 `resp.links`。
  2. 控速：默认 1.0s/请求（≈60 请求/分钟）。免费 token 配额是 1000 请求/5 分钟，
     匿名 500/5 分钟。原始骨架的 0.2s（≈300/分钟）会直接打爆限流被 429 拦下。
  3. 429 自动退避：读 `Retry-After`；没有就按 2**attempt + jitter 指数退避，最多重试。
  4. 用 `cardData=true`（而不是 `full=true`）拿 `base_model` 字段——轻量很多
     （不含文件清单），后续家族图边表就靠它。
  5. 每条记录加 `_scraped_at`，保留采集时间便于复现。

运行：
    python collect_hf.py --token hf_xxx --max-models 50000
    # 不传 --token 也能跑（匿名 500/5min），但建议用免费 token 把配额翻倍。
    # token 也可走环境变量：export HF_TOKEN=hf_xxx
    # 国内 huggingface.co 慢/不通时走镜像（同样识别 HF_ENDPOINT）：
    #   export HF_ENDPOINT=https://hf-mirror.com
    #   python collect_hf.py --max-models 50000
"""
import argparse
import json
import os
import re
import sys
import time
import random
from urllib.parse import urlparse, urlunparse

import requests

# Hub API 基地址。国内若 huggingface.co 慢/不通，可用镜像 hf-mirror.com：
#   export HF_ENDPOINT=https://hf-mirror.com
# （HF_ENDPOINT 是 huggingface_hub 的通用约定，本脚本同样识别。）
API_BASE_DEFAULT = "https://huggingface.co"


def parse_args():
    p = argparse.ArgumentParser(description="Collect Hugging Face Hub model metadata -> Bronze JSONL")
    p.add_argument("--out", default=None,
                   help="输出 JSONL 路径（默认 <项目>/data/bronze/models.jsonl）")
    p.add_argument("--token", default=os.environ.get("HF_TOKEN"),
                   help="HF 只读 token（把配额 500->1000/5min）。默认读环境变量 HF_TOKEN。")
    p.add_argument("--api-base", default=os.environ.get("HF_ENDPOINT", API_BASE_DEFAULT),
                   help="Hub API 基地址。镜像：HF_ENDPOINT=https://hf-mirror.com")
    p.add_argument("--limit", type=int, default=100,
                   help="每页条数。HF API 上限约 100，超过会被静默截断。")
    p.add_argument("--max-models", type=int, default=None,
                   help="最多采集 N 条后停止（None=全部）。课程建议先 5w~20w。")
    p.add_argument("--sort", default=None,
                   help="排序字段：createdAt / downloads / likes / lastModified / trendingScore")
    p.add_argument("--direction", type=int, default=None, help="-1 降序，1 升序")
    p.add_argument("--filter", default=None, help="tag 过滤，如 text-generation")
    p.add_argument("--pipeline-tag", default=None)
    p.add_argument("--author", default=None, help="限定某个作者/组织，如 meta-llama")
    p.add_argument("--search", default=None, help="id 子串搜索")
    p.add_argument("--full", action="store_true",
                   help="full=true（重：含文件清单）。一般没必要，cardData 已含 base_model。")
    p.add_argument("--min-interval", type=float, default=1.0,
                   help="请求间最小间隔秒数（安全值 ≤1 请求/秒）。有 token 可降到 0.5。")
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--max-retries", type=int, default=6)
    p.add_argument("--proxy", default="http://127.0.0.1:7897",
                   help="HTTP/HTTPS 代理。默认本机 Clash 7897；设为 '' 则直连。"
                        "注意：本环境 HTTPS_PROXY 被预设为失效的 53907，必须用本参数显式覆盖。")
    return p.parse_args()


def get_next_url(resp, base_api):
    """从响应里取下一页 URL。优先 requests 解析好的 Link 头，逐级兜底。

    关键修正：镜像（如 hf-mirror.com）返回的 Link 头里 next 经常是官方
    huggingface.co 的绝对地址；若当前网络连不上官方域名，分页会在第 2 页断掉。
    因此这里把 next URL 的 host/scheme 强制改写成当前 api_base 的，确保分页
    始终落在同一个 host 上。
    """
    nxt = resp.links.get("next")          # requests 已把 Link 头解析成 dict
    u = nxt["url"] if (nxt and nxt.get("url")) else None
    if not u:
        link = resp.headers.get("Link")    # 手动兜底
        if link:
            m = re.search(r'<([^>]+)>;\s*rel="next"', link)
            if m:
                u = m.group(1)
    if not u:
        cur = resp.headers.get("x-next-cursor")  # 旧/非标准兜底（HF 实际不用）
        if cur:
            base = resp.url.split("?")[0]
            u = f"{base}?cursor={requests.utils.quote(cur)}"
    if not u:
        return None
    try:
        p = urlparse(u)
        b = urlparse(base_api)
        if p.netloc and b.netloc and p.netloc != b.netloc:
            u = urlunparse(p._replace(netloc=b.netloc, scheme=b.scheme))
    except Exception:
        pass
    return u


def fetch(url, headers, timeout, min_interval, max_retries, params=None):
    """带控速 + 429 退避的 GET。成功返回 200 响应对象；所有重试耗尽则抛 RuntimeError。"""
    last = 0.0
    r = None
    for attempt in range(max_retries + 1):
        elapsed = time.time() - last
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            if attempt < max_retries:
                wait = (2 ** attempt) + random.random()
                print(f"[warn] 网络错误 {e}；{wait:.1f}s 后重试", file=sys.stderr)
                time.sleep(wait); last = time.time(); continue
            raise RuntimeError(f"网络错误且无重试额度：{e}") from e

        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = float(ra) if (ra and ra.isdigit()) else (2 ** attempt + random.random())
            print(f"[429] 被限流；休眠 {wait:.1f}s（剩余配额 {r.headers.get('Ratelimit')}）",
                  file=sys.stderr)
            time.sleep(wait); last = time.time(); continue

        if r.status_code != 200:
            print(f"[err] HTTP {r.status_code}: {r.text[:200]}", file=sys.stderr)
            if attempt < max_retries:
                wait = (2 ** attempt) + random.random()
                time.sleep(wait); last = time.time(); continue
            r.raise_for_status()

    last = time.time()
    return r


def fetch_json(url, headers, timeout, min_interval, max_retries, params=None,
              json_decode_retries=3):
    """fetch + 解析 JSON。网络正常但响应体被截断/损坏（镜像代理偶发）时，
    自动重取该页；若耗尽仍解析失败，返回 (r, None) 让上层决定跳过该页继续，
    而不是让整次采集 abort。r 的响应头（含 Link 下一页）仍可用。"""
    last_err = None
    for _ in range(json_decode_retries + 1):
        try:
            r = fetch(url, headers, timeout, min_interval, max_retries, params=params)
            return r, r.json()
        except (json.JSONDecodeError, ValueError) as e:
            last_err = e
            wait = 1.0 + random.random()
            print(f"[warn] 响应 JSON 解析失败（{e}）；{wait:.1f}s 后重取该页",
                  file=sys.stderr)
            time.sleep(wait)
    print(f"[warn] 该页（{url}）JSON 重试耗尽，将跳过本页继续；末次错误：{last_err}",
          file=sys.stderr)
    return None, None


def main():
    args = parse_args()
    api_base = (args.api_base or API_BASE_DEFAULT).rstrip("/")
    models_url = api_base + "/api/models"
    if args.limit > 100:
        print("[warn] HF API 每页上限约 100，已自动降到 100。", file=sys.stderr)
        args.limit = 100

    project_root = os.path.dirname(os.path.abspath(__file__))
    out = args.out or os.path.join(project_root, "data", "bronze", "models.jsonl")
    os.makedirs(os.path.dirname(out), exist_ok=True)

    headers = {"Authorization": f"Bearer {args.token}"} if args.token else {}
    headers["User-Agent"] = "hf_eco_analysis/1.0 (+course project)"

    # 本环境 HTTPS_PROXY 被预设为失效的 53907，requests 会优先用 env 代理而连不上；
    # 因此按 --proxy 显式覆盖进程内 env，确保走通 Clash 7897（设为 '' 则清掉代理直连）。
    if args.proxy:
        os.environ["HTTP_PROXY"] = args.proxy
        os.environ["HTTPS_PROXY"] = args.proxy
    else:
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("HTTPS_PROXY", None)
    os.environ.pop("NO_PROXY", None)
    os.environ.pop("no_proxy", None)

    params = {"limit": args.limit, "cardData": "true"}
    if args.sort:        params["sort"] = args.sort
    if args.direction is not None: params["direction"] = args.direction
    if args.filter:      params["filter"] = args.filter
    if args.pipeline_tag:params["pipeline_tag"] = args.pipeline_tag
    if args.author:      params["author"] = args.author
    if args.search:      params["search"] = args.search
    if args.full:        params["full"] = "true"

    url = models_url
    count = 0          # 唯一模型数（已去重）
    seen = set()       # 已写入的 model id，防镜像重复页导致的数据膨胀
    stale_streak = 0   # 连续 0 新增的页数，用于检测镜像卡在重复页
    t0 = time.time()
    out_tmp = out + ".tmp"   # 先写临时文件，成功后再原子替换，避免中途失败清空目标
    with open(out_tmp, "w", encoding="utf-8") as f:
        first = True
        while True:
            r, batch = fetch_json(url, headers, args.timeout, args.min_interval,
                                  args.max_retries, params=(params if first else None))
            first = False
            if batch is None:
                # 该页响应体损坏且重试耗尽：跳过它，用响应头的 Link 继续往后翻
                nxt = get_next_url(r, api_base) if r is not None else None
                if not nxt or nxt == url:
                    # 镜像对坏页可能返回指向自身的游标（卡死）-> 无法继续翻页，优雅停止
                    print("[warn] 坏页之后无法继续翻页（无下一页链接或游标卡死），停止。",
                          file=sys.stderr)
                    break
                url = nxt
                continue
            if not isinstance(batch, list) or len(batch) == 0:
                break
            scraped_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            new_this_page = 0
            for m in batch:
                mid = m.get("id")
                if mid in seen:
                    continue          # 镜像偶尔回吐重复页，丢弃重复 id
                seen.add(mid)
                m["_scraped_at"] = scraped_at
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
                count += 1
                new_this_page += 1
            f.flush()
            print(f"本页 {len(batch)} 条（新增 {new_this_page}），累计唯一 {count}，"
                  f"耗时 {time.time() - t0:.1f}s")

            # 断流保护：连续多页 0 新增 = 镜像卡在重复页，避免无限打转
            if new_this_page == 0:
                stale_streak += 1
                if stale_streak >= 3:
                    print("[warn] 连续 3 页无新增（镜像疑似卡在重复页），停止。",
                          file=sys.stderr)
                    break
            else:
                stale_streak = 0

            if args.max_models and count >= args.max_models:
                print(f"已达到 --max-models {args.max_models}，停止。")
                break
            nxt = get_next_url(r, api_base)
            if not nxt or nxt == url:
                break

    os.replace(out_tmp, out)   # 整轮成功才落地，失败则保留旧文件/tmp
    print(f"完成：写入 {count} 条唯一模型到 {out}")


if __name__ == "__main__":
    main()
