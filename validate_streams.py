#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全量检测 IPTV (iptv-org) 与 Radio Browser 源。
输出：按国家/分类拆分的 M3U + manifest.json 索引 + report.md 报告。
"""

import asyncio
import aiohttp
import re
import json
import os
import time
from datetime import datetime, timezone
from collections import defaultdict

# ---------------- 配置 ----------------
TV_SOURCE = "https://iptv-org.github.io/iptv/index.m3u"
RADIO_API_BASE = "https://de1.api.radio-browser.info/json/stations/search"
RADIO_PAGE_SIZE = 1000          # 分页大小
RADIO_MAX_TOTAL = 60000         # 安全上限，防止无限循环
CONCURRENCY = 80                # 并发数
TIMEOUT_SEC = 12                # 单请求超时
OUTPUT_DIR = "output"
REPORT_FILE = "report.md"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "VLC/3.0.20 LibVLC/3.0.20",
    "Lavf/58.76.100",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

TIMEOUT = aiohttp.ClientTimeout(total=TIMEOUT_SEC, connect=5)

# ---------------- 工具函数 ----------------
def sanitize_key(s: str) -> str:
    """把分组名转成安全的文件名 key"""
    s = (s or "unknown").strip().lower()
    s = re.sub(r'[^a-z0-9\u4e00-\u9fa5]+', '_', s)
    return s or "unknown"


def parse_m3u(text: str):
    """解析 M3U，返回 [{name, group, url, tvg_country}]"""
    entries = []
    cur = {}
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF:"):
            m_name = re.search(r',(.+)$', line)
            name = m_name.group(1).strip() if m_name else "Unknown"
            m_group = re.search(r'group-title="([^"]*)"', line)
            group = m_group.group(1) if m_group else "General"
            m_country = re.search(r'tvg-country="([^"]*)"', line)
            country = m_country.group(1) if m_country else ""
            cur = {"name": name, "group": group, "country": country}
        elif line.startswith("http") and cur:
            cur["url"] = line
            entries.append(cur)
            cur = {}
    return entries


# ---------------- CORS 与 HLS 检测 ----------------
async def check_cors(session, url, headers):
    try:
        async with session.options(url, headers=headers, timeout=TIMEOUT) as resp:
            cors = resp.headers.get("Access-Control-Allow-Origin")
            if cors:
                return True
    except Exception:
        pass
    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
            cors = resp.headers.get("Access-Control-Allow-Origin")
            return bool(cors)
    except Exception:
        return False


async def check_hls(session, m3u8_url, headers):
    """拉取 m3u8 并取第一个分片验证"""
    try:
        async with session.get(m3u8_url, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                return False, f"Playlist HTTP {resp.status}"
            text = await resp.text()
            segs = re.findall(r'^(?!#)(https?://\S+)$', text, re.MULTILINE)
            if not segs:
                return False, "No segments"
            seg_url = segs[0]
            async with session.get(seg_url, headers=headers, timeout=TIMEOUT) as sresp:
                if sresp.status == 200:
                    return True, "OK"
                return False, f"Segment HTTP {sresp.status}"
    except asyncio.TimeoutError:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)[:40]


async def validate_tv_entry(session, entry):
    result = {
        "name": entry["name"], "url": entry["url"], "group": entry["group"],
        "country": entry.get("country", ""), "valid": False, "reason": "", "ua": None, "cors": False,
    }
    for ua in USER_AGENTS:
        headers = {"User-Agent": ua}
        ok, reason = await check_hls(session, entry["url"], headers)
        if ok:
            cors_ok = await check_cors(session, entry["url"], headers)
            result.update({"valid": cors_ok, "reason": "OK" if cors_ok else "CORS blocked", "ua": ua, "cors": cors_ok})
            if cors_ok:
                return result
        else:
            result["reason"] = reason
    return result


# ---------------- Radio 检测 ----------------
async def fetch_all_radio(session):
    """分页拉取全部电台"""
    all_st = []
    offset = 0
    while offset < RADIO_MAX_TOTAL:
        url = f"{RADIO_API_BASE}?hidebroken=true&limit={RADIO_PAGE_SIZE}&offset={offset}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    print(f"Radio API HTTP {resp.status} at offset {offset}")
                    break
                batch = await resp.json()
        except Exception as e:
            print(f"Radio API error at offset {offset}: {e}")
            break
        if not batch:
            break
        all_st.extend(batch)
        print(f"  fetched {len(all_st)} radio stations...")
        if len(batch) < RADIO_PAGE_SIZE:
            break
        offset += RADIO_PAGE_SIZE
        await asyncio.sleep(0.3)
    return all_st


async def validate_radio_station(session, st):
    url = st.get("url_resolved") or st.get("url")
    if not url or not url.startswith("http"):
        return None
    result = {
        "name": st.get("name", "Unknown"), "url": url,
        "country": st.get("countrycode", "") or st.get("country", ""),
        "valid": False, "reason": "", "ua": None,
    }
    for ua in USER_AGENTS:
        headers = {"User-Agent": ua}
        try:
            async with session.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=True) as resp:
                if resp.status == 200:
                    ct = resp.headers.get("Content-Type", "").lower()
                    if any(k in ct for k in ("audio", "mpeg", "aac", "ogg", "opus", "mp3")):
                        result.update({"valid": True, "reason": "OK", "ua": ua})
                        return result
                    result["reason"] = f"CT:{ct[:20]}"
                else:
                    result["reason"] = f"HTTP {resp.status}"
        except asyncio.TimeoutError:
            result["reason"] = "Timeout"
        except Exception as e:
            result["reason"] = str(e)[:30]
    return result


# ---------------- 输出 ----------------
def write_m3u(path, items):
    with open(path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for it in items:
            f.write(f'#EXTINF:-1,{it["name"]}\n{it["url"]}\n')


def build_manifest(kind, groups, cdn_base):
    """groups: {group_key: {"display": str, "items": [...]}}"""
    files = []
    total = 0
    for key, g in sorted(groups.items(), key=lambda x: -len(x[1]["items"])):
        cnt = len(g["items"])
        if cnt == 0:
            continue
        total += cnt
        files.append({
            "name": g["display"],
            "count": cnt,
            "file": f"{kind}_{key}.m3u",
            "url": f"{cdn_base}/{kind}_{key}.m3u",
        })
    return {"total": total, "files": files}


# ---------------- 主流程 ----------------
async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    t0 = time.time()
    print(f"[{datetime.now(timezone.utc)}] Start validation")

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": "iptv-radio-checker/1.0"},
    ) as session:

        # ---- 1. TV ----
        print("Fetching TV playlist...")
        async with session.get(TV_SOURCE, timeout=aiohttp.ClientTimeout(total=120)) as resp:
            tv_text = await resp.text()
        tv_entries = parse_m3u(tv_text)
        print(f"TV entries: {len(tv_entries)}")

        sem = asyncio.Semaphore(CONCURRENCY)
        async def tv_task(e):
            async with sem:
                return await validate_tv_entry(session, e)

        tv_results = []
        batch_size = 2000
        for i in range(0, len(tv_entries), batch_size):
            chunk = tv_entries[i:i + batch_size]
            tv_results.extend(await asyncio.gather(*[tv_task(e) for e in chunk]))
            print(f"  TV validated {len(tv_results)}/{len(tv_entries)}")

        valid_tv = [r for r in tv_results if r["valid"]]

        # ---- 2. Radio ----
        print("Fetching radio stations (full)...")
        radio_stations = await fetch_all_radio(session)
        print(f"Radio stations: {len(radio_stations)}")

        async def radio_task(s):
            async with sem:
                return await validate_radio_station(session, s)

        radio_results = []
        for i in range(0, len(radio_stations), batch_size):
            chunk = radio_stations[i:i + batch_size]
            radio_results.extend(await asyncio.gather(*[radio_task(s) for s in chunk]))
            print(f"  Radio validated {len(radio_results)}/{len(radio_stations)}")

        valid_radio = [r for r in radio_results if r and r["valid"]]

    # ---- 3. 按国家/分类拆分 ----
    tv_groups = defaultdict(lambda: {"display": "", "items": []})
    for r in valid_tv:
        key = sanitize_key(r["country"] or r["group"] or "general")
        tv_groups[key]["display"] = r["country"] or r["group"] or "General"
        tv_groups[key]["items"].append(r)

    radio_groups = defaultdict(lambda: {"display": "", "items": []})
    for r in valid_radio:
        key = sanitize_key(r["country"] or "general")
        radio_groups[key]["display"] = r["country"] or "General"
        radio_groups[key]["items"].append(r)

    # 写拆分文件
    for key, g in tv_groups.items():
        write_m3u(os.path.join(OUTPUT_DIR, f"tv_{key}.m3u"), g["items"])
    for key, g in radio_groups.items():
        write_m3u(os.path.join(OUTPUT_DIR, f"radio_{key}.m3u"), g["items"])

    # ---- 4. manifest ----
    cdn_base = "https://cdn.jsdelivr.net/gh/REPLACE_USER/REPLACE_REPO@main/output"
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tv": build_manifest("tv", tv_groups, cdn_base),
        "radio": build_manifest("radio", radio_groups, cdn_base),
    }
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    # ---- 5. 报告 ----
    elapsed = time.time() - t0
    tv_total = len(tv_entries)
    radio_total = len(radio_stations)
    tv_cors_blocked = len([r for r in tv_results if not r["valid"] and "CORS" in r["reason"]])

    lines = [
        "# 流媒体源检测报告",
        f"\n生成时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"耗时: {elapsed/60:.1f} 分钟",
        "",
        "## 摘要",
        f"- 电视源总数: **{tv_total}**",
        f"- 电视有效源 (含 CORS): **{len(valid_tv)}**",
        f"- 电视因 CORS 被拦截: {tv_cors_blocked}",
        f"- 电台总数: **{radio_total}**",
        f"- 电台有效源: **{len(valid_radio)}**",
        "",
        "## 电视按分类统计 (Top 20)",
        "| 分类 | 数量 | 文件 |",
        "| :--- | ---: | :--- |",
    ]
    for f in manifest["tv"]["files"][:20]:
        lines.append(f"| {f['name']} | {f['count']} | `{f['file']}` |")
    lines += ["", "## 电台按分类统计 (Top 20)", "| 分类 | 数量 | 文件 |", "| :--- | ---: | :--- |"]
    for f in manifest["radio"]["files"][:20]:
        lines.append(f"| {f['name']} | {f['count']} | `{f['file']}` |")

    lines += ["", "## 电视源详细结果 (前 50 条)", "| 频道 | 状态 | 原因 | UA |", "| :--- | :--- | :--- | :--- |"]
    for r in tv_results[:50]:
        st = "✅" if r["valid"] else "❌"
        ua = (r["ua"][:18] + "…") if r["ua"] else "-"
        lines.append(f"| {r['name'][:20]} | {st} | {r['reason'][:28]} | {ua} |")

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Done in {elapsed/60:.1f} min. TV valid: {len(valid_tv)}, Radio valid: {len(valid_radio)}")


if __name__ == "__main__":
    asyncio.run(main())