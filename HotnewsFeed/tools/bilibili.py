# -*- coding: utf-8 -*-
"""哔哩哔哩公开空间视频采集适配器。"""

import re
from datetime import datetime, timezone
from typing import List, Optional

from config import Config
from task_pipelines.schemas import AccountPost

conf = Config()


def extract_space_uid(value: str) -> str:
    """从 UID 或 space.bilibili.com 空间地址中取 UID。"""
    value = value.strip()
    if value.isdigit():
        return value
    match = re.search(r"space\.bilibili\.com/(\d+)", value)
    if not match:
        raise ValueError(f"无法从 B 站空间地址解析 UID: {value}")
    return match.group(1)


def _ydl_options(limit: int) -> dict:
    cfg = conf.account_monitor
    options = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "playlistend": max(1, limit),
        "skip_download": True,
        "http_headers": {"Referer": "https://www.bilibili.com/"},
        "proxy": "",  # 不读取 Windows 系统代理，避免 localhost/MCP 同类代理污染
        "socket_timeout": 20,
        "retries": 2,
    }
    if cfg["bilibili_cookie"]:
        options["http_headers"]["Cookie"] = cfg["bilibili_cookie"]
    if cfg["cookie_file"]:
        from pathlib import Path
        if not Path(cfg["cookie_file"]).is_file():
            raise FileNotFoundError(f"B 站 cookie_file 不存在: {cfg['cookie_file']}")
        options["cookiefile"] = cfg["cookie_file"]
    return options


async def fetch_bilibili_space_videos(
    account: str,
    space_url: str,
    since: Optional[str] = None,
    limit: int = 20,
) -> List[AccountPost]:
    """读取 B 站 UP 主最近视频；风控时需在 config.ini 配置 Cookie。"""
    import asyncio
    import yt_dlp

    uid = extract_space_uid(space_url)
    url = f"https://space.bilibili.com/{uid}/video"

    def extract():
        with yt_dlp.YoutubeDL(_ydl_options(limit)) as ydl:
            return ydl.extract_info(url, download=False)

    try:
        info = await asyncio.to_thread(extract)
    except Exception as exc:
        raise RuntimeError(
            f"B 站空间 {uid} 读取失败: {exc}。"
            "如果是 352/412 风控，请在 [account_monitor] 配置 cookie_file。"
        ) from exc

    cutoff = None
    if since:
        try:
            cutoff = datetime.fromisoformat(since.replace("Z", "+00:00")).timestamp()
        except ValueError:
            cutoff = None
    posts: List[AccountPost] = []
    for entry in (info or {}).get("entries") or []:
        if not entry:
            continue
        timestamp = entry.get("timestamp") or entry.get("release_timestamp") or 0
        if cutoff and timestamp and float(timestamp) <= cutoff:
            continue
        bvid = str(entry.get("id") or entry.get("bvid") or "")
        video_url = entry.get("webpage_url") or entry.get("url") or f"https://www.bilibili.com/video/{bvid}"
        if video_url and not str(video_url).startswith("http"):
            video_url = f"https://www.bilibili.com/video/{bvid}"
        published = (
            datetime.fromtimestamp(float(timestamp), timezone.utc).isoformat(timespec="seconds")
            if timestamp else ""
        )
        posts.append(AccountPost(
            post_id=bvid, account=account, platform="bilibili",
            title=entry.get("title") or bvid,
            content=entry.get("description") or "",
            published_at=published, url=str(video_url),
        ))
    return posts[:limit]
