# -*- coding: utf-8 -*-
"""哔哩哔哩公开空间视频采集适配器。

该模块负责从 B 站 UP 主公开空间主页（https://space.bilibili.com/<uid>/video）拉取
最近发布的视频列表，并将其转换为流水线统一使用的 :class:`~task_pipelines.schemas.AccountPost`
数据结构，供账户监控（account_monitor）任务消费。

模块依赖:
- ``yt_dlp``        : B 站信息解析引擎。只做“提取元数据”（download=False），不实际下载文件；
                      也是后续 tools/video.py 提取字幕 / 音频转写的共用基础。
- ``Config``        : 全局配置单例。通过 ``account_monitor`` 属性拿到 [account_monitor] 段下的
                      bilibili_cookie（Cookie 字符串）与 cookie_file（Cookie 文件路径），
                      用于绕过 B 站 352/412 风控。
- ``AccountPost``   : task_pipelines/schemas.py 中定义的账户发布内容模型，本模块的输出类型。

典型调用链::

    collect.py  ->  fetch_bilibili_space_videos(account, space_url, since, limit)
                  ->  extract_space_uid(space_url)      # 解析 UID
                  ->  yt_dlp.YoutubeDL(...).extract_info()  # 拉取视频列表元数据
                  ->  AccountPost(...)                   # 包装成统一数据结构

对外只暴露两个接口：
- extract_space_uid            : 从 UID 或空间 URL 中解析出纯数字 UID（同步、可单测）。
- fetch_bilibili_space_videos  : 异步采集某 UP 主最近视频，返回 AccountPost 列表。
"""

import re
from datetime import datetime, timezone
from typing import List, Optional

from config import Config
from task_pipelines.schemas import AccountPost

# 全局配置对象（读取 config.ini）。本模块只用到其 account_monitor 属性。
conf = Config()


def extract_space_uid(value: str) -> str:
    """从 UID 或 space.bilibili.com 空间地址中取 UID。

    兼容两种输入形态，便于调用方随意传 URL 或纯 UID：
      1. 纯数字字符串，如 "312249633"            -> 原样返回
      2. 空间主页地址，如 "https://space.bilibili.com/312249633/video"
         -> 用正则捕获 URL 中的那串数字返回

    参数:
        value: UP 主的 UID 或 B 站空间主页地址。

    返回:
        纯数字 UID 字符串（去掉 URL 前后缀与空白）。

    抛出:
        ValueError: 既不是纯数字、也无法从 URL 中解析出 UID 时抛出，提示调用方修正输入。

    说明:
        str.strip()   -> 去掉输入首尾空白（来自 Web 表单或配置时常见）。
        str.isdigit() -> 判断是否已经是纯数字 UID；注意 it 对全角数字也返回 True，此处可接受。
        re.search(r"space\\.bilibili\\.com/(\\d+)", value)
                     -> 正则查找 "space.bilibili.com/" 之后紧跟的一串数字，
                        其中 "(\\d+)" 是捕获组 1（即 UID）。
    """
    # 先去掉首尾空白，避免 "\n312249633" 这类脏输入误判。
    value = value.strip()
    # 已经是纯数字 => 认为传入的就是 UID，直接返回。
    if value.isdigit():
        return value
    # 尝试从空间主页 URL 中提取数字 UID；匹配不到则说明输入不是合法的 B 站地址。
    match = re.search(r"space\.bilibili\.com/(\d+)", value)
    if not match:
        raise ValueError(f"无法从 B 站空间地址解析 UID: {value}")
    # match.group(1) 返回正则第一个捕获组，即 URL 中的那串纯数字 UID。
    return match.group(1)


def _ydl_options(limit: int) -> dict:
    """构造传给 yt-dlp 的选项字典。

    集中管理 B 站抓取所需的 yt-dlp 参数，供 fetch_bilibili_space_videos 复用，
    避免在调用处散落魔法参数。

    参数:
        limit: 期望拉取的最大视频条数，会映射到 yt-dlp 的 playlistend（列表截断数）。

    返回:
        yt-dlp 可识别的 options 字典（dict），可直接作为 ``yt_dlp.YoutubeDL(options)`` 的参数。

    说明:
        - 读取全局配置 ``conf.account_monitor``（一个 dict，见 config.py），
          其中 bilibili_cookie 是 Cookie 字符串、cookie_file 是 Netscape cookie 文件路径。
        - 两种登录态配置为“或”的关系：有字符串则放 http_headers 里，有文件则走 cookiefile，
          可同时配置，yt-dlp 会合并使用。
        - 显式设置 proxy="" 以屏蔽 Windows 系统代理（避免 localhost 被代理劫持的坑）。
    """
    # 取出 [account_monitor] 配置段字典（含 interval、cookie、asr 等键）。
    cfg = conf.account_monitor
    options = {
        "quiet": True,          # 不打印 yt-dlp 的日志输出，保持调用方输出干净。
        "no_warnings": True,    # 抑制警告信息，避免 B 站常见的“默认不支持”等噪音。
        "extract_flat": False,  # 全量提取各视频元数据（标题/发布时间/简介）；extract_flat=True 时 B 站只返回 BV 号，标题/时间全丢。
        "playlistend": max(1, limit),  # 限制最多解析前 limit 个视频（至少为 1，防传 0）。
        "skip_download": True,  # 只解析元数据、绝不下载文件（本模块只采集列表）。
        "http_headers": {"Referer": "https://www.bilibili.com/"},  # 带 Referer 降低被 B 站风控概率。
        "proxy": "",  # 不读取 Windows 系统代理，避免 localhost/MCP 同类代理污染
        "socket_timeout": 20,   # 网络超时（秒），防止请求挂死拖垮事件循环。
        "retries": 2,           # 网络/下载失败重试次数，增强稳定性。
    }
    # 若配置了 Cookie 字符串（bilibili_cookie），作为请求头附加，用于规避风控/获取高清信息。
    if cfg["bilibili_cookie"]:
        options["http_headers"]["Cookie"] = cfg["bilibili_cookie"]
    # 若配置了 cookie 文件（cookie_file），先校验文件真实存在再交给 yt-dlp 读取。
    if cfg["cookie_file"]:
        # 延迟 import：只有真正用到 cookie 文件时才引入 pathlib，避免顶层依赖。
        from pathlib import Path
        # Path.is_file() 判断文件是否存在且为普通文件；不存在则直接报错，比让 yt-dlp 静默失败更好排查。
        if not Path(cfg["cookie_file"]).is_file():
            raise FileNotFoundError(f"B 站 cookie_file 不存在: {cfg['cookie_file']}")
        # cookiefile 让 yt-dlp 从该文件加载登录态（Netscape 格式，可用浏览器扩展导出）。
        options["cookiefile"] = cfg["cookie_file"]
    return options


async def fetch_bilibili_space_videos(
    account: str,
    space_url: str,
    since: Optional[str] = None,
    limit: int = 20,
) -> List[AccountPost]:
    """读取 B 站 UP 主最近视频；风控时需在 config.ini 配置 Cookie。

    异步采集指定 UP 主公开空间主页最近发布的视频，转成 AccountPost 列表返回。
    若命中 352/412 风控，会抛出带提示信息的 RuntimeError（提示在 config.ini 配 Cookie）。

    参数:
        account:    账户标识（写入 AccountPost.account，例如 "bilibili_312249633"）。
        space_url:  UP 主空间地址或 UID（内部通过 extract_space_uid 解析）。
        since:      可选 ISO 8601 时间字符串（支持 "Z" 后缀），仅返回发布时间晚于该时刻的视频；
                    传 None 或格式非法时不作时间过滤。
        limit:      最多返回的视频条数（默认 20）。

    返回:
        List[AccountPost]：每条视频一个元素，按 yt-dlp 返回顺序（一般即发布时间倒序）。

    抛出:
        RuntimeError: 网络失败、风控或解析异常时抛出，附带可操作的排查提示。
    """
    import asyncio
    import yt_dlp

    # 1) 解析 UID：纯数字原样返回，URL 则提取其中的数字段。
    uid = extract_space_uid(space_url)
    # B 站 UP 主“视频”标签页 URL，yt-dlp 会把它当作播放列表展开。
    url = f"https://space.bilibili.com/{uid}/video"

    def extract():
        # yt_dlp.YoutubeDL 是 yt-dlp 的核心类，负责下载 / 解析。
        # 用上下文管理器打开（内部会做插件的初始化与清理），传入 _ydl_options 构造的参数。
        with yt_dlp.YoutubeDL(_ydl_options(limit)) as ydl:
            # extract_info(url, download=False) 只拉取元数据不下载；
            # 对空间地址会返回一个含 "entries" 键的播放列表结构。
            return ydl.extract_info(url, download=False)

    try:
        # 2) yt_dlp 是阻塞式调用，包一层 asyncio.to_thread 丢到线程池执行，
        #    避免阻塞事件循环（这正是本函数被声明为 async 的原因）。
        info = await asyncio.to_thread(extract)
    except Exception as exc:
        # 把底层异常统一包装为 RuntimeError，并带上“如何排查风控”的提示，
        # 避免把 yt-dlp 晦涩的原始 traceback 直接抛给上层调用者。
        raise RuntimeError(
            f"B 站空间 {uid} 读取失败: {exc}。"
            "如果是 352/412 风控，请在 [account_monitor] 配置 cookie_file。"
        ) from exc

    # 3) 解析 since 参数：ISO 8601 -> Unix 时间戳，作为时间过滤的边界（闭区间：<=cutoff 的丢弃）。
    cutoff = None
    if since:
        try:
            # datetime.fromisoformat 解析 ISO 时间；replace("Z", "+00:00") 把 "Z"(UTC) 换成明确时区，
            # 因为 fromisoformat 直到 Python 3.11 才原生支持 "Z"。
            cutoff = datetime.fromisoformat(since.replace("Z", "+00:00")).timestamp()
        except ValueError:
            # 传入的 since 格式非法时不设截止时间，退化为拉全部（宁可多返回也不因入参报错）。
            cutoff = None

    # 4) 遍历播放列表的每个条目，做字段映射并组装成 AccountPost。
    posts: List[AccountPost] = []
    # info.get("entries") 是各视频条目的列表；B 站元数据条目可能为 None，需防御。
    for entry in (info or {}).get("entries") or []:
        if not entry:
            continue
        # 取发布时间：优先 timestamp，回退到 release_timestamp，再没有就按 0（视为未知）。
        timestamp = entry.get("timestamp") or entry.get("release_timestamp") or 0
        # 时间过滤：若设置了 since 且本条时间早于/等于 cutoff，则跳过（不发旧视频）。
        if cutoff and timestamp and float(timestamp) <= cutoff:
            continue
        # 视频 ID：yt-dlp 在 id / bvid 两个键上可能任选其一，都取不到则退化为空串。
        bvid = str(entry.get("id") or entry.get("bvid") or "")
        # 视频地址：优先取完整 URL；yt-dlp 对单条可能只给相对路径，需要手动拼回完整链接。
        video_url = entry.get("webpage_url") or entry.get("url") or f"https://www.bilibili.com/video/{bvid}"
        # 兜底：拿到的是相对路径（不以 http 开头）时，用 BV 号拼出标准视频页 URL。
        if video_url and not str(video_url).startswith("http"):
            video_url = f"https://www.bilibili.com/video/{bvid}"
        # 发布时间格式化：Unix 时间戳 -> UTC ISO 8601 字符串（精确到秒），无时间则留空。
        published = (
            datetime.fromtimestamp(float(timestamp), timezone.utc).isoformat(timespec="seconds")
            if timestamp else ""
        )
        # 组装 AccountPost（dataclass，构造时按关键字传参）：
        # post_id=BV 号、account=账户标识、platform 固定 "bilibili"、
        # title/content 取不到时分别回退到 bvid 和空串、published_at/url 为上面的结果。
        posts.append(AccountPost(
            post_id=bvid, account=account, platform="bilibili",
            title=entry.get("title") or bvid,
            content=entry.get("description") or "",
            published_at=published, url=str(video_url),
        ))
    # 最终再按 limit 截断一次（playlistend 已限制，此处是双保险）。
    return posts[:limit]
