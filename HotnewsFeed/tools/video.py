# -*- coding: utf-8 -*-
"""视频内容提取：字幕优先，无字幕时下载音频并用 DashScope ASR 转写。

该模块负责从单个视频地址提取“可读文本内容”：优先抓取视频字幕；没有字幕时下载音频并
调用阿里云 DashScope ASR（语音识别）转写成文本；最终交给 LLM 生成摘要与关键词。
当前实现只支持 B 站（bilibili），供账户监控（account_monitor）处理新发布视频时调用。

模块依赖:
- ``yt_dlp``                 : B 站信息解析引擎。这里有两种用途：
                               ① download=False 提取元数据（标题/作者/时间/字幕列表）；
                               ② download=True 下载最佳音频并交给 FFmpeg 转 WAV。
- ``imageio_ffmpeg``         : 提供 ffmpeg 可执行文件绝对路径，作为 yt-dlp 音频后处理的
                               ffmpeg_location（省去手动安装配置 ffmpeg）。
- ``dashscope``              : 阿里云百炼 SDK。Recognition 是语音识别（ASR）类，负责把
                               WAV 音频转成文本（默认模型 paraformer-realtime-v2）。
- ``ChatOpenAI``             : langchain-openai 的 LLM 客户端。base_url/api_key 指向兼容
                               OpenAI 协议的服务（本项目为 qwen-plus dashscope 兼容接口），
                               用于从转写文本生成摘要与关键词。
- ``Config``                 : 全局配置单例。通过 ``account_monitor`` 属性拿到 B 站 Cookie /
                               ASR 模型 / 临时目录等；通过 ``llm`` 属性拿到 LLM 配置。
- ``VideoContent``           : 本模块定义的 dataclass 输出模型（含转写、摘要、关键词）。

典型调用链::

    mcp_servers/mcp_access.py: extract_video_content(video_url, platform, prefer_subtitle)
      -> 本模块 extract_video_content(video_url, platform, prefer_subtitle)
        -> yt_dlp.YoutubeDL(...).extract_info(url, download=False)   # 1 提取元数据
        -> _pick_subtitle(info) -> _subtitle_text(track)             # 2 字幕优先
        -> _download_wav(video_url, dir) -> _transcribe(wav)         # 3 无字幕则 ASR 转写
        -> _summarize(title, transcript, description)                # 4 生成摘要与关键词
        -> VideoContent(...)                                          # 5 包装成统一数据结构

对外暴露的接口：
- VideoContent           : 视频内容数据模型（dataclass）。
- extract_video_content  : 异步提取单个视频内容，返回 VideoContent。
"""

import asyncio
import json
import re
import tempfile
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from langchain_openai import ChatOpenAI

from config import Config
from create_logger import logger

# 全局配置对象（读取 config.ini）。本模块用到其 llm 与 account_monitor 属性。
conf = Config()


@dataclass
class VideoContent:
    """视频内容数据模型：一次视频提取的完整输出。

    字段说明:
        video_id:      视频 ID（优先取 yt-dlp 元数据的 id / bvid）。
        title:         视频标题。
        author:        上传者 / 作者。
        published_at:  发布时间（UTC ISO 8601，精确到秒；未知为空串）。
        transcript:    字幕或 ASR 转写的全文（一句一行）。
        summary:       LLM 生成的摘要（200 字内）。
        keywords:      关键词列表（默认空列表）。
        source_url:    原始视频地址。
        content_source: 内容来源标记：subtitle（字幕）/ asr（语音转写）/ metadata（仅有简介）。
    """
    video_id: str                    # 视频 ID。
    title: str                       # 视频标题。
    author: str                      # 上传者 / 作者。
    published_at: str                # 发布时间（UTC ISO 8601）。
    transcript: str                  # 字幕或 ASR 转写全文。
    summary: str                     # LLM 摘要。
    keywords: List[str] = field(default_factory=list)  # 关键词列表，默认空。
    source_url: str = ""             # 原始视频地址。
    content_source: str = "metadata"  # subtitle / asr / metadata 内容来源标记。


def _ydl_options(download: bool = False, output: str = "") -> dict:
    """构造传给 yt-dlp 的选项字典（B 站参数集中管理）。

    参数:
        download: 是否实际下载媒体文件。False 只解析元数据（skip_download=True）；
                  True 则下载并交给 FFmpeg 后处理（_download_wav 使用）。
        output:   yt-dlp 的输出文件名模板（outtmpl），仅在 download=True 时传。

    返回:
        yt-dlp 可识别的 options 字典，可直接作为 ``yt_dlp.YoutubeDL(options)`` 的参数。

    抛出:
        FileNotFoundError: 配置了 cookie_file 但文件不存在时抛出。

    说明:
        - ``conf.account_monitor`` 是 [account_monitor] 配置段 dict（见 config.py），
          其中 bilibili_cookie 是 Cookie 字符串、cookie_file 是 Netscape cookie 文件路径。
        - 显式设置 proxy="" 屏蔽 Windows 系统代理（避免 localhost/MCP 同类代理污染）。
        - outtmpl 里的 %(ext)s 是 yt-dlp 占位符，由实际媒体扩展名自动填充。
    """
    # 读取 [account_monitor] 配置段（含 B 站 Cookie、ASR 模型、临时目录等键）。
    cfg = conf.account_monitor
    # 带 Referer 头降低被 B 站风控（352/412）的概率。
    headers = {"Referer": "https://www.bilibili.com/"}
    # 配置了 Cookie 字符串则附加到请求头，用于规避风控 / 获取高清信息。
    if cfg["bilibili_cookie"]:
        headers["Cookie"] = cfg["bilibili_cookie"]
    # 汇总成 yt-dlp 可识别的选项字典。
    options = {
        "quiet": True,               # 不打印 yt-dlp 日志，保持调用方输出干净。
        "no_warnings": True,         # 抑制警告，避免 B 站“默认不支持”等噪音。
        "http_headers": headers,     # 请求头（Referer/Cookie）随每次请求携带。
        "skip_download": not download,  # download=False 时只解析元数据，不实际下载。
        "proxy": "",                 # 不读 Windows 系统代理，避免 localhost/MCP 代理污染。
        "socket_timeout": 20,        # 网络超时（秒），防止请求挂死。
        "retries": 2,                # 网络/下载失败重试次数，增强稳定性。
    }
    # 配置了 cookie 文件时：先校验存在，再交给 yt-dlp 加载登录态。
    if cfg["cookie_file"]:
        # Path.is_file() 判断文件存在且为普通文件；不存在直接报错，便于排查。
        if not Path(cfg["cookie_file"]).is_file():
            raise FileNotFoundError(f"B 站 cookie_file 不存在: {cfg['cookie_file']}")
        # cookiefile 让 yt-dlp 从该文件读取登录态（Netscape 格式，浏览器扩展可导出）。
        options["cookiefile"] = cfg["cookie_file"]
    # 指定了输出模板（下载音频场景）则写入 outtmpl。
    if output:
        options["outtmpl"] = output
    return options


def _pick_subtitle(info: dict) -> dict:
    """从 yt-dlp 元数据中挑选最优字幕轨道。

    参数:
        info: yt-dlp 的 extract_info 返回值（dict），含 subtitles / automatic_captions 键。

    返回:
        一个字幕轨道的 dict（含 url 字段）；没有任何可用轨道时返回空 dict {}。

    说明:
        - ``info.get("subtitles")``          : 人工上传字幕（dict：语言 → 轨道列表）。
        - ``info.get("automatic_captions")`` : 自动生成字幕（B 站 AI 字幕通常在这里）。
        - 语言优先级 zh-CN → zh-Hans → zh → ai-zh → danmaku，匹配到即返回该语言的
          最后一个轨道（candidates[-1]，通常清晰度/内容更完整）。
        - 指定语言都没命中时，兜底遍历两个字幕字典里任意语言的第一组候选。
    """
    # 人工上传字幕字典：语言 -> 轨道列表；没有则空 dict。
    tracks = info.get("subtitles") or {}
    # 自动生成字幕字典（B 站 AI 字幕通常在这里）；没有则空 dict。
    automatic = info.get("automatic_captions") or {}
    # 依次尝试常见中文字幕语言。
    for language in ("zh-CN", "zh-Hans", "zh", "ai-zh", "danmaku"):
        # 人工字幕或自动字幕任一命中即可；取该语言轨道列表。
        candidates = tracks.get(language) or automatic.get(language) or []
        if candidates:
            # 取最后一个轨道（通常清晰度/内容更完整）。
            return candidates[-1]
    # 语言白名单都没命中：退而求其次，遍历两个字幕字典返回任意一种语言。
    for source in (tracks, automatic):
        for candidates in source.values():
            if candidates:
                return candidates[-1]
    # 完全没有任何字幕轨道，返回空 dict 由调用方降级处理。
    return {}


def _subtitle_text(track: dict) -> str:
    """下载并解析一个字幕轨道的文本内容。

    参数:
        track: _pick_subtitle 返回的字幕轨道 dict，需含 url 字段。

    返回:
        字幕全文（每行一句，用换行连接）；抓取或解析失败返回空串 ""。

    说明:
        - 兼容三种字幕格式：
          ① JSON 字幕（dict 含 body 列表，每项 content 字段）—— B 站常见；
          ② JSON 字幕（dict 含 events 列表，逐段 segs[].utf8）—— YouTube 风格；
          ③ 纯文本字幕（WEBVTT / SRT / 逐行文本）—— 逐行清洗后拼接。
        - url 以 "//" 开头时补上 "https:" 协议前缀（yt-dlp 常返回协议相对地址）。
        - ``urllib.request.urlopen`` 是标准库同步 HTTP 请求，带 UA 与 20 秒超时。
        - 清洗纯文本时跳过空行、时间轴行（"-->"）、纯数字行、WEBVTT/NOTE 头，
          并用正则 ``<[^>]+>`` 剥掉富文本标签。
    """
    url = track.get("url") or ""
    # 协议相对地址（//...）补全成 https，否则 urllib 无法直接请求。
    if url.startswith("//"):
        url = "https:" + url
    # 没有地址则无内容可抓，返回空串。
    if not url:
        return ""
    # 标准库 HTTP 请求：带浏览器 UA 降低被拒概率。
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    # 20 秒超时；errors="replace" 防止非法字节解码抛错。
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read().decode("utf-8", errors="replace")
    try:
        # 优先按 JSON 解析（B 站 / YouTube 风格字幕）。
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("body"), list):
            # B 站风格：body 数组，每项的 content 字段即一句字幕。
            return "\n".join(str(row.get("content", "")) for row in data["body"] if row.get("content"))
        # YouTube 风格：events 数组，逐段取 segs[].utf8 拼接。
        events = data.get("events", []) if isinstance(data, dict) else []
        texts = []
        for event in events:
            for segment in event.get("segs") or []:
                if segment.get("utf8"):
                    texts.append(segment["utf8"])
        if texts:
            return "\n".join(texts)
    except json.JSONDecodeError:
        # 不是 JSON：落到下方按 WEBVTT/SRT 纯文本清洗。
        pass
    # 非 JSON：当作 WEBVTT/SRT 纯文本，逐行过滤噪音后拼接。
    lines = []
    for line in raw.splitlines():
        text = line.strip()
        # 跳过空行、时间轴行（-->）、纯数字行、WEBVTT/NOTE 头。
        if not text or "-->" in text or text.isdigit() or text.startswith(("WEBVTT", "NOTE")):
            continue
        # 正则剥掉富文本标签，只留纯文本。
        lines.append(re.sub(r"<[^>]+>", "", text))
    return "\n".join(lines)


def _download_wav(video_url: str, directory: str) -> str:
    """下载视频最佳音频并用 FFmpeg 转成 16kHz 单声道 WAV。

    参数:
        video_url: 视频页面地址（B 站）。
        directory: 临时目录路径，音频文件与 WAV 都会写在这里（临时目录上下文内）。

    返回:
        转换后 WAV 文件的绝对路径字符串（目录下第一个 *.wav）。

    抛出:
        RuntimeError: yt-dlp 下载/后处理完成后目录里没找到 WAV 文件时抛出。

    说明:
        - 延迟 import：imageio_ffmpeg / yt_dlp 体积大，仅真正需要下载音频时才引入。
        - ``imageio_ffmpeg.get_ffmpeg_exe()`` 返回 imageio-ffmpeg 自带的 ffmpeg 可执行
          文件路径，省去用户手动安装 ffmpeg 并配置 PATH。
        - ``FFmpegExtractAudio`` 是 yt-dlp 内建后处理器，把已下载媒体抽出音轨；
          preferredcodec="wav" 指定输出 WAV；postprocessor_args 追加 ``-ar 16000 -ac 1``
          （16kHz 采样率、单声道），这是 DashScope ASR 的标准输入要求。
        - yt-dlp 的输出模板 output 用 %(ext)s 占位，实际扩展名由下载格式决定。
    """
    # 延迟 import：仅真正需要下载音频时才引入重依赖。
    import imageio_ffmpeg
    import yt_dlp

    # 输出模板：audio.<ext>，实际扩展名由下载格式决定。
    output = str(Path(directory) / "audio.%(ext)s")
    # 复用 _ydl_options 基础选项，开启下载并指定输出模板。
    options = _ydl_options(download=True, output=output)
    options.update({
        "format": "bestaudio/best",  # 优先最佳纯音频，没有则取最佳画质再抽音轨。
        "ffmpeg_location": imageio_ffmpeg.get_ffmpeg_exe(),  # 用 imageio 自带 ffmpeg 可执行文件。
        "postprocessors": [{  # yt-dlp 内建后处理：下载后自动抽音轨。
            "key": "FFmpegExtractAudio", "preferredcodec": "wav",  # 抽出音频并转成 WAV。
        }],
        "postprocessor_args": ["-ar", "16000", "-ac", "1"],  # 16kHz 单声道，DashScope ASR 标准输入。
    })
    with yt_dlp.YoutubeDL(options) as ydl:
        # 下载视频并触发 FFmpeg 后处理转 WAV。
        ydl.download([video_url])
    # 后处理完成后再扫描目录，确认确实产出了 WAV 文件。
    files = list(Path(directory).glob("*.wav"))
    if not files:
        raise RuntimeError("视频音频转换后未找到 WAV 文件")
    return str(files[0])


def _transcribe(wav_path: str) -> str:
    """调用 DashScope ASR 把 WAV 音频转写成文本。

    参数:
        wav_path: 待识别的 WAV 文件绝对路径。

    返回:
        识别出的文本（每句一行，用换行连接）；无识别结果返回空串 ""。

    抛出:
        RuntimeError: DashScope 返回非 200 状态码时抛出，附带服务端错误信息。

    说明:
        - 延迟 import：仅真正需要转写时才引入 dashscope，保持模块顶层轻量。
        - ``conf.llm["api_key"]`` 复用 LLM 的 api_key（阿里云百炼 DashScope 的 API Key）。
        - ``Recognition`` 是 dashscope.audio.asr 的语音识别类：model 取配置的 asr_model
          （默认 paraformer-realtime-v2），format/sample_rate 必须与音频参数一致
          （wav、16000），callback=None 表示同步调用。
        - ``recognition.call(wav_path)`` 同步执行识别；``get_sentence()`` 返回句子列表，
          每句可能是 dict（含 text 或 sentence 字段）。
    """
    # 延迟 import：仅真正需要转写时才引入 dashscope。
    import dashscope
    from dashscope.audio.asr import Recognition

    # 复用 LLM 的 api_key（阿里云百炼 DashScope 的 API Key）。
    dashscope.api_key = conf.llm["api_key"]
    recognition = Recognition(
        model=conf.account_monitor["asr_model"],  # 配置的 ASR 模型（默认 paraformer-realtime-v2）。
        callback=None,                            # None = 同步调用，直接等结果返回。
        format="wav", sample_rate=16000,          # 与 _download_wav 产出的音频参数一致。
    )
    result = recognition.call(wav_path)
    # DashScope 成功时 status_code 为 200，否则按失败处理并透出服务端消息。
    if getattr(result, "status_code", 500) != 200:
        raise RuntimeError(f"DashScope ASR 失败: {getattr(result, 'message', result)}")
    sentences = result.get_sentence() or []
    # 单句时可能直接返回 dict，归一化成列表统一处理。
    if isinstance(sentences, dict):
        sentences = [sentences]
    # 逐句取 text/sentence 字段，去空白后换行连接。
    return "\n".join(
        str(sentence.get("text") or sentence.get("sentence") or "").strip()
        for sentence in sentences
        if sentence.get("text") or sentence.get("sentence")
    )


def _summarize(title: str, transcript: str, description: str) -> tuple[str, List[str]]:
    """调用 LLM 生成内容摘要与关键词。

    参数:
        title:       视频标题。
        transcript:  字幕 / ASR 转写全文。
        description: 视频简介（当没有转写时作为摘要输入来源）。

    返回:
        tuple[str, List[str]]：(summary, keywords)。无任何输入来源时返回 ("", [])；
        JSON 解析失败时返回清洗后的文本前 500 字符作为摘要、空关键词列表。

    说明:
        - ``ChatOpenAI`` 是 langchain-openai 的 LLM 客户端，配置取自 ``conf.llm``
          （model_name / base_url / api_key），temperature=0.1 让输出更偏向确定性。
        - prompt 要求 LLM 只输出严格 JSON：{"summary": "...", "keywords": [...]}；
          转写文本过长时截断到前 12000 字符，避免超出模型上下文窗口。
        - ``llm.invoke(prompt).content`` 是模型回复文本；用正则 ``^```json\\s*|\\s*```$``
          剥掉可能存在的 Markdown 代码围栏后 ``json.loads`` 解析。
        - 关键词最多保留前 10 个。
    """
    # 摘要输入来源优先级：转写 > 简介 > 标题。
    source = transcript or description or title
    # 三者皆空则无摘要可生成，返回空。
    if not source:
        return "", []
    # ChatOpenAI 配置取自 conf.llm（qwen-plus dashscope 兼容接口）。
    llm = ChatOpenAI(
        model=conf.llm["model_name"], base_url=conf.llm["base_url"],
        api_key=conf.llm["api_key"], temperature=0.1,  # 低温度偏向确定性输出。
    )
    # 要求模型只输出严格 JSON；转写过长时截断到 12000 字符防超上下文。
    prompt = (
        "你是视频内容编辑。根据标题和转写文本输出严格 JSON："
        '{"summary":"200字内内容摘要","keywords":["关键词"]}。'
        f"\n标题：{title}\n内容：{source[:12000]}"
    )
    response = llm.invoke(prompt).content or ""
    # 去掉 LLM 可能套的 Markdown ```json ... ``` 围栏，只留纯 JSON。
    cleaned = re.sub(r"^```json\s*|\s*```$", "", response.strip())
    try:
        data = json.loads(cleaned)
        # 摘要与关键词分别取；关键词最多保留前 10 个。
        return str(data.get("summary", "")), [str(x) for x in data.get("keywords", [])[:10]]
    except (json.JSONDecodeError, AttributeError):
        # 非 JSON 回复：降级把原文本前 500 字符当摘要，避免直接抛错中断流程。
        return cleaned[:500], []


async def extract_video_content(
    video_url: str,
    platform: str = "bilibili",
    prefer_subtitle: bool = True,
) -> VideoContent:
    """提取单个视频内容，当前实现 Bilibili。

    异步入口：提取元数据 →（可选）字幕 →（无字幕）ASR 转写 → LLM 摘要，最后封装成
    VideoContent 返回。字幕或 ASR 任一成功都会产出 transcript；两者都失败则只保留
    简介（content_source="metadata"）。

    参数:
        video_url:       视频页面地址。
        platform:        视频平台，当前仅支持 "bilibili" / "bili" / "b站"（大小写不敏感）。
        prefer_subtitle: 是否优先尝试字幕。True 先找字幕；无字幕或字幕抓取为空时再降级
                         ASR。False 则直接走音频转写。

    返回:
        VideoContent：标题、作者、发布时间、转写、摘要、关键词、内容来源。

    抛出:
        ValueError: platform 不是支持的值时抛出。

    说明:
        - ``yt_dlp.YoutubeDL`` 是阻塞式调用，统一经 ``asyncio.to_thread`` 丢线程池，
          避免阻塞事件循环（这正是本函数声明为 async 的原因）。
        - ``tempfile.TemporaryDirectory`` 创建临时目录，其中 dir 参数优先使用配置的
          temp_dir（存在时才用，防止目录未创建导致报错）；with 退出后目录自动清理。
        - 内容来源标记：subtitle（字幕成功）/ asr（ASR 成功）/ metadata（仅有简介）。
    """
    # 平台白名单校验：目前只实现了 B 站适配，其它平台直接报错。
    if platform.lower() not in ("bilibili", "bili", "b站"):
        raise ValueError(f"暂不支持的视频平台: {platform}")
    import yt_dlp

    def metadata():
        # yt_dlp.YoutubeDL 是核心类；download=False 只解析元数据不下载。
        with yt_dlp.YoutubeDL(_ydl_options()) as ydl:
            return ydl.extract_info(video_url, download=False)

    # yt-dlp 是阻塞调用，放线程池执行避免卡事件循环。
    info = await asyncio.to_thread(metadata)
    transcript = ""              # 转写文本初始为空。
    content_source = "metadata"  # 内容来源默认只有简介。
    if prefer_subtitle:
        # 优先字幕：从元数据里挑出最优字幕轨道并抓取其文本。
        track = _pick_subtitle(info)
        if track:
            transcript = await asyncio.to_thread(_subtitle_text, track)
            content_source = "subtitle" if transcript else "metadata"
    if not transcript:
        try:
            # 无字幕：下载音频 → ASR 转写。临时目录优先用配置的 temp_dir。
            with tempfile.TemporaryDirectory(dir=conf.account_monitor["temp_dir"] if Path(
                    conf.account_monitor["temp_dir"]).exists() else None) as directory:
                wav = await asyncio.to_thread(_download_wav, video_url, directory)
                transcript = await asyncio.to_thread(_transcribe, wav)
            content_source = "asr" if transcript else "metadata"
        except Exception as exc:
            # 音频转写失败不致命：降级为只用简介，并记 warning 便于排查。
            logger.warning(f"[video] 无字幕且音频转写失败，仅使用简介: {exc}")
    # 生成摘要与关键词（LLM 阻塞调用同样放线程池）。
    summary, keywords = await asyncio.to_thread(
        _summarize, info.get("title") or "", transcript, info.get("description") or ""
    )
    # 发布时间：Unix 时间戳 -> UTC ISO 8601（精确到秒）；无时间则留空。
    timestamp = info.get("timestamp") or info.get("release_timestamp") or 0
    published = datetime.fromtimestamp(float(timestamp), timezone.utc).isoformat(timespec="seconds") if timestamp else ""
    return VideoContent(
        video_id=str(info.get("id") or info.get("bvid") or ""),  # 视频 ID（id/bvid 任取其一）。
        title=info.get("title") or "",                           # 标题，取不到留空。
        author=info.get("uploader") or "",                       # 上传者 / 作者。
        published_at=published,                                  # 格式化后的发布时间。
        transcript=transcript,                                   # 字幕/ASR 全文。
        summary=summary,                                         # LLM 摘要。
        keywords=keywords,                                       # 关键词列表。
        source_url=info.get("webpage_url") or video_url,         # 原始地址回退到入参。
        content_source=content_source,                           # subtitle/asr/metadata 来源标记。
    )
