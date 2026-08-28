# -*- coding: utf-8 -*-
"""视频内容提取：字幕优先，无字幕时下载音频并用 DashScope ASR 转写。"""

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

conf = Config()


@dataclass
class VideoContent:
    video_id: str
    title: str
    author: str
    published_at: str
    transcript: str
    summary: str
    keywords: List[str] = field(default_factory=list)
    source_url: str = ""
    content_source: str = "metadata"  # subtitle / asr / metadata


def _ydl_options(download: bool = False, output: str = "") -> dict:
    cfg = conf.account_monitor
    headers = {"Referer": "https://www.bilibili.com/"}
    if cfg["bilibili_cookie"]:
        headers["Cookie"] = cfg["bilibili_cookie"]
    options = {
        "quiet": True, "no_warnings": True, "http_headers": headers,
        "skip_download": not download,
        "proxy": "", "socket_timeout": 20, "retries": 2,
    }
    if cfg["cookie_file"]:
        if not Path(cfg["cookie_file"]).is_file():
            raise FileNotFoundError(f"B 站 cookie_file 不存在: {cfg['cookie_file']}")
        options["cookiefile"] = cfg["cookie_file"]
    if output:
        options["outtmpl"] = output
    return options


def _pick_subtitle(info: dict) -> dict:
    tracks = info.get("subtitles") or {}
    automatic = info.get("automatic_captions") or {}
    for language in ("zh-CN", "zh-Hans", "zh", "ai-zh", "danmaku"):
        candidates = tracks.get(language) or automatic.get(language) or []
        if candidates:
            return candidates[-1]
    for source in (tracks, automatic):
        for candidates in source.values():
            if candidates:
                return candidates[-1]
    return {}


def _subtitle_text(track: dict) -> str:
    url = track.get("url") or ""
    if url.startswith("//"):
        url = "https:" + url
    if not url:
        return ""
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read().decode("utf-8", errors="replace")
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("body"), list):
            return "\n".join(str(row.get("content", "")) for row in data["body"] if row.get("content"))
        events = data.get("events", []) if isinstance(data, dict) else []
        texts = []
        for event in events:
            for segment in event.get("segs") or []:
                if segment.get("utf8"):
                    texts.append(segment["utf8"])
        if texts:
            return "\n".join(texts)
    except json.JSONDecodeError:
        pass
    lines = []
    for line in raw.splitlines():
        text = line.strip()
        if not text or "-->" in text or text.isdigit() or text.startswith(("WEBVTT", "NOTE")):
            continue
        lines.append(re.sub(r"<[^>]+>", "", text))
    return "\n".join(lines)


def _download_wav(video_url: str, directory: str) -> str:
    import imageio_ffmpeg
    import yt_dlp

    output = str(Path(directory) / "audio.%(ext)s")
    options = _ydl_options(download=True, output=output)
    options.update({
        "format": "bestaudio/best",
        "ffmpeg_location": imageio_ffmpeg.get_ffmpeg_exe(),
        "postprocessors": [{
            "key": "FFmpegExtractAudio", "preferredcodec": "wav",
        }],
        "postprocessor_args": ["-ar", "16000", "-ac", "1"],
    })
    with yt_dlp.YoutubeDL(options) as ydl:
        ydl.download([video_url])
    files = list(Path(directory).glob("*.wav"))
    if not files:
        raise RuntimeError("视频音频转换后未找到 WAV 文件")
    return str(files[0])


def _transcribe(wav_path: str) -> str:
    import dashscope
    from dashscope.audio.asr import Recognition

    dashscope.api_key = conf.llm["api_key"]
    recognition = Recognition(
        model=conf.account_monitor["asr_model"], callback=None,
        format="wav", sample_rate=16000,
    )
    result = recognition.call(wav_path)
    if getattr(result, "status_code", 500) != 200:
        raise RuntimeError(f"DashScope ASR 失败: {getattr(result, 'message', result)}")
    sentences = result.get_sentence() or []
    if isinstance(sentences, dict):
        sentences = [sentences]
    return "\n".join(
        str(sentence.get("text") or sentence.get("sentence") or "").strip()
        for sentence in sentences
        if sentence.get("text") or sentence.get("sentence")
    )


def _summarize(title: str, transcript: str, description: str) -> tuple[str, List[str]]:
    source = transcript or description or title
    if not source:
        return "", []
    llm = ChatOpenAI(
        model=conf.llm["model_name"], base_url=conf.llm["base_url"],
        api_key=conf.llm["api_key"], temperature=0.1,
    )
    prompt = (
        "你是视频内容编辑。根据标题和转写文本输出严格 JSON："
        '{"summary":"200字内内容摘要","keywords":["关键词"]}。'
        f"\n标题：{title}\n内容：{source[:12000]}"
    )
    response = llm.invoke(prompt).content or ""
    cleaned = re.sub(r"^```json\s*|\s*```$", "", response.strip())
    try:
        data = json.loads(cleaned)
        return str(data.get("summary", "")), [str(x) for x in data.get("keywords", [])[:10]]
    except (json.JSONDecodeError, AttributeError):
        return cleaned[:500], []


async def extract_video_content(
    video_url: str,
    platform: str = "bilibili",
    prefer_subtitle: bool = True,
) -> VideoContent:
    """提取单个视频内容，当前实现 Bilibili。"""
    if platform.lower() not in ("bilibili", "bili", "b站"):
        raise ValueError(f"暂不支持的视频平台: {platform}")
    import yt_dlp

    def metadata():
        with yt_dlp.YoutubeDL(_ydl_options()) as ydl:
            return ydl.extract_info(video_url, download=False)

    info = await asyncio.to_thread(metadata)
    transcript = ""
    content_source = "metadata"
    if prefer_subtitle:
        track = _pick_subtitle(info)
        if track:
            transcript = await asyncio.to_thread(_subtitle_text, track)
            content_source = "subtitle" if transcript else "metadata"
    if not transcript:
        try:
            with tempfile.TemporaryDirectory(dir=conf.account_monitor["temp_dir"] if Path(
                    conf.account_monitor["temp_dir"]).exists() else None) as directory:
                wav = await asyncio.to_thread(_download_wav, video_url, directory)
                transcript = await asyncio.to_thread(_transcribe, wav)
            content_source = "asr" if transcript else "metadata"
        except Exception as exc:
            logger.warning(f"[video] 无字幕且音频转写失败，仅使用简介: {exc}")
    summary, keywords = await asyncio.to_thread(
        _summarize, info.get("title") or "", transcript, info.get("description") or ""
    )
    timestamp = info.get("timestamp") or info.get("release_timestamp") or 0
    published = datetime.fromtimestamp(float(timestamp), timezone.utc).isoformat(timespec="seconds") if timestamp else ""
    return VideoContent(
        video_id=str(info.get("id") or info.get("bvid") or ""),
        title=info.get("title") or "", author=info.get("uploader") or "",
        published_at=published, transcript=transcript, summary=summary,
        keywords=keywords, source_url=info.get("webpage_url") or video_url,
        content_source=content_source,
    )
