#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Video Agent：经 Video MCP 提取字幕/音频转写，并生成视频摘要。"""

from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

from a2a.protocol import encode_result, parse_task, reply_text
from create_logger import logger

agent_card = AgentCard(
    name="video",
    description="视频内容 Agent：提取 B 站字幕，无字幕时音频转写，再生成摘要和关键词",
    url="http://localhost:8008",
    version="1.0.0",
    capabilities={"streaming": False, "memory": False},
    skills=[AgentSkill(
        id="extract_video_content",
        name="extract_video_content",
        description="输入视频地址，返回标题、作者、转写、摘要、关键词和内容来源",
        tags=["video", "subtitle", "asr", "summary", "bilibili"],
        examples=[
            '{"video_id":"BV...","transcript":"...","summary":"...",'
            '"keywords":["..."] ,"content_source":"subtitle|asr"}'
        ],
        input_modes=["text/plain"], output_modes=["application/json"],
    )],
)


class VideoAgent(A2AServer):
    name = "video"
    role = "视频字幕提取 · 音频转写 · 内容摘要"

    def __init__(self):
        super().__init__(agent_card=agent_card)

    def extract(self, video_url: str, platform: str = "bilibili",
                prefer_subtitle: bool = True):
        from mcp_servers.mcp_access import extract_video_content, sync_call
        logger.info(f"[video-agent] 提取视频内容: {video_url}")
        return sync_call(extract_video_content(video_url, platform, prefer_subtitle))

    def handle_message(self, message):
        task_type, params, from_agent = parse_task(message)
        logger.info(f"[video-agent] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            if task_type != "extract_video_content":
                raise ValueError(f"未知任务类型: {task_type}")
            result = self.extract(
                params.get("video_url", ""), params.get("platform", "bilibili"),
                params.get("prefer_subtitle", True),
            )
            text = encode_result(True, result, None)
        except Exception as exc:
            logger.error(f"[video-agent] 处理失败: {exc}")
            text = encode_result(False, None, str(exc))
        return reply_text(text, message.message_id, message.conversation_id)


def create_video_agent():
    logger.info("=== Video Agent 信息 ===")
    logger.info(f"名称: {VideoAgent.name}")
    logger.info(f"职责: {VideoAgent.role}")
    return VideoAgent()


if __name__ == "__main__":
    run_server(create_video_agent(), host="127.0.0.1", port=8008)
