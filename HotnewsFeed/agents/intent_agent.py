#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: intent_agent.py
项目: HotnewsFeed

本文件干什么：
    意图识别 Agent —— 把「用户的话 + 最近对话」喂给 LLM，识别出 HotnewsFeed 的上游任务意图：
      hotspot_query   ① 查询某模块当前热点新闻
      latest_news     ② 查询某模块最新新闻
      account_follow   ③ 一次性查询账户最近发布
      account_monitor  ④ 持续账户监控的注册、检查、状态或停止
    （超出范围 → out_of_scope）
    提示词模板见 prompt/main_prompt.py 的 HotnewsFeedPrompts.intent_prompt()，
    输出 JSON 结构（intents / user_queries / follow_up_message）与协调调度 Agent 的 task_type 对齐。

模块依赖:
- ``langchain_core.prompts`` : ChatPromptTemplate，LangChain 提示词模板类（from_template 建模板）。
- ``langchain_openai``       : ChatOpenAI，OpenAI 兼容接口调用通义千问（qwen-plus，见 config.ini [llm]）。
- ``Config``                 : 全局配置单例（config.ini）。conf.llm 提供 LLM 参数，conf.temperature 提供采样温度。
- ``prompt.main_prompt``     : HotnewsFeedPrompts 提示词模板库。intent_prompt() 返回意图识别模板。
- ``create_logger``          : 项目统一日志器。

典型调用链::

    CoordinatorAgent.route(intent)
      → intent_agent(user_input)                        # 本模块对外函数
      → HotnewsFeedPrompts.intent_prompt() | llm        # LangChain 链：模板 + ChatOpenAI
      → chain.invoke({conversation_history, query, current_date}).content
      → re.sub 去掉 ```json 代码块包裹
      → json.loads 解析成 dict → 提取 intents / user_queries / follow_up_message
      → 返回给协调器做任务路由

对外暴露的接口：
- intent_agent : 唯一对外函数。输入用户话术，返回 (intents, user_queries, follow_up_message)。

用法：
    from agents.intent_agent import intent_agent
    intents, user_queries, follow_up_message = intent_agent("帮我查一下科技模块的热点新闻")

演示：
    python -m agents.intent_agent    # 跑一段真实 LLM 意图识别（需 config.ini 里的 api_key 有效）
"""

# json：解析 LLM 返回的意图 JSON；re：清理 LLM 输出的 Markdown 代码块；datetime：取当前日期。
import json
import re
from datetime import datetime

# ======================= 引入需要用到的模块 =======================
# ChatPromptTemplate：LangChain 的提示词模板类（和模板库同款）。
from langchain_core.prompts import ChatPromptTemplate
# ChatOpenAI：OpenAI 兼容接口调用通义千问（qwen-plus，见 config.ini [llm]）。
from langchain_openai import ChatOpenAI

# 项目统一配置 / 日志 / 提示词模板库
from config import Config
from create_logger import logger
from prompt.main_prompt import HotnewsFeedPrompts

# ===== 全局配置与 LLM =====
# conf 取 config.ini（[llm] 的 base_url / api_key / model_name，[temperature] 的采样温度）。
# llm 是 ChatOpenAI 对象，用 OpenAI 兼容接口调 qwen-plus，给意图识别链用。
conf = Config()
llm = ChatOpenAI(
    model=conf.llm["model_name"],
    base_url=conf.llm["base_url"],
    api_key=conf.llm["api_key"],
    temperature=conf.temperature,
)

# 最近对话历史（本轮之前）。【模拟】新版对话接入后替换成真正的多轮上下文。
conversation_history = ""


def intent_agent(user_input):
    """
    用法：识别用户输入的意图。把「用户的话 + 最近对话」喂给 LLM，让它输出 JSON 意图。

    链路：
      HotnewsFeedPrompts.intent_prompt()（LangChain 模板）
      → | llm（ChatOpenAI）串成链
      → chain.invoke({conversation_history, query, current_date}) 调用 LLM
      → re.sub 去掉 Markdown 代码块包裹 → json.loads 解析成 dict → .get 取三字段

    参数:
        user_input: 用户本轮输入的问题字符串。

    返回:
        (intents, user_queries, follow_up_message) 三个值：
          intents              意图列表，如 ['hotspot_query']（可能同时命中多个意图）；
          user_queries         每个意图改写后的明确问题，如 {"hotspot_query": "查询科技模块当前热点新闻"}；
          follow_up_message    追问消息（有歧义或 out_of_scope 时非空，协调器会直接返回给用户）。

    抛出:
        json.JSONDecodeError: LLM 输出不是合法 JSON 时抛出（LLM 偶发不守格式，上层需兜底）。

    说明:
        - ``HotnewsFeedPrompts.intent_prompt()`` 是 prompt/main_prompt.py 里的静态方法，
          返回一个 ChatPromptTemplate（占位符 {current_date} / {conversation_history} / {query}）。
        - ``| llm`` 是 LangChain 的管道符：把模板和 LLM 串成可调用的链，invoke 填占位符后执行。
        - ``re.sub(r'^```json\\s*|\\s*```$', '', ...)`` 去掉 LLM 常输出的 Markdown 代码块
          （```json ... ```），避免 json.loads 解析失败。
        - ``conversation_history`` 只取最近 6 行对话，防止多轮上下文超出模型输入长度。
    """

    # ===== 创建意图识别链：提示模板 + LLM =====
    # LangChain 管道符 | ：把 HotnewsFeedPrompts 里的意图识别模板和 llm 串成一条链。
    chain = HotnewsFeedPrompts.intent_prompt() | llm

    # ===== 调用 LLM 进行意图识别 =====
    # datetime.now().strftime('%Y-%m-%d')：取当前日期（给 LLM 当"当前日期"）。
    #   与 SmartVoyage 原版用 pytz 取上海时区不同，这里先用本地时间；跨时区部署再换。
    current_date = datetime.now().strftime('%Y-%m-%d')
    # chain.invoke({占位符})：填好模板里的变量，执行链，返回 LLM 的回答对象。
    #   conversation_history 取最近 6 行对话（'\n'.join(...split("\n")[-6:])），太长会超过模型上下文。
    #   .content.strip()：content 是回答文本，strip() 去首尾空白。
    intent_response = chain.invoke(
        {"conversation_history": '\n'.join(conversation_history.split("\n")[-6:]),
         "query": user_input, "current_date": current_date}).content.strip()
    logger.info(f"意图识别原始响应: {intent_response}")

    # ===== 清理响应 =====
    # LLM 有时会把 JSON 包在 Markdown 代码块里（```json ... ```），
    # re.sub(正则, 替换成, 字符串)：把开头的 ```json 和结尾的 ``` 去掉，再 strip()。
    intent_response = re.sub(r'^```json\s*|\s*```$', '', intent_response).strip()
    logger.info(f"清理后响应: {intent_response}")

    # ===== 解析成字典 =====
    # json.loads(字符串)：把 JSON 字符串解析成 Python 字典。
    # 注意：若 LLM 仍输出非 JSON（清理后仍不合规），这里会抛 JSONDecodeError，由上层兜底。
    # LLM 输出的 JSON 结构是（见 prompt/main_prompt.py 的 HotnewsFeedPrompts.intent_prompt）：
    # {"intents": ["intent1", "intent2"], "user_queries": {"intent1": "user_query1", ...}, "follow_up_message": "追问消息"}
    intent_output = json.loads(intent_response)

    # ===== 提取意图、改写问题和追问消息 =====
    # .get(key, 默认值)：取键，取不到就用默认值（防止 LLM 漏字段导致 KeyError）
    intents = intent_output.get("intents", [])            # 意图列表
    user_queries = intent_output.get("user_queries", {})  # 每个意图改写后的问题（字典）
    follow_up_message = intent_output.get("follow_up_message", "")  # 追问消息
    logger.info(f"intents: {intents}||user_queries: {user_queries}||follow_up_message: {follow_up_message}")

    # 返回三元组，供协调器做任务路由；follow_up_message 非空时协调器会直接回给用户。
    return intents, user_queries, follow_up_message


if __name__ == "__main__":
    # 真实 LLM 意图识别演示（需 config.ini 里的 api_key 有效）
    demo = "帮我查一下科技模块的热点新闻"
    intents, user_queries, follow_up_message = intent_agent(demo)   # 调用意图识别（真实 LLM）。
    print(f"\n用户输入: {demo}")
    print(f"intents: {intents}")
    print(f"user_queries: {user_queries}")
    print(f"follow_up_message: {follow_up_message}")

    # 再试一个「关注账户」的意图
    demo2 = "关注 @新京报 的微博新发布"
    intents2, user_queries2, follow_up_message2 = intent_agent(demo2)   # 第二次调用，验证账户关注类意图。
    print(f"\n用户输入: {demo2}")
    print(f"intents: {intents2}")
    print(f"user_queries: {user_queries2}")
    print(f"follow_up_message: {follow_up_message2}")
