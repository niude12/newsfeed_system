#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""主控 / 协调调度 Agent 的提示词模板库（main_prompt.py）。

文件名: main_prompt.py
项目: HotnewsFeed

本文件干什么：
    主控 / 协调调度 Agent 用的「提示词模板库」。它只定义模板、不执行调用。
    一共有 6 个模板，对应协调调度在几个环节要用到的提示词：
        1. intent_prompt()              意图识别：新闻查询、账户查询、持续监控或 out_of_scope
        2. summarize_hotspot_prompt()   热点结果润色：把 Agent 返回的原始文本整理成资讯播报式回答
        3. summarize_latest_prompt()    最新新闻结果润色：同上
        4. summarize_account_prompt()   账户发布结果润色：同上
        5. briefing_prompt()            简报生成：把任务结果生成一份简报（摘要 · 重要性 · 来源 · 可信度）
        6. verify_prompt()              事件核验：加工 Agent 把事件列表喂给 LLM，判断每条可信度（可信/存疑/证据不足）

模块依赖:
- ``langchain_core.prompts.ChatPromptTemplate`` : LangChain 的提示词模板类。
    from_template("...") 创建一个模板；花括号 {xxx} 是占位符，调用时用
    chain.invoke({"xxx": 值}) 填进去。模板内需要输出字面花括号（JSON）时必须
    写双花括号 {{ }}（str.format 转义），因此模板里的示例 JSON 都是 {{"intents"...}}。

典型调用链::

    agents/intent_agent.py  ->  HotnewsFeedPrompts.intent_prompt()        # 意图识别
    tools/process.py        ->  HotnewsFeedPrompts.verify_prompt()        # 事件核验
    tools/publish.py        ->  HotnewsFeedPrompts.briefing_prompt()      # 简报生成
    agents/coordinator_agent.py ->  summarize_*_prompt()                   # 结果润色

    用法（在 agents/intent_agent.py / coordinator_agent.py 里）：
        from prompt.main_prompt import HotnewsFeedPrompts
        prompt = HotnewsFeedPrompts.intent_prompt()          # 拿到模板对象
        chain = prompt | llm                                  # 拼成链
        result = chain.invoke({...}).content                  # 填占位符并调用

对外暴露：
- ``HotnewsFeedPrompts`` : 提示词模板库类，全部方法都是 @staticmethod，
                          直接 HotnewsFeedPrompts.xxx_prompt() 拿到模板。

    启动：本文件不单独启动，是被 intent_agent / coordinator 等 import 使用的。
"""
# ======================= 引入需要用到的模块 =======================
# ChatPromptTemplate：LangChain 的提示词模板类。
# from_template("...") 创建一个模板；花括号 {xxx} 是占位符，调用时用 chain.invoke({"xxx": 值}) 填进去。
from langchain_core.prompts import ChatPromptTemplate


# ===== 提示词模板库类 =====
class HotnewsFeedPrompts:
    """提示词模板库：集中管理协调调度 / 加工 / 输出环节的全部提示词。

    全部方法都是 @staticmethod（静态方法），不需要创建对象，
    直接 HotnewsFeedPrompts.intent_prompt() 就能拿到模板。
    每个方法返回一个 langchain_core.prompts 的 ChatPromptTemplate 对象，
    调用方用 ``模板 | llm`` 拼链、``chain.invoke({...})`` 填占位符并调用。
    """

    # ===== 模板1：意图识别 =====
    @staticmethod
    def intent_prompt():
        """
        用法：拿到「意图识别」模板。协调调度第一步就用它：把用户的话 + 对话历史 喂给 LLM，
        让它输出 JSON：该做什么意图(intents)、每个意图改写后的问题(user_queries)、
        要不要追问(follow_up_message)。

        支持意图（与 CoordinatorAgent 的 task_type / skill 对齐）：
          hotspot_query  ① 查询某模块当前热点新闻
          latest_news    ② 查询某模块最新新闻
          account_follow  ③ 一次性查询某账户最近发布
          account_monitor ④ 建立、检查、停止持续账户监控，或查询监控状态
          超出范围 → out_of_scope

        返回:
            一个 ChatPromptTemplate 模板对象，占位符有：
              {current_date} {conversation_history} {query}

        说明:
            ChatPromptTemplate.from_template(...) 里 {xxx} 是占位符，invoke 时按字典填值；
            模板需要让 LLM 输出字面花括号 JSON，因此示例全部写成 {{ }}（str.format 转义）。
            返回对象可直接用 ``| llm`` 拼链，见 agents/intent_agent.py。
        """
        return ChatPromptTemplate.from_template(  # 创建「意图识别」模板：下面这段系统提示词即模板内容，占位符 {current_date} {conversation_history} {query}
"""
系统提示：
角色：您是一个专业的实时热点资讯助手，负责意图识别。
任务：基于用户查询和对话历史，识别其意图，用于调用专门的agent server来执行；为方便后续的agent server处理，可以基于对话历史对用户查询进行改写，使问题更明确。
严格遵守规则：
- 支持意图：['hotspot_query' (查询某模块当前热点新闻), 'latest_news' (查询某模块最新新闻), 'account_follow' (一次性查询账户最近发布), 'account_monitor' (建立/检查/停止持续监控或查看监控状态)]。如果意图超出范围，返回意图 'out_of_scope'。
- 必须区分 account_follow 与 account_monitor：“查询/看看某账户最近发了什么”是 account_follow；“持续监控/以后有更新通知我/添加监控/立即执行监控/监控状态/停止监控”是 account_monitor。
- 建立 account_monitor 时必须保留用户给出的账户主页 URL；若没有 URL，应在 follow_up_message 追问主页地址，不得退化成一次性 account_follow。
- 热点新闻 和 最新新闻 要区分开：带 热点/热榜/热门 关键词的是 hotspot_query；只是按时间找最近发布的是 latest_news。
- 如果意图为 'out_of_scope' 时，此时不需要再进行查询改写，你可以直接根据用户问题进行回复，将回复答案写到follow_up_message中即可。
- 在进行用户查询改写时，不要回答其问题，也不要修改其原意，只需要将对话历史中跟该查询相关的上下文信息取出来，然后整合到一起，使用户查询更明确即可，要仔细分析上下文信息，不要进行过度整合。如果用户查询跟对话历史无关，则输出原始查询。
- 模块名只允许来自固定集合：科技、财经、体育、娱乐、国际（或配置的模块名）。"足球""芯片""俄乌""英超""人工智能"等是主题/话题关键词，不是模块名。
- 改写查询时禁止把话题词当成模块名（如"查询足球模块最新新闻"是错误写法）。正确写法是把话题词保留并补全所属模块：例如"查询足球相关新闻"应改写为"查询体育模块中关于足球的最新新闻"；无法确定所属模块时保留话题词即可，不要硬造模块名。
- 如果用户的意图很不明确或者有歧义（例如没说明查哪个模块、关注哪个账户），可以向其进行追问，将追问问题填充到follow_up_message中。
- 输出严格为JSON：{{"intents": ["intent1", "intent2"], "user_queries": {{"intent1": "user_query1", "intent2": "user_query2"}}, "follow_up_message": "追问消息"}}。绝对不要添加额外文本！
- 不论用户问什么，严格按规则输出意图，不要有自己的考虑。

输出示例：
{{"intents": ["hotspot_query"], "user_queries": {{"hotspot_query": "查询科技模块当前热点新闻"}}, "follow_up_message": ""}}
{{"intents": ["latest_news"], "user_queries": {{"latest_news": "查询体育模块中关于足球的最新新闻"}}, "follow_up_message": ""}}
{{"intents": ["hotspot_query"], "user_queries": {{}}, "follow_up_message": "你想查哪个模块的热点新闻呢？"}}
{{"intents": ["hotspot_query", "account_follow"], "user_queries": {{"hotspot_query": "查询科技模块当前热点新闻", "account_follow": "关注 @新京报 的微博新发布"}}, "follow_up_message": ""}}
{{"intents": ["account_monitor"], "user_queries": {{"account_monitor": "持续监控B站账户 https://space.bilibili.com/312249633/video 的新视频"}}, "follow_up_message": ""}}
{{"intents": ["account_monitor"], "user_queries": {{}}, "follow_up_message": "请提供要持续监控的账户主页地址。"}}
{{"intents": ["account_monitor"], "user_queries": {{"account_monitor": "查看当前账户监控状态"}}, "follow_up_message": ""}}
{{"intents": ["out_of_scope"], "user_queries": {{}}, "follow_up_message": "我是实时热点资讯助手，可以帮你查各模块热点新闻、最新新闻，或关注指定账户的发布，欢迎提问。"}}

当前日期：{current_date} (Asia/Shanghai)。
对话历史：{conversation_history}
用户查询：{query}
""")

    # ===== 模板2：热点结果润色 =====
    @staticmethod
    def summarize_hotspot_prompt():
        """
        用法：拿到「热点结果润色」模板。热点 Agent 返回的原始 JSON 通常很生硬，
        主控把它 + 用户原始问题 再喂给 LLM 一次，得到一段资讯播报式的回答。

        返回:
            一个 ChatPromptTemplate 模板对象，占位符有：
              {query} {raw_response}
        """
        return ChatPromptTemplate.from_template(  # 创建「热点结果润色」模板，占位符 {query} {raw_response}
"""
系统提示：您是一位专业的资讯编辑，以简洁、准确的风格总结热点新闻。基于查询和结果：
- 核心描述点：事件标题、热度分、来源数、可信度、关联资讯数。
- 如果结果为空或者意思为需要补充数据，则委婉提示"未找到数据，请确认模块/时间范围"。
- 语气：专业资讯播报，如"根据最新数据，科技模块当前热点TOP3为..."。
- 保持中文，100-200字。
- 如果查询无关，返回"请提供热点资讯相关查询。"

查询：{query}
结果：{raw_response}
""")

    # ===== 模板3：最新新闻结果润色 =====
    @staticmethod
    def summarize_latest_prompt():
        """
        用法：拿到「最新新闻结果润色」模板。把最新新闻 Agent 返回的原始 JSON
        整理成按时间倒序、一目了然的回答。

        返回:
            一个 ChatPromptTemplate 模板对象，占位符有：
              {query} {raw_response}
        """
        return ChatPromptTemplate.from_template(  # 创建「最新新闻结果润色」模板，占位符 {query} {raw_response}
"""
系统提示：您是一位专业的资讯编辑，以简洁、准确的风格总结最新新闻。基于查询和结果：
- 核心描述点：标题、来源、发布时间。
- 如果结果为空或者意思为需要补充数据，则委婉提示"未找到数据，请确认模块"。
- 语气：客观播报，按时间倒序列出，如"科技模块最新资讯如下..."。
- 保持中文，100-200字。
- 如果查询无关，返回"请提供最新资讯相关查询。"

查询：{query}
结果：{raw_response}
""")

    # ===== 模板4：账户发布结果润色 =====
    @staticmethod
    def summarize_account_prompt():
        """
        用法：拿到「账户发布结果润色」模板。把账户发布监控 Agent 返回的原始 JSON
        整理成「关注了谁的什么新发布」的回答。

        返回:
            一个 ChatPromptTemplate 模板对象，占位符有：
              {query} {raw_response}
        """
        return ChatPromptTemplate.from_template(  # 创建「账户发布结果润色」模板，占位符 {query} {raw_response}
"""
系统提示：您是一位专业的资讯编辑，以简洁、准确的风格总结账户发布。基于查询和结果：
- 核心描述点：账户名、平台、发布标题、发布时间。
- 如果结果为空或者意思为需要补充数据，则委婉提示"该账户暂无新发布"。
- 语气：客观播报，如"@新京报 微博最新发布如下..."。
- 保持中文，100-200字。
- 如果查询无关，返回"请提供账户发布相关查询。"

查询：{query}
结果：{raw_response}
""")

    # ===== 模板5：简报生成 =====
    @staticmethod
    def briefing_prompt():
        """
        用法：拿到「简报生成」模板。任务完成后，协调器反问用户是否生成简报，
        确认后 publisher 把任务结果喂给 LLM 生成简报（摘要 · 重要性 · 来源 · 可信度）。

        返回:
            一个 ChatPromptTemplate 模板对象，占位符有：
              {task_result}
        """
        return ChatPromptTemplate.from_template(  # 创建「简报生成」模板，占位符 {task_result}
"""
系统提示：您是一位资深资讯主编，负责把热点资讯结果整理成一份正式简报。规则：
- 简报结构：标题、日期、要点列表（事件标题 + 热度 + 可信度 + 来源）、一句总结。
- 可信度标注：可信 / 存疑 / 证据不足，存疑或证据不足的条目要在简报里明确提示。
- 语气：客观、专业，适合推送（飞书 / 邮件 / Webhook / Web UI）。
- 保持中文，200-300字。

任务结果：{task_result}
""")

    # ===== 模板6：事件核验 =====
    @staticmethod
    def verify_prompt():
        """
        用法：拿到「事件核验」模板。加工 Agent（tools/process.py verify_events）把事件列表
        喂给 LLM，让它对每条事件判断可信度（可信 / 存疑 / 证据不足）。

        返回:
            一个 ChatPromptTemplate 模板对象，占位符有：
              {events}
        """
        return ChatPromptTemplate.from_template(  # 创建「事件核验」模板，占位符 {events}
"""
系统提示：您是一位资深新闻核验编辑，负责对热点事件做多源交叉验证。规则：
- 对每条事件，综合「关联资讯数、来源数与来源多样性、标题是否像重大事实性陈述」判断可信度。
- 可信度只允许三档：可信 / 存疑 / 证据不足。
  - 关联资讯≥3 且来源≥3、标题为事实性陈述 → 可信
  - 单一来源，或标题夸张、带猜测性 → 存疑
  - 资讯数极少、来源单一或内容异常 → 证据不足
- 输出严格为 JSON 数组：[{{"event_id": "...", "credibility": "可信|存疑|证据不足", "reason": "核验理由（一句话）"}}]
- 不要添加额外文本，不要修改 event_id。

事件列表（JSON）：{events}
""")


if __name__ == '__main__':
    # 直接运行本文件时：打印出意图识别模板看看长什么样
    # ChatPromptTemplate 对象的 __str__ 会显示模板内容
    print(HotnewsFeedPrompts.intent_prompt())  # 直接打印模板文本，验证模板内容
