"""加工与发布工具仍在使用的提示词。"""

from langchain_core.prompts import ChatPromptTemplate


class ToolPrompts:
    @staticmethod
    def briefing_prompt():
        return ChatPromptTemplate.from_template("""
系统提示：您是一位资深资讯主编，负责把热点资讯结果整理成一份正式简报。规则：
- 简报结构：标题、日期、要点列表（事件标题 + 热度 + 可信度 + 来源）、一句总结。
- 可信度标注：可信 / 存疑 / 证据不足，存疑或证据不足的条目要明确提示。
- 语气客观、专业，适合推送；保持中文，200-300字。

任务结果：{task_result}
""")

    @staticmethod
    def verify_prompt():
        return ChatPromptTemplate.from_template("""
系统提示：您是一位资深新闻核验编辑，负责对热点事件做多源交叉验证。规则：
- 综合关联资讯数、来源数与来源多样性、标题是否为重大事实性陈述判断可信度。
- 可信度只允许：可信 / 存疑 / 证据不足。
- 输出严格为 JSON 数组：[{{"event_id":"...","credibility":"可信|存疑|证据不足","reason":"一句话理由"}}]
- 不要添加额外文本，不要修改 event_id。

事件列表（JSON）：{events}
""")
