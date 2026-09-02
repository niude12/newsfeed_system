#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""离线新闻库测试入口：输入问题，exit 退出。

命令行交互程序：读取用户输入的问题，调用 OfflineNewsService.query() 走
「Redis 精确缓存 → Milvus 向量召回 → MySQL 取原文」链路返回离线新闻，
并把结果格式化为易读文本打印到终端。

对外暴露：
- main        : 交互主循环入口；
- format_rows : 把 query() 结果格式化为多行文本的内部辅助函数。
"""

import sys

from offline_news import OfflineNewsService


def format_rows(result: dict) -> str:
    """把 query() 返回的结果字典格式化为多行可读文本。

    参数:
        result: OfflineNewsService.query() 的返回，含 "items" 列表与 "source" 链路标记。

    返回:
        str：多行文本。首行汇总条数与来源链路，随后逐条输出序号/板块/标题/来源/时间/链接/摘要。

    说明:
        - result.get("source") 是查询链路标识：redis（缓存命中）或 milvus+mysql。
        - row.get("summary") or row.get("content")：摘要缺省时用正文兜底；
          text[:240] 截断长文本，避免终端刷屏。
    """
    rows = result.get("items", [])  # 记录行列表；缺省为空列表。
    # 首行汇总：条数 + 来源链路（redis 或 milvus+mysql）。
    lines = [f"== 离线新闻（{len(rows)} 条，来源链路：{result.get('source')}） =="]
    for index, row in enumerate(rows, 1):  # 从 1 开始给每条结果编号。
        lines.append(f"{index}. [{row.get('module')}] {row.get('title')}")  # 序号 + 板块 + 标题。
        lines.append(f"   {row.get('source')} · {row.get('published_at') or '时间未知'}")  # 来源 + 发布时间。
        if row.get("url"):
            lines.append(f"   {row['url']}")  # 原文链接。
        text = row.get("summary") or row.get("content") or ""  # 摘要缺省时用正文兜底。
        if text:
            lines.append(f"   {text[:240]}")  # 截断长文本，避免终端刷屏。
    return "\n".join(lines)  # 多行文本拼接成最终结果。


def main():
    """交互主循环：从 stdin 读问题，调用离线查询并打印结果。

    说明:
        - sys.stdout / sys.stdin.reconfigure(encoding="utf-8")：保证 Windows 控制台
          中文不报编码错误（reconfigure 失败则忽略，例如非终端环境）。
        - 输入 exit / quit / q 退出循环；空输入跳过本次。
        - 单次查询异常只打印错误提示，不中断循环。
    """
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # 控制台 stdout 强制 UTF-8，防中文报错。
        sys.stdin.reconfigure(encoding="utf-8")  # 控制台 stdin 强制 UTF-8。
    except Exception:
        pass  # 非终端环境（如重定向）不支持 reconfigure 时静默忽略。
    service = OfflineNewsService()  # 创建离线新闻服务实例。
    print("离线新闻查询：Redis精确缓存 → Milvus向量检索 → MySQL原文（最多3条）")
    print("输入 exit 退出")
    while True:  # 交互主循环，直到用户输入退出关键词。
        query = input("离线查询> ").strip()  # 读一行输入并去首尾空白。
        if query.lower() in ("exit", "quit", "q"):  # 命中退出关键词。
            break
        if query:  # 非空输入才发起查询。
            try:
                print(format_rows(service.query(query)))  # 查询并格式化打印。
            except Exception as exc:
                print(f"[离线查询失败] {exc}")  # 单次失败只打印提示，不中断循环。


if __name__ == "__main__":
    main()
