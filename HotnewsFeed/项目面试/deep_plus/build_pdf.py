# -*- coding: utf-8 -*-
"""把 deep_plus/content 下的 markdown 草稿编译成两个 PDF。

文件一：《HotnewsFeed 项目面试深度解析》(p1_s1..p1_s8)  ->  项目面试答案_deep_plus.pdf
文件二：《项目之外 · Agent 通用技术面试深度解析》(p2_s1..p2_s10) -> 项目之外面试问题_deep_plus.pdf

实现要点：
- 用 PyMuPDF(fitz) 的 TextWriter 逐行排版，支持中文（微软雅黑/雅黑粗体/黑体）。
- 支持的 markdown：标题(#/##/###/####)、加粗(**..**)、行内代码(`..`)、代码块(```)、
  无序列表(-)、有序列表(1.)、表格行(|..|)。
- 代码块与行内代码在 ASCII 部分用 Consolas，中文部分回退到黑体，保证中文正常显示。
"""
import os
import re
import fitz

BASE = os.path.dirname(os.path.abspath(__file__))
CONTENT = os.path.join(BASE, "content")
OUT_DIR = os.path.dirname(BASE)  # 项目面试/

F_BODY = "C:/Windows/Fonts/msyh.ttc"      # 微软雅黑（正文）
F_BOLD = "C:/Windows/Fonts/msyhbd.ttc"    # 微软雅黑粗体
F_HEAD = "C:/Windows/Fonts/simhei.ttf"    # 黑体（标题）
F_CODE = "C:/Windows/Fonts/consola.ttf"   # Consolas（代码 ASCII）

font_body = fitz.Font(fontfile=F_BODY)
font_bold = fitz.Font(fontfile=F_BOLD)
font_head = fitz.Font(fontfile=F_HEAD)
font_code = fitz.Font(fontfile=F_CODE)

PAGE_W, PAGE_H = 595.0, 842.0   # A4
MARGIN = 50.0
TOP, BOTTOM = 46.0, 46.0
CW = PAGE_W - 2 * MARGIN

# 配色（RGB 0..1）
C_TITLE = (0.10, 0.16, 0.28)
C_SUB = (0.42, 0.42, 0.42)
C_SECTION = (0.05, 0.35, 0.60)
C_QUESTION = (0.02, 0.45, 0.30)
C_LABEL = (0.75, 0.25, 0.10)
C_BODY = (0.15, 0.15, 0.15)
C_CODE_BG = (0.93, 0.94, 0.95)
C_CODE = (0.13, 0.15, 0.25)

INLINE_RE = re.compile(r"(\*\*.+?\*\*|`.+?`)")


def is_cjk(ch):
    o = ord(ch)
    return (0x3000 <= o <= 0x303F) or (0x4E00 <= o <= 0x9FFF) or \
           (0x3400 <= o <= 0x4DBF) or (0xFF00 <= o <= 0xFFEF) or \
           (0x2000 <= o <= 0x206F)


def code_runs(text):
    """把一段代码文字拆成 (子串, 字体) 序列：ASCII 用 Consolas，其余回退黑体。"""
    out = []
    buf, cur = "", None
    for ch in text:
        f = font_code if (ord(ch) < 128 and font_code.has_glyph(ord(ch))) else font_head
        if cur is None:
            cur, buf = f, ch
        elif f is cur:
            buf += ch
        else:
            out.append((buf, cur))
            cur, buf = f, ch
    if buf:
        out.append((buf, cur))
    return out


def parse_inline(text):
    """把段落拆成 (文字, 样式) 列表；样式 n=正文 b=粗体 c=代码。"""
    parts, last = [], 0
    for m in INLINE_RE.finditer(text):
        if m.start() > last:
            parts.append((text[last:m.start()], "n"))
        seg = m.group(0)
        if seg.startswith("**"):
            parts.append((seg[2:-2], "b"))
        else:
            parts.append((seg[1:-1], "c"))
        last = m.end()
    if last < len(text):
        parts.append((text[last:], "n"))
    return parts


def style_font(style):
    return font_bold if style == "b" else font_body


def tokenize(parts, size):
    """把已解析的段落拆成可换行 token：(文字, 字体)。空格与 CJK 字符是换行边界。"""
    tokens = []
    for text, style in parts:
        if style == "c":
            runs = code_runs(text)
        else:
            runs = [(text, style_font(style))]
        for sub, f in runs:
            buf = ""
            for ch in sub:
                if ch in (" ", "\u3000", "\t"):
                    if buf:
                        tokens.append((buf, f))
                        buf = ""
                elif is_cjk(ch):
                    if buf:
                        tokens.append((buf, f))
                        buf = ""
                    tokens.append((ch, f))
                else:
                    buf += ch
            if buf:
                tokens.append((buf, f))
    return tokens


class Doc:
    def __init__(self, title, subtitle):
        self.doc = fitz.open()
        self.title = title
        self.subtitle = subtitle
        self.page = None
        self.y = 0.0
        self._new_page()

    def _new_page(self):
        self.page = self.doc.new_page(width=PAGE_W, height=PAGE_H)
        self.y = TOP

    def need(self, h):
        if self.y + h > PAGE_H - BOTTOM:
            self._new_page()

    def put_line(self, tokens, x0, size, color, leading):
        """用 TextWriter 画出 token 序列（同一行，基准线 self.y）。"""
        tw = fitz.TextWriter(self.page.rect)
        x = x0
        for text, f in tokens:
            tw.append((x, self.y + size * 0.85), text, font=f, fontsize=size)
            x += f.text_length(text, fontsize=size)
        tw.write_text(self.page, color=color)

    def wrap_draw(self, tokens, x0, width, size, color, leading,
                  first_indent=0.0, hanging=0.0):
        """按宽度换行并绘制一段。first_indent=首行缩进，hanging=换行后悬挂缩进。"""
        line = []
        start_x = x0 + first_indent
        used = 0.0
        width_line = width - first_indent

        def flush(new_start):
            nonlocal line, start_x, used, width_line
            if not line:
                return
            self.need(leading * size)
            self.put_line(line, start_x, size, color, leading)
            self.y += leading * size
            line = []
            start_x = x0 + new_start
            used = 0.0
            width_line = width - new_start

        for tok in tokens:
            tok_w = tok[1].text_length(tok[0], fontsize=size)
            if line and used + tok_w > width_line:
                flush(hanging)
            if tok_w > width_line:  # 单个 token 超宽：按字符硬拆
                for ch in tok[0]:
                    ch_w = tok[1].text_length(ch, fontsize=size)
                    if line and used + ch_w > width_line:
                        flush(hanging)
                    line.append((ch, tok[1]))
                    used += ch_w
            else:
                line.append(tok)
                used += tok_w
        flush(hanging)

    def draw_code_block(self, lines):
        size = 8.6
        leading = 1.35
        h = leading * size * len(lines) + 14
        self.need(h)
        y0 = self.y
        self.page.draw_rect(fitz.Rect(MARGIN - 4, y0, PAGE_W - MARGIN + 4, y0 + h),
                            color=None, fill=C_CODE_BG)
        self.y += 7
        for ln in lines:
            runs = code_runs(ln.rstrip("\n"))
            tw = fitz.TextWriter(self.page.rect)
            x = MARGIN + 6
            for text, f in runs:
                tw.append((x, self.y + size * 0.85), text, font=f, fontsize=size)
                x += f.text_length(text, fontsize=size)
            tw.write_text(self.page, color=C_CODE)
            self.y += leading * size
        self.y = y0 + h + 4


def parse_table_row(line):
    cells = [c.strip() for c in line.strip().strip("|").split("|")]
    if all(re.fullmatch(r":?-{2,}:?", c or "-") for c in cells):
        return None  # 分隔行
    return cells


def render_md(doc, path, first):
    with open(path, "r", encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    i, n = 0, len(lines)
    while i < n:
        ln = lines[i]
        if not ln.strip():
            i += 1
            continue
        s = ln.strip()

        # 代码块
        if s.startswith("```"):
            block = []
            i += 1
            while i < n and not lines[i].strip().startswith("```"):
                block.append(lines[i])
                i += 1
            i += 1
            doc.draw_code_block(block)
            continue

        # 表格
        if s.startswith("|") and s.endswith("|"):
            cells = parse_table_row(s)
            if cells is None:
                i += 1
                continue
            text = "  │  ".join(cells)
            doc.wrap_draw(tokenize(parse_inline(text), 9.2), MARGIN, CW, 9.2,
                          C_BODY, 1.45)
            i += 1
            continue

        # 标题
        m = re.match(r"^(#{1,4})\s+(.*)$", s)
        if m:
            level = len(m.group(1))
            text = re.sub(r"[`*]", "", m.group(2))
            if level == 1:
                if first:
                    doc.need(70)
                    doc.put_line([(doc.title, font_head)], MARGIN, 20, C_TITLE, 1.35)
                    doc.y += 20 * 1.35
                    doc.put_line([(doc.subtitle, font_body)], MARGIN, 12, C_SUB, 1.3)
                    doc.y += 12 * 1.3
                    doc.put_line([("整理日期：2026-08-30", font_body)], MARGIN, 10,
                                 C_SUB, 1.3)
                    doc.y += 10 * 1.3 + 8
                    first = False
                # 其它文件里的 # 标题重复，跳过
            elif level == 2:
                doc.need(26)
                doc.put_line([(text, font_bold)], MARGIN, 15, C_SECTION, 1.35)
                doc.y += 15 * 1.35 + 4
            elif level == 3:
                doc.need(24)
                doc.put_line([(text, font_bold)], MARGIN, 12.5, C_QUESTION, 1.35)
                doc.y += 12.5 * 1.35 + 3
            else:
                doc.need(22)
                doc.put_line([(text, font_bold)], MARGIN, 11, C_BODY, 1.35)
                doc.y += 11 * 1.35 + 3
            i += 1
            continue

        # 【深度回答】等整行标签
        if re.fullmatch(r"【[^】]{2,20}】", s):
            doc.need(22)
            doc.put_line([(s, font_bold)], MARGIN, 11.5, C_LABEL, 1.4)
            doc.y += 11.5 * 1.4 + 2
            i += 1
            continue

        # 无序列表
        if s.startswith("- ") or s.startswith("* "):
            content = s[2:]
            tokens = tokenize(parse_inline(content), 10.5)
            tokens.insert(0, ("• ", font_body))
            doc.wrap_draw(tokens, MARGIN, CW, 10.5, C_BODY, 1.5,
                          hanging=11.0)
            i += 1
            continue

        # 有序列表
        mo = re.match(r"^(\d+)[.、]\s+(.*)$", s)
        if mo:
            content = mo.group(2)
            tokens = tokenize(parse_inline(content), 10.5)
            tokens.insert(0, (mo.group(1) + ". ", font_bold))
            doc.wrap_draw(tokens, MARGIN, CW, 10.5, C_BODY, 1.5,
                          hanging=15.0)
            i += 1
            continue

        # 普通段落
        doc.wrap_draw(tokenize(parse_inline(s), 10.5), MARGIN, CW, 10.5,
                      C_BODY, 1.5)
        i += 1


def build(file_ids, out_name, subtitle):
    out_path = os.path.join(OUT_DIR, out_name)
    title = ""
    doc = None
    for idx, fid in enumerate(file_ids):
        path = os.path.join(CONTENT, f"{fid}.md")
        if not os.path.exists(path):
            print(f"[跳过] 不存在: {path}")
            continue
        if doc is None:
            with open(path, "r", encoding="utf-8") as fh:
                for ln in fh:
                    m = re.match(r"^#\s+(.*)$", ln.strip())
                    if m:
                        title = m.group(1).strip()
                        break
            doc = Doc(title, subtitle)
        render_md(doc, path, first=(idx == 0))
    # 每页底部页码
    for pno, page in enumerate(doc.doc, start=1):
        num = str(pno)
        w = font_body.text_length(num, fontsize=8)
        page.insert_text((PAGE_W / 2 - w / 2, PAGE_H - 20), num,
                         fontname="f0", fontfile=F_BODY, fontsize=8,
                         color=(0.45, 0.45, 0.45))
    doc.doc.save(out_path)
    print(f"[完成] {out_path}  共 {len(doc.doc)} 页")


if __name__ == "__main__":
    build([f"p1_s{i}" for i in range(1, 9)],
          "项目面试答案_deep_plus.pdf",
          "HotnewsFeed 项目面试 · 深度解析版（deep_plus）")
    build(["p2_s1", "p2_s2", "p2_s3", "p2_s4", "p2_s5",
           "p2_s6", "p2_s7", "p2_s8", "p2_s9", "p2_s10"],
          "项目之外面试问题_deep_plus.pdf",
          "Agent 通用技术面试 · 深度解析版（deep_plus）")
