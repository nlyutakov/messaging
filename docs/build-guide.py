"""Build the offline English guide with the system Cairo library."""

import ctypes as c
import ctypes.util
from pathlib import Path
import re
import textwrap


def cairo_function(name, arguments, result=None):
    function = getattr(cairo, name)
    function.argtypes = arguments
    function.restype = result
    return function


cairo = c.CDLL(ctypes.util.find_library("cairo"))
ptr = c.c_void_p
number = c.c_double
surface_create = cairo_function("cairo_pdf_surface_create", [c.c_char_p, number, number], ptr)
surface_finish = cairo_function("cairo_surface_finish", [ptr])
surface_status = cairo_function("cairo_surface_status", [ptr], c.c_int)
surface_destroy = cairo_function("cairo_surface_destroy", [ptr])
create = cairo_function("cairo_create", [ptr], ptr)
destroy = cairo_function("cairo_destroy", [ptr])
font = cairo_function("cairo_select_font_face", [ptr, c.c_char_p, c.c_int, c.c_int])
size = cairo_function("cairo_set_font_size", [ptr, number])
color = cairo_function("cairo_set_source_rgb", [ptr, number, number, number])
move = cairo_function("cairo_move_to", [ptr, number, number])
show = cairo_function("cairo_show_text", [ptr, c.c_char_p])
page = cairo_function("cairo_show_page", [ptr])

directory = Path(__file__).resolve().parent
surface = surface_create(str(directory / "CourierLane-guide-en.pdf").encode(), 595, 842)
context = create(surface)
cursor = 72
page_number = 1


def line(value, x, y, points=11, bold=False, mono=False):
    font(context, b"DejaVu Sans Mono" if mono else b"DejaVu Sans", 0, int(bold))
    size(context, points)
    color(context, 0.08, 0.28, 0.48) if bold else color(context, 0.09, 0.14, 0.20)
    move(context, x, y)
    show(context, value.encode("utf-8"))


def finish_page():
    global page_number, cursor
    line("CourierLane  ·  .NET 10 guide", 45, 811, 9)
    line(str(page_number), 535, 811, 9)
    page(context)
    page_number += 1
    cursor = 63


chapters = (
    "getting-started", "architecture", "configuration", "commands", "rpc",
    "events", "retry", "idempotency", "redis-provider", "postgresql-provider",
    "sqlite-provider", "dead-letter-queues", "dlq-management",
    "transactional-outbox", "deployment", "troubleshooting",
)
line("CourierLane", 44, 46, 23, bold=True)
for chapter in chapters:
    code = False
    for raw in (directory / f"{chapter}.md").read_text().splitlines():
        if raw.startswith("```"):
            code = not code
            continue
        if not raw or re.fullmatch(r"\|[ |:-]+\|", raw):
            cursor += 9
            continue

        heading = raw.startswith("#")
        content = raw if code else re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", raw)
        content = content.lstrip("# ").replace("`", "") if not code else content
        if heading and cursor > 720:
            finish_page()
        if heading:
            cursor += 8
        for segment in textwrap.wrap(content, width=76 if code else 73, replace_whitespace=not code):
            if cursor > 772:
                finish_page()
            line(segment, 46 if not code else 53, cursor, 16 if heading else 10 if code else 11, heading, code)
            cursor += 22 if heading else 15
    cursor += 10

finish_page()
destroy(context)
surface_finish(surface)
status = surface_status(surface)
surface_destroy(surface)
if status:
    raise RuntimeError(f"Cairo PDF creation failed with status {status}")
