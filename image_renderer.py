import io
import math
import time
import requests
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from PIL import ImageDraw, ImageFont
from PIL import Image as PILImage

from config import (
    FONT_FILE,
    _SCALE,
    _CELL,
    _CELL_GAP,
    _GRID_COLS,
    _GRID_ROWS,
    _GRID_W,
    _GRID_H,
    _AVATAR_D,
    _CARD_PAD,
    _CARD_W,
    _CARD_H,
    _CARD_GAP,
    _IMG_PAD,
    _COLOR_MAP,
    _DEFAULT_CELL,
    _CARD_BG,
    _BORDER,
    _AVATAR_FB,
    _INF_SQUARE_SIZE,
    _INF_ROW_GAP,
    _INF_LINE_GAP,
    _INF_SECTION_GAP,
    _INF_ROUNDS,
    _INF_NAME_FS,
    _INF_SUB_FS,
    _INF_STAT_FS,
    _INF_SCORE_FS,
)

@lru_cache(maxsize=None)
def _font(size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(FONT_FILE, size)


def _fetch_avatar(url: str, retries: int = 3) -> PILImage.Image | None:
    sep = "&" if "?" in url else "?"
    url = f"{url}{sep}size=64"
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=10)
            if res.ok:
                img = PILImage.open(io.BytesIO(res.content)).convert(
                    "RGBA"
                )
                img = img.resize(
                    (_AVATAR_D, _AVATAR_D), PILImage.LANCZOS
                )
                return img
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(1)
    return None


def _circular_avatar(img: PILImage.Image, d: int) -> PILImage.Image:
    img = img.resize((d, d), PILImage.LANCZOS)
    mask = PILImage.new("L", (d, d), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, d - 1, d - 1), fill=255)
    out = PILImage.new("RGBA", (d, d), (0, 0, 0, 0))
    out.paste(img, mask=mask)
    return out


def _fetch_all_avatars(
    urls: set[str],
) -> dict[str, PILImage.Image | None]:
    results: dict[str, PILImage.Image | None] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_avatar, u): u for u in urls if u}
        for f in futures:
            img = f.result()
            if img:
                img = _circular_avatar(img, _AVATAR_D)
            results[futures[f]] = img
    return results


def _render_classic_canvas(
    results: list[dict],
    avatars: dict[str, PILImage.Image | None],
) -> PILImage.Image:
    n = len(results)
    best_cols = 1
    best_diff = float("inf")
    for c in range(1, n + 1):
        rows = math.ceil(n / c)
        w = 2 * _IMG_PAD + c * _CARD_W + (c - 1) * _CARD_GAP
        h = 2 * _IMG_PAD + rows * _CARD_H + (rows - 1) * _CARD_GAP
        diff = abs(w / h - 32 / 9)
        if diff < best_diff:
            best_diff = diff
            best_cols = c

    cols = best_cols
    total_rows = math.ceil(n / cols)
    tw = 2 * _IMG_PAD + cols * _CARD_W + (cols - 1) * _CARD_GAP
    th = 2 * _IMG_PAD + total_rows * _CARD_H + (total_rows - 1) * _CARD_GAP

    img = PILImage.new("RGBA", (tw, th), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    for i, r in enumerate(results):
        col = i % cols
        row = i // cols
        cx = _IMG_PAD + col * (_CARD_W + _CARD_GAP)
        cy = _IMG_PAD + row * (_CARD_H + _CARD_GAP)

        draw.rectangle(
            [cx, cy, cx + _CARD_W, cy + _CARD_H],
            fill=_CARD_BG,
            outline=_BORDER,
            width=_SCALE,
        )

        ax = cx + _CARD_PAD
        ay = cy + (_CARD_H - _AVATAR_D) // 2
        avatar_url = r.get("avatar_url", "")
        avatar_img = avatars.get(avatar_url) if avatar_url else None

        if avatar_img:
            img.paste(avatar_img, (ax, ay), avatar_img)
        else:
            draw.ellipse(
                [ax, ay, ax + _AVATAR_D, ay + _AVATAR_D],
                fill=_AVATAR_FB,
            )

        draw.ellipse(
            [ax, ay, ax + _AVATAR_D, ay + _AVATAR_D],
            outline=_BORDER,
            width=2 * _SCALE,
        )

        gx = cx + 2 * _CARD_PAD + _AVATAR_D
        gy = cy + _CARD_PAD
        colors = r.get("colors") or []

        for gr in range(_GRID_ROWS):
            for gc in range(_GRID_COLS):
                x = gx + gc * (_CELL + _CELL_GAP)
                y = gy + gr * (_CELL + _CELL_GAP)
                fill = _DEFAULT_CELL
                if gr < len(colors) and gc < len(colors[gr]):
                    hint = colors[gr][gc]
                    if hint in _COLOR_MAP:
                        fill = _COLOR_MAP[hint]
                draw.rectangle([x, y, x + _CELL, y + _CELL], fill=fill)

    return img


def _render_inferno_canvas(
    results: list[dict],
    avatars: dict[str, PILImage.Image | None],
    set_number: int | str,
) -> PILImage.Image:
    fl = _font(_INF_SUB_FS)
    ft = _font(16 * _SCALE)
    fsc = _font(_INF_SCORE_FS)

    dummy = PILImage.new("RGBA", (1, 1))
    dd = ImageDraw.Draw(dummy)

    label_h = dd.textbbox((0, 0), "Ag", font=fl)[3]
    stat_h = dd.textbbox((0, 0), "Ag", font=ft)[3]
    score_h = dd.textbbox((0, 0), "+100", font=fsc)[3]
    row_h = max(_INF_SQUARE_SIZE, score_h)

    squares_col_h = (
        _INF_ROUNDS * row_h + (_INF_ROUNDS - 1) * _INF_ROW_GAP
    )

    stats_content_h = (
        label_h
        + _INF_LINE_GAP
        + stat_h
        + _INF_SECTION_GAP
        + label_h
        + _INF_LINE_GAP
        + stat_h
    )
    text_content_h = max(squares_col_h, stats_content_h)
    card_h = max(text_content_h, _AVATAR_D) + 2 * _CARD_PAD

    score_label_w = dd.textbbox((0, 0), "+100", font=fsc)[2]
    squares_col_w = (
        _INF_SQUARE_SIZE + _INF_ROW_GAP * 2 + score_label_w
    )

    stat_gap = 12 * _SCALE
    pts_label_w = dd.textbbox((0, 0), "PTS", font=fl)[2]
    pts_val_w = dd.textbbox((0, 0), "500/500", font=ft)[2]
    time_label_w = dd.textbbox((0, 0), "TIME", font=fl)[2]
    time_val_w = dd.textbbox((0, 0), "0:00.0", font=ft)[2]
    stats_w = max(pts_label_w, pts_val_w, time_label_w, time_val_w)

    text_w = squares_col_w + stat_gap + stats_w
    card_w = 3 * _CARD_PAD + _AVATAR_D + text_w

    n = len(results)
    best_cols = 1
    best_diff = float("inf")
    for c in range(1, n + 1):
        rows = math.ceil(n / c)
        w = 2 * _IMG_PAD + c * card_w + (c - 1) * _CARD_GAP
        h = 2 * _IMG_PAD + rows * card_h + (rows - 1) * _CARD_GAP
        diff = abs(w / h - 32 / 9)
        if diff < best_diff:
            best_diff = diff
            best_cols = c

    cols = best_cols
    total_rows = math.ceil(n / cols)
    tw = 2 * _IMG_PAD + cols * card_w + (cols - 1) * _CARD_GAP
    th = (
        2 * _IMG_PAD
        + total_rows * card_h
        + (total_rows - 1) * _CARD_GAP
    )

    img = PILImage.new("RGBA", (tw, th), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    for i, r in enumerate(results):
        col = i % cols
        row = i // cols
        cx = _IMG_PAD + col * (card_w + _CARD_GAP)
        cy = _IMG_PAD + row * (card_h + _CARD_GAP)

        draw.rectangle(
            [cx, cy, cx + card_w, cy + card_h],
            fill=_CARD_BG,
            outline=_BORDER,
            width=_SCALE,
        )

        ax = cx + _CARD_PAD
        ay = cy + (card_h - _AVATAR_D) // 2
        avatar_url = r.get("avatar_url", "")
        avatar_img = avatars.get(avatar_url) if avatar_url else None

        if avatar_img:
            img.paste(avatar_img, (ax, ay), avatar_img)
        else:
            draw.ellipse(
                [ax, ay, ax + _AVATAR_D, ay + _AVATAR_D],
                fill=_AVATAR_FB,
            )
        draw.ellipse(
            [ax, ay, ax + _AVATAR_D, ay + _AVATAR_D],
            outline=_BORDER,
            width=2 * _SCALE,
        )

        tx = cx + 2 * _CARD_PAD + _AVATAR_D
        ty = cy + _CARD_PAD

        # Squares column
        scores = r.get("score_history") or []
        for j in range(_INF_ROUNDS):
            score = scores[j] if j < len(scores) else 0
            if score >= 100:
                color = _COLOR_MAP["GREEN"]
            elif score >= 60:
                color = _COLOR_MAP["YELLOW"]
            else:
                color = _COLOR_MAP["RED"]

            ry = ty + j * (row_h + _INF_ROW_GAP)
            sq_y = ry + (row_h - _INF_SQUARE_SIZE) // 2
            draw.rectangle(
                [
                    tx,
                    sq_y,
                    tx + _INF_SQUARE_SIZE,
                    sq_y + _INF_SQUARE_SIZE,
                ],
                fill=color,
            )
            score_x = tx + _INF_SQUARE_SIZE + _INF_ROW_GAP * 2
            score_ty = ry + (row_h - score_h) // 2
            draw.text(
                (score_x, score_ty),
                f"+{score}",
                font=fsc,
                fill="#FFFFFF",
            )

        # Stats column
        stats_x = tx + squares_col_w + stat_gap

        total_score = r.get("total_score", 0)
        draw.text(
            (stats_x, ty), "PTS", font=fl, fill="#888888"
        )
        draw.text(
            (stats_x, ty + label_h + _INF_LINE_GAP),
            f"{total_score}/500",
            font=ft,
            fill="#CCCCCC",
        )

        total_time = r.get("total_time_seconds", 0) or 0
        minutes = int(total_time) // 60
        seconds = total_time - minutes * 60
        time_y = (
            ty
            + label_h
            + _INF_LINE_GAP
            + stat_h
            + _INF_SECTION_GAP
        )
        draw.text(
            (stats_x, time_y), "TIME", font=fl, fill="#888888"
        )
        draw.text(
            (stats_x, time_y + label_h + _INF_LINE_GAP),
            f"{minutes}:{seconds:04.1f}",
            font=ft,
            fill="#CCCCCC",
        )

    return img


def _render_daily_image(
    classic_results: list[dict] | None,
    inferno_data: dict | None,
    avatars: dict[str, PILImage.Image | None],
) -> bytes:
    panels: list[PILImage.Image] = []

    if classic_results:
        panels.append(_render_classic_canvas(classic_results, avatars))
    if inferno_data:
        panels.append(
            _render_inferno_canvas(
                inferno_data["results"],
                avatars,
                inferno_data.get("set_number", "?"),
            )
        )

    if not panels:
        return b""

    gap = _CARD_GAP
    total_w = max(p.width for p in panels)
    total_h = sum(p.height for p in panels) + gap * (len(panels) - 1)

    img = PILImage.new("RGBA", (total_w, total_h), (0, 0, 0, 0))
    y = 0
    for p in panels:
        img.paste(p, (0, y))
        y += p.height + gap
        p.close()
    panels.clear()

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    img.close()
    return buf.getvalue()
