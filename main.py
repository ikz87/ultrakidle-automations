import io
import os
import re
import time
import gc

from functools import lru_cache
from datetime import datetime, timezone, timedelta

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from PIL import ImageDraw, ImageFont
from PIL import Image as PILImage
from supabase import create_client
from dotenv import load_dotenv

import base64
import math
from concurrent.futures import ThreadPoolExecutor
from fastapi import BackgroundTasks, Query
from PIL import ImageDraw

load_dotenv()

BOT_TOKEN = os.environ["DISCORD_AUTOMATION_BOT_TOKEN"]
BOT_USER_ID = os.environ["DISCORD_AUTOMATION_BOT_ID"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
REACTION_THRESHOLD = 3
INGEST_BATCH_SIZE = 200
RESOLVE_BATCH_SIZE = 500
DISCORD_API = "https://discord.com/api/v10"
TARGET_WIDTH = 1920
TARGET_HEIGHT = 1080
ASPECT_TOLERANCE = 0.02
LEVEL_PATTERN = re.compile(
    r"^(\d+-\d+|\d+-[A-Z]\d*|P-\d+)$", re.IGNORECASE
)
REPORT_CHANNEL_ID = "1484843417736314932"

DISCORD_HEADERS = {
    "Authorization": f"Bot {BOT_TOKEN}",
    "Content-Type": "application/json",
    "User-Agent": "DiscordBot (https://ultrakidle.online, 1.0)",
}
SUPABASE_FUNCTIONS_URL = f"{SUPABASE_URL}/functions/v1"
SUBMISSIONS_BATCH_SIZE = 10
SUBMISSIONS_REPORT_CHANNEL_ID = "1481872631144775680"

DAILY_COMPONENTS = [
    {
        "type": 1,
        "components": [
            {
                "type": 2,
                "style": 2,
                "label": "Play on Discord",
                "custom_id": "launch_activity",
                "emoji": {"name": "🎮"},
            },
            {
                "type": 2,
                "style": 5,
                "label": "Open in browser",
                "url": "https://ultrakidle.online/",
                "emoji": {"name": "🌐"},
            },
        ],
    },
]


def _fetch_all_submissions(sb):
    """Paginates through all image_submissions records."""
    all_data = []
    limit = 1000
    offset = 0
    while True:
        res = (
            sb.from_("image_submissions")
            .select("id, level_id, discord_user_id, status")
            .range(offset, offset + limit - 1)
            .execute()
        )
        data = res.data or []
        all_data.extend(data)
        if len(data) < limit:
            break
        offset += limit
    return all_data

def _send_message(
    channel_id: str,
    message: str | None = None,
    *,
    bot: str = "main",
    components: list[dict] | None = None,
    attachments: list[dict] | None = None,
    max_retries: int = 5,
) -> bool:
    payload: dict = {"channel_id": channel_id, "bot": bot}
    if message:
        payload["message"] = message
    if components:
        payload["components"] = components
    if attachments:
        payload["attachments"] = attachments

    for attempt in range(max_retries):
        try:
            res = requests.post(
                f"{SUPABASE_FUNCTIONS_URL}/send-message",
                headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=120,
            )
            data = safe_json(res)
            if data and data.get("ok"):
                return True
            if res.status_code in (400, 401, 403, 404, 502):
                print(
                    f"[{channel_id}] Non-retryable error "
                    f"({res.status_code}): "
                    f"{data.get('error') if data else res.text[:200]}"
                )
                return False
            print(
                f"[{channel_id}] Edge error {res.status_code} "
                f"(attempt {attempt + 1}/{max_retries}), "
                "retrying in 3s"
            )
            time.sleep(3)
        except Exception as e:
            print(
                f"[{channel_id}] Request error "
                f"(attempt {attempt + 1}/{max_retries}): {e}, "
                "retrying in 5s"
            )
            time.sleep(5)

    print(f"[{channel_id}] Failed after {max_retries} attempts")
    return False


_SCALE = 1
_CELL = 16 * _SCALE
_CELL_GAP = 3 * _SCALE
_GRID_COLS = 6
_GRID_ROWS = 5
_GRID_W = _GRID_COLS * (_CELL + _CELL_GAP) - _CELL_GAP
_GRID_H = _GRID_ROWS * (_CELL + _CELL_GAP) - _CELL_GAP
_AVATAR_D = 48 * _SCALE
_CARD_PAD = 12 * _SCALE
_CARD_W = 3 * _CARD_PAD + _AVATAR_D + _GRID_W
_CARD_H = _GRID_H + 2 * _CARD_PAD
_CARD_GAP = 16 * _SCALE
_IMG_PAD = 24 * _SCALE
_COLOR_MAP = {"GREEN": "#00C950", "YELLOW": "#F0B100", "RED": "#FB2C36"}
_DEFAULT_CELL = "#3a3a3c"
_CARD_BG = "#0D0D0D"
_BORDER = "#ffffff"
_AVATAR_FB = "#444444"
FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts")
FONT_FILE = os.path.join(FONT_DIR, "font.ttf")

_INF_SQUARE_SIZE = 12 * _SCALE
_INF_ROW_GAP = 3 * _SCALE
_INF_LINE_GAP = 2 * _SCALE
_INF_SECTION_GAP = 8 * _SCALE
_INF_ROUNDS = 5

_INF_NAME_FS = 16 * _SCALE
_INF_SUB_FS = 11 * _SCALE
_INF_STAT_FS = 12 * _SCALE
_INF_SCORE_FS = 12 * _SCALE

app = FastAPI()


@lru_cache(maxsize=None)
def _font(size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(FONT_FILE, size)

def safe_json(res: requests.Response):
    try:
        return res.json()
    except Exception:
        print(
            f"[debug] Non-JSON response ({res.status_code}): {res.text[:200]}"
        )
        return None

# ── Avatar helpers ──

def _call_edge(
    fn_name: str,
    payload: dict | None = None,
    max_retries: int = 5,
) -> dict | None:
    for attempt in range(max_retries):
        try:
            res = requests.post(
                f"{SUPABASE_FUNCTIONS_URL}/{fn_name}",
                headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload or {},
                timeout=120,
            )
            data = safe_json(res)
            if res.ok and data:
                return data
            if res.status_code in (400, 401, 403, 404, 500):
                print(
                    f"[edge] {fn_name} returned {res.status_code}, "
                    f"aborting: {res.text[:200]}"
                )
                return None
            print(
                f"[edge] {fn_name} returned {res.status_code} "
                f"(attempt {attempt + 1}/{max_retries}), "
                "retrying in 3s"
            )
            time.sleep(3)
        except Exception as e:
            print(
                f"[edge] {fn_name} request error "
                f"(attempt {attempt + 1}/{max_retries}): {e}, "
                "retrying in 5s"
            )
            time.sleep(5)

    print(f"[edge] {fn_name} failed after {max_retries} attempts")
    return None


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


def _circular_avatar(img: PILImage.Image, d: int) -> PILImage.Image:
    img = img.resize((d, d), PILImage.LANCZOS)
    mask = PILImage.new("L", (d, d), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, d - 1, d - 1), fill=255)
    out = PILImage.new("RGBA", (d, d), (0, 0, 0, 0))
    out.paste(img, mask=mask)
    return out


# ── Image rendering ──


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

# ── Message formatting ──

def _format_header() -> str:
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    date_str = yesterday.strftime("%B %d, %Y")
    return (
        f"🔴 ULTRAKIDLE {date_str} has ended!\n"
        f"Here are yesterday's results:"
    )


def _format_classic_section(data: dict) -> str:
    rank_emojis = {
        "1/5": "<:prank:1485131026328977418>",
        "2/5": "<:srank:1485131053654872134>",
        "3/5": "<:arank:1485130880623312896>",
        "4/5": "<:brank:1485130946314502225>",
        "5/5": "<:crank:1485130972075655289>",
        "X/5": "<:drank:1485130996323057734>",
    }

    def _display(r: dict) -> str:
        if r.get("pings_opted_in") and r.get("discord_id"):
            return f"<@{r['discord_id']}>"
        return r["name"]

    grouped: dict[str, list[str]] = {}
    for r in data["results"]:
        key = f"{r['attempts']}/5" if r["is_win"] else "X/5"
        grouped.setdefault(key, []).append(_display(r))

    sorted_groups = sorted(
        grouped.items(),
        key=lambda kv: 99 if kv[0] == "X/5" else int(kv[0][0]),
    )

    best_key = sorted_groups[0][0]
    lines = []
    for key, names in sorted_groups:
        prefix = "👑 " if key == best_key and key != "X/5" else ""
        emoji = rank_emojis.get(key, "")
        lines.append(f"{emoji} {key}:\n> {' | '.join(names)}")

    streak = data.get("streak", 0)
    if streak == 1:
        streak_line = "\nThe streak begins... 👀"
    elif streak > 1:
        streak_line = f"\nThis server is on a {streak} day streak! 🔥"
    else:
        streak_line = ""

    day = data.get("day_number", "?")
    return (
        f"**Classic #{day}**{streak_line}\n" + "\n".join(lines)
    )


def _format_inferno_section(data: dict) -> str:
    results = data.get("results", [])
    if not results:
        return ""

    set_number = data.get("set_number", "?")

    tier_order = [
        ("P-rank", 500, "<:prank:1485131026328977418>"),
        ("S-rank", 400, "<:srank:1485131053654872134>"),
        ("A-rank", 300, "<:arank:1485130880623312896>"),
        ("B-rank", 200, "<:brank:1485130946314502225>"),
        ("C-rank", 100, "<:crank:1485130972075655289>"),
        ("D-rank", 0, "<:drank:1485130996323057734>"),
    ]

    def _display(r: dict) -> str:
        if r.get("pings_opted_in") and r.get("discord_id"):
            return f"<@{r['discord_id']}>"
        return r["name"]

    grouped: dict[str, list[str]] = {}
    for r in results:
        score = r.get("total_score", 0)
        tier = "D-rank"
        for name, threshold, _ in tier_order:
            if score >= threshold:
                tier = name
                break
        grouped.setdefault(tier, []).append(_display(r))

    best_tier = None
    lines = []
    for tier, threshold, emoji in tier_order:
        if tier in grouped:
            if best_tier is None:
                best_tier = tier
            prefix = "👑 " if tier == best_tier else ""
            names = " | ".join(grouped[tier])
            label = str(threshold) if threshold == 500 else f"{threshold}+"
            lines.append(f"{emoji} {label}:\n> {names}")

    return (
        f"**Infernoguessr #{set_number}**\n" + "\n".join(lines)
    )

# ── Supabase + Edge function calls ──

def _rpc_guild_combined_summary(guild_id: str) -> dict | None:
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    max_retries = 5
    for attempt in range(max_retries):
        try:
            res = sb.rpc(
                "get_guild_combined_summary",
                {"p_guild_id": guild_id},
            ).execute()
            return res.data
        except Exception as e:
            msg = str(e)
            if any(
                s in msg
                for s in ("Access denied", "P0001", "not a member")
            ):
                print(f"[{guild_id}] Permanent RPC error: {e}")
                return None
            print(
                f"[{guild_id}] RPC error "
                f"(attempt {attempt + 1}/{max_retries}): {e}, "
                "retrying in 2s"
            )
            time.sleep(2)

    print(f"[{guild_id}] RPC failed after {max_retries} attempts")
    return None


# ── Orchestrator ──

def _run_daily_notifications(
    filter_channel: str | None, test_channel: str | None
):
    started = time.time()
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    query = sb.from_("daily_notification_channels").select(
        "guild_id, channel_id"
    )
    if filter_channel:
        query = query.eq("channel_id", filter_channel)
    channels = query.execute().data or []

    if not channels:
        print("[daily] No channels found")
        return

    guild_channels: dict[str, list[str]] = {}
    for row in channels:
        guild_channels.setdefault(row["guild_id"], []).append(
            row["channel_id"]
        )

    guild_ids = list(guild_channels.keys())
    print(
        f"[daily] {len(channels)} channels across "
        f"{len(guild_ids)} guilds"
    )

    GUILD_BATCH = 5
    succeeded = 0
    failed = 0
    skipped = 0
    failures: list[str] = []
    classic_count = 0
    inferno_count = 0

    for bi in range(0, len(guild_ids), GUILD_BATCH):
        batch = guild_ids[bi : bi + GUILD_BATCH]
        batch_num = bi // GUILD_BATCH + 1
        print(
            f"[daily] Batch {batch_num} — {len(batch)} guilds"
        )

        classic_summaries: dict[str, dict] = {}
        inferno_summaries: dict[str, dict] = {}

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {
                pool.submit(_rpc_guild_combined_summary, gid): gid
                for gid in batch
            }

            for f in futures:
                gid = futures[f]
                data = f.result()
                if not data:
                    continue

                daily = data.get("daily")
                if daily and daily.get("results"):
                    classic_summaries[gid] = daily

                inferno = data.get("inferno")
                if (
                    inferno
                    and inferno.get("results")
                    and len(inferno["results"]) > 0
                ):
                    inferno_summaries[gid] = inferno

        active = set(classic_summaries.keys()) | set(
            inferno_summaries.keys()
        )
        skipped += len(batch) - len(active)
        classic_count += len(classic_summaries)
        inferno_count += len(inferno_summaries)

        if not active:
            continue

        avatar_urls: set[str] = set()
        for data in classic_summaries.values():
            for r in data["results"]:
                if url := r.get("avatar_url"):
                    avatar_urls.add(url)
        for data in inferno_summaries.values():
            for r in data["results"]:
                if url := r.get("avatar_url"):
                    avatar_urls.add(url)

        avatars = _fetch_all_avatars(avatar_urls)

        for gid in active:
            parts: list[str] = [_format_header()]

            if gid in classic_summaries:
                parts.append(
                    _format_classic_section(classic_summaries[gid])
                )
            if gid in inferno_summaries:
                parts.append(
                    _format_inferno_section(inferno_summaries[gid])
                )

            parts.append("New dailies are waiting!")
            msg = "\n\n".join(parts)

            png = _render_daily_image(
                classic_summaries.get(gid, {}).get("results"),
                inferno_summaries.get(gid),
                avatars,
            )

            attachments: list[dict] = []
            if png:
                attachments.append(
                    {
                        "base64": base64.b64encode(png).decode(),
                        "filename": "results.png",
                        "content_type": "image/png",
                    }
                )
                del png

            targets = (
                [test_channel]
                if test_channel
                else guild_channels.get(gid, [])
            )
            for ch_id in targets:
                ok = _send_message(
                    ch_id,
                    msg,
                    components=DAILY_COMPONENTS,
                    attachments=attachments,
                )
                if ok:
                    succeeded += 1
                else:
                    failed += 1
                    failures.append(ch_id)

            del attachments
            gc.collect()

        avatars.clear()
        del classic_summaries, inferno_summaries
        gc.collect()

    elapsed = round(time.time() - started, 1)
    report = (
        f"[daily] Done in {elapsed}s — "
        f"{succeeded} sent, {skipped} skipped, {failed} failed"
    )
    print(report)
    if failures:
        print(f"[daily] Failed channels: {', '.join(failures)}")

    report_msg = (
        f"📊 **Daily notification report**\n"
        f"Sent: {succeeded} | Skipped: {skipped} | "
        f"Failed: {failed}\n"
        f"Classic: {classic_count} guilds | "
        f"Inferno: {inferno_count} guilds\n"
        f"Duration: {elapsed}s"
    )
    if failures:
        report_msg += f"\nFailed: {', '.join(failures)}"
    _send_message(REPORT_CHANNEL_ID, report_msg, bot="automation")


def _run_refetch_submitters():
    started = time.time()
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    # Sync new submitters from image_submissions
    new_submitters = (
        sb.from_("image_submissions")
        .select("discord_user_id, discord_name, discord_avatar_url")
        .execute()
        .data
        or []
    )

    seen = set()
    for s in new_submitters:
        uid = s["discord_user_id"]
        if uid in seen:
            continue
        seen.add(uid)
        sb.from_("submitter_profiles").upsert(
            {
                "discord_user_id": uid,
                "discord_name": s["discord_name"],
                "discord_avatar_url": s.get("discord_avatar_url"),
            },
            on_conflict="discord_user_id",
            ignore_duplicates=True,
        ).execute()

    print(f"[refetch] Ensured {len(seen)} submitter profiles exist")

    profiles = (
        sb.from_("submitter_profiles")
        .select("discord_user_id")
        .execute()
        .data
        or []
    )

    if not profiles:
        print("[refetch] No profiles found")
        return

    all_uids = [row["discord_user_id"] for row in profiles]
    print(f"[refetch] {len(all_uids)} profiles to sync")

    BATCH_SIZE = 15
    updated = 0
    failed = 0
    failures: list[str] = []

    for i in range(0, len(all_uids), BATCH_SIZE):
        batch = all_uids[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(
            f"[refetch] Batch {batch_num} ({len(batch)} users)"
        )

        data = _call_edge(
            "refetch-submitters", {"user_ids": batch}
        )
        if not data or not data.get("results"):
            print(f"[refetch] Batch {batch_num} failed")
            failed += len(batch)
            failures.extend(batch)
            continue

        for r in data["results"]:
            uid = r["discord_user_id"]
            if r.get("error"):
                print(
                    f"[refetch] {uid} error: {r['error']}"
                )
                failed += 1
                failures.append(uid)
                continue

            try:
                print(
                    f"[refetch] {uid} "
                    f"name={r['discord_name']!r} "
                    f"avatar={'yes' if r['discord_avatar_url'] else 'no'}"
                )
                sb.from_("submitter_profiles").upsert(
                    {
                        "discord_user_id": uid,
                        "discord_name": r["discord_name"],
                        "discord_avatar_url": r["discord_avatar_url"],
                        "updated_at": datetime.now(
                            timezone.utc
                        ).isoformat(),
                    },
                    on_conflict="discord_user_id",
                ).execute()
                updated += 1
            except Exception as e:
                print(f"[refetch] Upsert error for {uid}: {e}")
                failed += 1
                failures.append(uid)

    elapsed = round(time.time() - started, 1)
    print(
        f"[refetch] Done in {elapsed}s — "
        f"{updated} updated, {failed} failed"
    )

    report_msg = (
        f"📊 **Submitter profile sync report**\n"
        f"Processed: {len(all_uids)} | Updated: {updated} | "
        f"Failed: {failed}\n"
        f"Duration: {elapsed}s"
    )
    if failures:
        report_msg += f"\nFailed: {', '.join(failures)}"
    _send_message(REPORT_CHANNEL_ID, report_msg, bot="automation")


def _run_poll_submissions(report_channel: str | None):
    started = time.time()
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    report_ch = report_channel or SUBMISSIONS_REPORT_CHANNEL_ID

    total_ingested = 0
    total_rejected_ingest = 0
    total_approved = 0
    total_rejected_resolve = 0
    total_expired = 0
    total_skipped = 0

    # ── Phase 1: Discover ──
    print("[submissions] Phase 1: Discovering threads")
    discover_data = _call_edge("submissions-discover")
    if not discover_data:
        print("[submissions] Discover failed, aborting")
        _send_message(report_ch, "❌ **Submissions poll failed** — discover error", bot="automation")
        return

    all_threads = discover_data.get("threads", [])
    print(f"[submissions] Discovered {len(all_threads)} new thread(s)")

    # ── Phase 2: Ingest in batches ──
    if all_threads:
        print("[submissions] Phase 2: Ingesting")
        for i in range(0, len(all_threads), SUBMISSIONS_BATCH_SIZE):
            batch = all_threads[i : i + SUBMISSIONS_BATCH_SIZE]
            batch_num = i // SUBMISSIONS_BATCH_SIZE + 1
            print(
                f"[submissions] Ingest batch {batch_num} "
                f"({len(batch)} threads)"
            )

            result = _call_edge(
                "submissions-ingest", {"threads": batch}
            )
            if result:
                total_ingested += result.get("ingested", 0)
                total_rejected_ingest += result.get("rejected", 0)
            else:
                print(
                    f"[submissions] Ingest batch {batch_num} failed"
                )
    else:
        print("[submissions] Nothing to ingest")

    # ── Phase 3: Resolve in batches ──
    print("[submissions] Phase 3: Resolving pending submissions")
    pending = (
        sb.from_("image_submissions")
        .select("*")
        .eq("status", "pending")
        .order("created_at", desc=False)
        .execute()
        .data
        or []
    )
    print(f"[submissions] {len(pending)} pending submission(s)")

    if pending:
        for i in range(0, len(pending), SUBMISSIONS_BATCH_SIZE):
            batch = pending[i : i + SUBMISSIONS_BATCH_SIZE]
            batch_num = i // SUBMISSIONS_BATCH_SIZE + 1
            print(
                f"[submissions] Resolve batch {batch_num} "
                f"({len(batch)} submissions)"
            )

            result = _call_edge(
                "submissions-resolve", {"submissions": batch}
            )
            if result:
                total_approved += result.get("approved", 0)
                total_rejected_resolve += result.get("rejected", 0)
                total_expired += result.get("expired", 0)
                total_skipped += result.get("skipped", 0)
            else:
                print(
                    f"[submissions] Resolve batch {batch_num} failed"
                )
    else:
        print("[submissions] Nothing to resolve")

    # ── Phase 4: Build summary report ──
    print("[submissions] Phase 4: Building report")

    all_subs = _fetch_all_submissions(sb)
    approved_subs = [s for s in all_subs if s["status"] == "approved"]

    levels = (
        sb.from_("levels")
        .select("id, level_number, level_name")
        .execute()
        .data
        or []
    )

    report_lines = [
        "📊 **Submissions Poll Report**",
        "",
        f"**Discovered:** {len(all_threads)}",
        f"**Ingested:** {total_ingested} | "
        f"**Rejected (ingest):** {total_rejected_ingest}",
        f"**Approved:** {total_approved} | "
        f"**Rejected (vote):** {total_rejected_resolve} | "
        f"**Expired:** {total_expired} | "
        f"**Skipped:** {total_skipped}",
    ]
    
    if approved_subs and levels:
        level_counts: dict[str, int] = {}
        user_counts: dict[str, int] = {}
        for s in approved_subs:
            level_counts[s["level_id"]] = (
                level_counts.get(s["level_id"], 0) + 1
            )
            user_counts[s["discord_user_id"]] = (
                user_counts.get(s["discord_user_id"], 0) + 1
            )

        level_stats = [
            {
                "number": l["level_number"],
                "name": l["level_name"],
                "count": level_counts.get(l["id"], 0),
            }
            for l in levels
        ]

        least_10 = sorted(level_stats, key=lambda x: x["count"])[:10]
        most_10 = sorted(
            level_stats, key=lambda x: x["count"], reverse=True
        )[:10]

        top_users = sorted(
            user_counts.items(), key=lambda x: x[1], reverse=True
        )[:5]

        # Resolve user display names from submitter_profiles
        user_ids = [uid for uid, _ in top_users]
        profiles = (
            sb.from_("submitter_profiles")
            .select("discord_user_id, discord_name")
            .in_("discord_user_id", user_ids)
            .execute()
            .data
            or []
        )
        name_map = {
            p["discord_user_id"]: p["discord_name"] for p in profiles
        }

        def fmt_levels(lst):
            return "\n".join(
                f"{i+1}. **{l['number']}** — {l['name']} ({l['count']})"
                for i, l in enumerate(lst)
            )

        def fmt_users(lst):
            return "\n".join(
                f"{i+1}. **{name_map.get(uid, uid)}** — "
                f"{count} approved submission{'s' if count != 1 else ''}"
                for i, (uid, count) in enumerate(lst)
            )

        report_lines += [
            "",
            f"**Total approved (all time):** {len(approved_subs)}",
            "",
            "📉 **Levels with fewest submissions:**",
            fmt_levels(least_10),
            "",
            "📈 **Levels with most submissions:**",
            fmt_levels(most_10),
            "",
            "🏆 **Top contributors:**",
            fmt_users(top_users),
        ]

    elapsed = round(time.time() - started, 1)
    report_lines.insert(1, f"Duration: {elapsed}s")

    report_msg = "\n".join(report_lines)
    print(f"[submissions] Done in {elapsed}s")
    _send_message(report_ch, report_msg, bot="automation")

def _send_stats_report(report_ch: str):
    """Internal helper to generate the stats report without a scan."""
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    all_subs = _fetch_all_submissions(sb)
    approved_subs = [s for s in all_subs if s["status"] == "approved"]

    levels = (
        sb.from_("levels")
        .select("id, level_number, level_name")
        .execute()
        .data
        or []
    )

    level_counts: dict[str, int] = {}
    user_counts: dict[str, int] = {}
    for s in approved_subs:
        level_counts[s["level_id"]] = level_counts.get(s["level_id"], 0) + 1
        user_counts[s["discord_user_id"]] = (
            user_counts.get(s["discord_user_id"], 0) + 1
        )

    level_stats = [
        {
            "number": l["level_number"],
            "name": l["level_name"],
            "count": level_counts.get(l["id"], 0),
        }
        for l in levels
    ]

    least_10 = sorted(level_stats, key=lambda x: x["count"])[:10]
    most_10 = sorted(level_stats, key=lambda x: x["count"], reverse=True)[:10]
    top_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    user_ids = [uid for uid, _ in top_users]
    profiles = (
        sb.from_("submitter_profiles")
        .select("discord_user_id, discord_name")
        .in_("discord_user_id", user_ids)
        .execute()
        .data
        or []
    )
    name_map = {p["discord_user_id"]: p["discord_name"] for p in profiles}

    def fmt_levels(lst):
        return "\n".join(
            f"{i+1}. **{l['number']}** — {l['name']} ({l['count']})"
            for i, l in enumerate(lst)
        )

    def fmt_users(lst):
        return "\n".join(
            f"{i+1}. **{name_map.get(uid, uid)}** — {count} approved"
            for i, (uid, count) in enumerate(lst)
        )

    report_msg = (
        "📊 **Current Submissions Stats**\n\n"
        f"**Total approved:** {len(approved_subs)}\n\n"
        "📉 **Levels with fewest submissions:**\n"
        f"{fmt_levels(least_10)}\n\n"
        "📈 **Levels with most submissions:**\n"
        f"{fmt_levels(most_10)}\n\n"
        "🏆 **Top contributors:**\n"
        f"{fmt_users(top_users)}"
    )
    _send_message(report_ch, report_msg, bot="automation")


@app.post("/test/submissions-report")
def test_submissions_report(
    request: Request,
    background_tasks: BackgroundTasks,
    report_channel: str | None = Query(None),
):
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {SUPABASE_SERVICE_ROLE_KEY}":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, 401)

    target = report_channel or REPORT_CHANNEL_ID
    background_tasks.add_task(_send_stats_report, target)
    return {"ok": True, "status": "report generation started"}

@app.post("/cron/refetch-submitters-data")
def refetch_submitters_data(
    request: Request,
    background_tasks: BackgroundTasks,
):
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {SUPABASE_SERVICE_ROLE_KEY}":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, 401)

    background_tasks.add_task(_run_refetch_submitters)
    return {"ok": True, "status": "started"}


@app.post("/cron/daily-notifications")
def daily_notifications(
    request: Request,
    background_tasks: BackgroundTasks,
    filter_channel: str | None = Query(
        None, description="Only process this channel's guild"
    ),
    test_channel: str | None = Query(
        None, description="Redirect ALL sends to this channel"
    ),
):
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {SUPABASE_SERVICE_ROLE_KEY}":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, 401)

    background_tasks.add_task(
        _run_daily_notifications, filter_channel, test_channel
    )
    return {"ok": True, "status": "started"}

@app.post("/cron/poll-submissions")
def poll_submissions(
    request: Request,
    background_tasks: BackgroundTasks,
    report_channel: str | None = Query(
        None, description="Override report channel ID"
    ),
):
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {SUPABASE_SERVICE_ROLE_KEY}":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, 401)

    background_tasks.add_task(_run_poll_submissions, report_channel)
    return {"ok": True, "status": "started"}

@app.post("/admin/force-approve-submission")
def force_approve_submission(
    request: Request,
    message_id: str = Query(..., description="Discord message ID of the submission"),
):
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {SUPABASE_SERVICE_ROLE_KEY}":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, 401)

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    row = (
        sb.from_("image_submissions")
        .select("*")
        .eq("message_id", message_id)
        .maybe_single()
        .execute()
    )

    if not row.data:
        return JSONResponse(
            {"ok": False, "error": "Submission not found"}, 404
        )

    submission = row.data

    result = _call_edge(
        "submissions-resolve",
        {"submissions": [submission], "force_approve": True},
    )

    if not result:
        return JSONResponse(
            {"ok": False, "error": "Resolve edge function failed"}, 502
        )

    return {
        "ok": True,
        "submission_id": submission["id"],
        "previous_status": submission["status"],
        "resolve_result": result,
    }


@app.get("/health")
def health():
    return {"ok": True}
