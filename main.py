import io
import os
import re
import time
from datetime import datetime, timezone, timedelta

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
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
REPORT_CHANNEL_ID = "1478290652172259462"

DISCORD_HEADERS = {
    "Authorization": f"Bot {BOT_TOKEN}",
    "Content-Type": "application/json",
    "User-Agent": "DiscordBot (https://ultrakidle.online, 1.0)",
}
SUPABASE_FUNCTIONS_URL = f"{SUPABASE_URL}/functions/v1"


_SCALE = 2
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

app = FastAPI()


def safe_json(res: requests.Response):
    try:
        return res.json()
    except Exception:
        print(
            f"[debug] Non-JSON response ({res.status_code}): {res.text[:200]}"
        )
        return None

# ── Avatar helpers ──


def _fetch_avatar(url: str, retries: int = 3) -> PILImage.Image | None:
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=10)
            if res.ok:
                return PILImage.open(io.BytesIO(res.content)).convert(
                    "RGBA"
                )
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(1)
    return None


def _fetch_all_avatars(
    urls: set[str],
) -> dict[str, PILImage.Image | None]:
    results: dict[str, PILImage.Image | None] = {}
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_fetch_avatar, u): u for u in urls if u}
        for f in futures:
            results[futures[f]] = f.result()
    return results


def _circular_avatar(img: PILImage.Image, d: int) -> PILImage.Image:
    img = img.resize((d, d), PILImage.LANCZOS)
    mask = PILImage.new("L", (d, d), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, d - 1, d - 1), fill=255)
    out = PILImage.new("RGBA", (d, d), (0, 0, 0, 0))
    out.paste(img, mask=mask)
    return out


# ── Image rendering ──


def _render_image(
    results: list[dict],
    avatars: dict[str, PILImage.Image | None],
) -> bytes:
    n = len(results)
    best_cols = 1
    best_diff = float("inf")
    for c in range(1, n + 1):
        rows = math.ceil(n / c)
        w = 2 * _IMG_PAD + c * _CARD_W + (c - 1) * _CARD_GAP
        h = 2 * _IMG_PAD + rows * _CARD_H + (rows - 1) * _CARD_GAP
        diff = abs(w / h - 16 / 9)
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
            circ = _circular_avatar(avatar_img, _AVATAR_D)
            img.paste(circ, (ax, ay), circ)
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

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# ── Message formatting ──


def _format_message(data: dict) -> str:
    grouped: dict[str, list[str]] = {}
    for r in data["results"]:
        key = f"{r['attempts']}/5" if r["is_win"] else "X/5"
        grouped.setdefault(key, []).append(r["name"])

    sorted_groups = sorted(
        grouped.items(),
        key=lambda kv: 99 if kv[0] == "X/5" else int(kv[0][0]),
    )

    best_key = sorted_groups[0][0]
    lines = []
    for key, names in sorted_groups:
        prefix = "👑 " if key == best_key and key != "X/5" else ""
        lines.append(f"{prefix}{key}: {'  '.join(names)}")

    streak = data.get("streak", 0)
    if streak == 1:
        streak_line = "The streak begins... 👀 "
    elif streak > 1:
        streak_line = f"This server is on a {streak} day streak! 🔥"
    else:
        streak_line = ""

    day = data.get("day_number", "?")
    return (
        f"🔴 ULTRAKIDLE #{day} has ended!\n{streak_line}\n"
        f"Here are yesterday's results:\n"
        + "\n".join(lines)
        + "\n\nA new enemy is waiting!"
    )


# ── Supabase + Edge function calls ──


def _rpc_guild_summary(guild_id: str) -> dict | None:
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    while True:
        try:
            res = sb.rpc(
                "get_guild_daily_summary", {"p_guild_id": guild_id}
            ).execute()
            return res.data
        except Exception as e:
            print(f"[{guild_id}] RPC error: {e}, retrying in 2s")
            time.sleep(2)


def _send_via_edge(
    channel_id: str, message: str, png_b64: str | None = None
) -> bool:
    payload: dict = {"channel_id": channel_id, "message": message}
    if png_b64:
        payload["png_base64"] = png_b64

    while True:
        try:
            res = requests.post(
                f"{SUPABASE_FUNCTIONS_URL}/send-daily-message",
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
            # 502 = edge function forwarded a non-retryable Discord error
            if res.status_code == 502:
                print(
                    f"[{channel_id}] Discord rejected: "
                    f"{data.get('error') if data else res.text[:200]}"
                )
                return False
            print(
                f"[{channel_id}] Edge error {res.status_code}, "
                "retrying in 3s"
            )
            time.sleep(3)
        except Exception as e:
            print(f"[{channel_id}] Request error: {e}, retrying in 5s")
            time.sleep(5)


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

    # Deduplicate guilds
    guild_channels: dict[str, list[str]] = {}
    for row in channels:
        guild_channels.setdefault(row["guild_id"], []).append(
            row["channel_id"]
        )

    print(
        f"[daily] {len(channels)} channels across "
        f"{len(guild_channels)} guilds"
    )

    # Fetch all guild summaries (concurrency-limited)
    summaries: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(_rpc_guild_summary, gid): gid
            for gid in guild_channels
        }
        for f in futures:
            gid = futures[f]
            data = f.result()
            if data and data.get("results"):
                summaries[gid] = data

    print(
        f"[daily] {len(summaries)} guilds with results "
        f"(skipped {len(guild_channels) - len(summaries)})"
    )

    if not summaries:
        print("[daily] Nothing to send")
        return

    # Collect unique avatar URLs and fetch them all
    avatar_urls: set[str] = set()
    for data in summaries.values():
        for r in data["results"]:
            url = r.get("avatar_url")
            if url:
                avatar_urls.add(url)

    print(f"[daily] Fetching {len(avatar_urls)} unique avatars")
    avatars = _fetch_all_avatars(avatar_urls)

    # Render images and format messages per guild
    guild_payloads: dict[str, tuple[str, str]] = {}  # gid -> (msg, png_b64)
    for gid, data in summaries.items():
        msg = _format_message(data)
        png = _render_image(data["results"], avatars)
        png_b64 = base64.b64encode(png).decode()
        guild_payloads[gid] = (msg, png_b64)

    print(f"[daily] Rendered {len(guild_payloads)} images, sending...")

    # Send sequentially
    succeeded = 0
    failed = 0
    skipped = len(guild_channels) - len(summaries)
    failures: list[str] = []

    for gid, ch_ids in guild_channels.items():
        if gid not in guild_payloads:
            continue

        msg, png_b64 = guild_payloads[gid]
        targets = (
            [test_channel] if test_channel else ch_ids
        )

        for ch_id in targets:
            ok = _send_via_edge(ch_id, msg, png_b64)
            if ok:
                succeeded += 1
            else:
                failed += 1
                failures.append(ch_id)

    elapsed = round(time.time() - started, 1)
    report = (
        f"[daily] Done in {elapsed}s — "
        f"{succeeded} sent, {skipped} skipped, {failed} failed"
    )
    print(report)
    if failures:
        print(f"[daily] Failed channels: {', '.join(failures)}")

    # Send report to report channel
    report_msg = (
        f"📊 **Daily notification report**\n"
        f"Sent: {succeeded} | Skipped: {skipped} | Failed: {failed}\n"
        f"Duration: {elapsed}s"
    )
    if failures:
        report_msg += f"\nFailed: {', '.join(failures)}"
    _send_via_edge(REPORT_CHANNEL_ID, report_msg)


# ── Endpoint ──


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


@app.get("/health")
def health():
    return {"ok": True}
