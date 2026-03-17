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
REPORT_CHANNEL_ID = "1481872631144775680"

DISCORD_HEADERS = {
    "Authorization": f"Bot {BOT_TOKEN}",
    "Content-Type": "application/json",
    "User-Agent": "DiscordBot (https://ultrakidle.online, 1.0)",
}

app = FastAPI()


def safe_json(res: requests.Response):
    try:
        return res.json()
    except Exception:
        print(
            f"[debug] Non-JSON response ({res.status_code}): {res.text[:200]}"
        )
        return None


def discord_fetch(
    url: str, method="GET", json=None
) -> requests.Response:
    for attempt in range(5):
        try:
            res = requests.request(
                method,
                url,
                headers=DISCORD_HEADERS,
                json=json,
                timeout=15,
            )
            if res.ok or res.status_code == 404:
                return res
            if res.status_code == 429:
                data = safe_json(res)
                retry_after = (
                    data.get("retry_after", 1) if data else 1
                )
                print(
                    f"[discord] 429, retrying in {retry_after}s"
                )
                time.sleep(retry_after)
                continue
            if res.status_code >= 500:
                backoff = 1 * (2**attempt)
                print(
                    f"[discord] {res.status_code} on {url}, "
                    f"retrying in {backoff}s"
                )
                time.sleep(backoff)
                continue
            return res
        except Exception as e:
            print(f"[discord] Request error on attempt {attempt}: {e}")
            time.sleep(1 * (2**attempt))
    raise RuntimeError(f"[discord] All retries exhausted for {url}")


def send_message(channel_id: str, content: str):
    res = discord_fetch(
        f"{DISCORD_API}/channels/{channel_id}/messages",
        method="POST",
        json={"content": content},
    )
    if not res.ok:
        print(
            f"[msg] Failed in {channel_id}: {res.status_code}"
        )


def close_thread(thread_id: str):
    res = discord_fetch(
        f"{DISCORD_API}/channels/{thread_id}",
        method="PATCH",
        json={"archived": True, "locked": True},
    )
    if not res.ok:
        print(
            f"[thread] Failed to close {thread_id}: "
            f"{res.status_code}"
        )


def add_reaction(
    channel_id: str, message_id: str, emoji: str
):
    encoded = requests.utils.quote(emoji, safe="")
    res = discord_fetch(
        f"{DISCORD_API}/channels/{channel_id}/messages/"
        f"{message_id}/reactions/{encoded}/@me",
        method="PUT",
    )
    if not res.ok:
        print(
            f"[react] Failed to add {emoji} to "
            f"{message_id}: {res.status_code}"
        )


def remove_reaction(
    channel_id: str, message_id: str, emoji: str
):
    encoded = requests.utils.quote(emoji, safe="")
    res = discord_fetch(
        f"{DISCORD_API}/channels/{channel_id}/messages/"
        f"{message_id}/reactions/{encoded}/@me",
        method="DELETE",
    )
    if not res.ok and res.status_code != 404:
        print(
            f"[react] Failed to remove {emoji} from "
            f"{message_id}: {res.status_code}"
        )


def reject_thread(
    thread_id: str, message_id: str, reason: str
):
    add_reaction(thread_id, message_id, "❌")
    send_message(
        thread_id, f"❌ **Submission rejected** — {reason}"
    )
    close_thread(thread_id)


def get_reaction_users(
    channel_id: str, message_id: str, emoji: str
) -> list[str]:
    encoded = requests.utils.quote(emoji, safe="")
    res = discord_fetch(
        f"{DISCORD_API}/channels/{channel_id}/messages/"
        f"{message_id}/reactions/{encoded}?limit=100"
    )
    data = safe_json(res)
    if not data or not isinstance(data, list):
        return []
    return [
        u["id"] for u in data if u.get("id") != BOT_USER_ID
    ]


def get_active_forum_threads(
    guild_id: str, forum_channel_id: str
) -> list:
    res = discord_fetch(
        f"{DISCORD_API}/guilds/{guild_id}/threads/active"
    )
    data = safe_json(res)
    if not data:
        return []
    return [
        t
        for t in data.get("threads", [])
        if t["parent_id"] == forum_channel_id
        and not t.get("thread_metadata", {}).get(
            "archived", False
        )
    ]


def get_starter_message(thread_id: str) -> dict | None:
    res = discord_fetch(
        f"{DISCORD_API}/channels/{thread_id}/messages/{thread_id}"
    )
    if not res.ok:
        return None
    return safe_json(res)


def download_image(url: str) -> bytes:
    res = requests.get(url, timeout=20)
    if not res.ok:
        raise RuntimeError(
            f"Failed to download image: {res.status_code}"
        )
    return res.content


def is_aspect_ratio_16x9(width: int, height: int) -> bool:
    return abs(width / height - 16 / 9) < ASPECT_TOLERANCE


def discord_avatar_url(
    user_id: str, avatar_hash: str | None
) -> str:
    if not avatar_hash:
        index = (int(user_id) >> 22) % 6
        return (
            f"https://cdn.discordapp.com/embed/avatars/"
            f"{index}.png"
        )
    ext = "gif" if avatar_hash.startswith("a_") else "png"
    return (
        f"https://cdn.discordapp.com/avatars/"
        f"{user_id}/{avatar_hash}.{ext}"
    )


def is_image_attachment(a: dict) -> bool:
    if (a.get("content_type") or "").startswith("image/"):
        return True
    return bool(
        re.search(
            r"\.(png|jpe?g|webp|gif)$",
            a.get("filename") or "",
            re.IGNORECASE,
        )
    )


@app.post("/run")
def run_poll(request: Request):
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {SUPABASE_SERVICE_ROLE_KEY}":
        return JSONResponse(
            {"error": "Unauthorized"}, status_code=401
        )

    start = time.time()
    print("[poll-submissions] Starting run")

    supabase = create_client(
        SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
    )

    levels_res = (
        supabase.table("levels")
        .select("id, level_number, level_name")
        .execute()
    )
    levels = levels_res.data
    if not levels:
        print("[init] No levels found")
        return JSONResponse(
            {"error": "No levels found"}, status_code=500
        )

    print(f"[init] Loaded {len(levels)} levels")
    level_map = {l["level_number"]: l["id"] for l in levels}

    forums_res = (
        supabase.table("submission_forums")
        .select("channel_id, guild_id")
        .execute()
    )
    forums = forums_res.data
    if not forums:
        print("[init] No tracked forums")
        return JSONResponse(
            {
                "ok": True,
                "ingested": 0,
                "resolved": 0,
                "stale": 0,
            }
        )

    print(f"[init] Tracking {len(forums)} forum(s)")

    total_ingested = 0
    total_resolved = 0
    total_approved_this_cycle = 0
    total_stale = 0

    # --- PHASE 1: Ingest ---
    for forum in forums:
        print(
            f"[ingest] Discovering threads in forum "
            f"{forum['channel_id']}"
        )
        active_threads = get_active_forum_threads(
            forum["guild_id"], forum["channel_id"]
        )
        print(
            f"[ingest] Found {len(active_threads)} active "
            f"thread(s)"
        )

        thread_ids = [t["id"] for t in active_threads]
        if not thread_ids:
            continue

        existing_res = (
            supabase.table("image_submissions")
            .select("message_id")
            .in_("message_id", thread_ids)
            .execute()
        )
        rejected_res = (
            supabase.table("rejected_threads")
            .select("thread_id")
            .in_("thread_id", thread_ids)
            .execute()
        )

        existing_ids = {
            e["message_id"]
            for e in (existing_res.data or [])
        }
        rejected_ids = {
            r["thread_id"]
            for r in (rejected_res.data or [])
        }

        new_threads = [
            t
            for t in active_threads
            if t["id"] not in existing_ids
            and t["id"] not in rejected_ids
        ][:INGEST_BATCH_SIZE]

        print(
            f"[ingest] {len(new_threads)} new thread(s) to "
            f"process"
        )

        for thread in new_threads:
            try:
                time.sleep(1)
                title = (thread.get("name") or "").strip()
                match = LEVEL_PATTERN.match(title)
                if not match:
                    print(
                        f"[ingest] Rejecting {thread['id']} "
                        f'— invalid title: "{title}"'
                    )
                    reject_thread(
                        thread["id"],
                        thread["id"],
                        f"Post title must be exactly a level "
                        f"name (e.g. `2-1`, `P-2`, `0-E`, "
                        f"`7-S`). Got: `{title}`",
                    )
                    supabase.table(
                        "rejected_threads"
                    ).insert(
                        {"thread_id": thread["id"]}
                    ).execute()
                    continue

                level_number = match.group(1).upper()
                level_id = level_map.get(level_number)
                if not level_id:
                    print(
                        f"[ingest] Rejecting {thread['id']} "
                        f'— unknown level "{level_number}"'
                    )
                    reject_thread(
                        thread["id"],
                        thread["id"],
                        f"Level `{level_number}` was not "
                        f"found in the database.",
                    )
                    supabase.table(
                        "rejected_threads"
                    ).insert(
                        {"thread_id": thread["id"]}
                    ).execute()
                    continue

                msg = get_starter_message(thread["id"])
                if not msg:
                    print(
                        f"[ingest] Closing {thread['id']} "
                        f"— no starter message"
                    )
                    close_thread(thread["id"])
                    continue

                image_attachments = [
                    a
                    for a in (msg.get("attachments") or [])
                    if is_image_attachment(a)
                ]
                if len(image_attachments) == 0:
                    print(
                        f"[ingest] Rejecting {thread['id']} "
                        f"— no image"
                    )
                    reject_thread(
                        thread["id"],
                        thread["id"],
                        "The first message must contain "
                        "exactly one image attachment.",
                    )
                    supabase.table(
                        "rejected_threads"
                    ).insert(
                        {"thread_id": thread["id"]}
                    ).execute()
                    continue
                if len(image_attachments) > 1:
                    print(
                        f"[ingest] Rejecting {thread['id']} "
                        f"— {len(image_attachments)} images"
                    )
                    reject_thread(
                        thread["id"],
                        thread["id"],
                        f"The first message must contain "
                        f"exactly one image. Found "
                        f"{len(image_attachments)}.",
                    )
                    supabase.table(
                        "rejected_threads"
                    ).insert(
                        {"thread_id": thread["id"]}
                    ).execute()
                    continue

                attachment = image_attachments[0]
                image_data = download_image(
                    attachment["url"]
                )
                img = PILImage.open(io.BytesIO(image_data))
                if not is_aspect_ratio_16x9(
                    img.width, img.height
                ):
                    print(
                        f"[ingest] Rejecting {thread['id']} "
                        f"— not 16:9 "
                        f"({img.width}x{img.height})"
                    )
                    reject_thread(
                        thread["id"],
                        thread["id"],
                        f"Image must be 16:9 aspect ratio. "
                        f"Got {img.width}×{img.height}.",
                    )
                    supabase.table(
                        "rejected_threads"
                    ).insert(
                        {"thread_id": thread["id"]}
                    ).execute()
                    continue

                author = msg["author"]
                display_name = (
                    author.get("global_name")
                    or author["username"]
                )

                supabase.table("image_submissions").insert(
                    {
                        "guild_id": forum["guild_id"],
                        "channel_id": thread["id"],
                        "message_id": thread["id"],
                        "discord_user_id": author["id"],
                        "discord_name": display_name,
                        "discord_avatar_url": discord_avatar_url(
                            author["id"],
                            author.get("avatar"),
                        ),
                        "level_id": level_id,
                        "image_url": attachment["url"],
                    }
                ).execute()

                print(
                    f"[ingest] ✓ Ingested {thread['id']} "
                    f"— level {level_number} by "
                    f"{display_name}"
                )
                add_reaction(
                    thread["id"], thread["id"], "👀"
                )
                total_ingested += 1

            except Exception as e:
                print(
                    f"[ingest] Unexpected error for thread "
                    f"{thread['id']}: {e}"
                )
                try:
                    close_thread(thread["id"])
                except Exception:
                    pass

    # --- PHASE 2: Resolve ---
    pending_res = (
        supabase.table("image_submissions")
        .select("*")
        .eq("status", "pending")
        .order("created_at", desc=False)
        .limit(RESOLVE_BATCH_SIZE)
        .execute()
    )
    pending = pending_res.data or []
    print(
        f"[resolve] {len(pending)} pending submission(s) "
        f"to check"
    )

    for sub in pending:
        try:
            time.sleep(1)
            thread_res = discord_fetch(
                f"{DISCORD_API}/channels/{sub['channel_id']}"
            )
            if not thread_res.ok:
                print(
                    f"[resolve] Failed thread fetch "
                    f"{sub['channel_id']}: "
                    f"{thread_res.status_code}"
                )
                continue

            thread_data = safe_json(thread_res)
            if not thread_data:
                continue

            current_title = (
                (thread_data.get("name") or "")
                .strip()
                .upper()
            )
            level_match = LEVEL_PATTERN.match(current_title)
            level_id = (
                level_map.get(level_match.group(1))
                if level_match
                else None
            )

            if not level_id:
                print(
                    f"[resolve] Rejecting #{sub['id']} - "
                    f"title edited to invalid"
                )
                reject_thread(
                    sub["channel_id"],
                    sub["message_id"],
                    f"Title edited to invalid level: "
                    f'"{current_title}"',
                )
                supabase.table("image_submissions").update(
                    {
                        "status": "rejected",
                        "resolved_at": datetime.now(
                            timezone.utc
                        ).isoformat(),
                    }
                ).eq("id", sub["id"]).execute()
                total_resolved += 1
                continue

            if level_id != sub["level_id"]:
                supabase.table("image_submissions").update(
                    {"level_id": level_id}
                ).eq("id", sub["id"]).execute()
                sub["level_id"] = level_id

            up_users = get_reaction_users(
                sub["channel_id"], sub["message_id"], "👍"
            )
            down_users = get_reaction_users(
                sub["channel_id"], sub["message_id"], "👎"
            )
            up, down = len(up_users), len(down_users)

            supabase.table("image_submissions").update(
                {"thumbs_up": up, "thumbs_down": down}
            ).eq("id", sub["id"]).execute()

            if (
                up < REACTION_THRESHOLD
                and down < REACTION_THRESHOLD
            ):
                print(
                    f"[resolve] #{sub['id']} — {up}👍 "
                    f"{down}👎 (need "
                    f"{REACTION_THRESHOLD}), skipping"
                )
                continue

            if up > down:
                fresh_msg = get_starter_message(
                    sub["channel_id"]
                )
                fresh_attachment = next(
                    (
                        a
                        for a in (
                            fresh_msg.get("attachments")
                            or []
                        )
                        if is_image_attachment(a)
                    ),
                    None,
                )
                if not fresh_attachment:
                    print(
                        f"[resolve] #{sub['id']} — image "
                        f"no longer available"
                    )
                    reject_thread(
                        sub["channel_id"],
                        sub["message_id"],
                        "Original image is no longer "
                        "available.",
                    )
                    supabase.table(
                        "image_submissions"
                    ).update(
                        {
                            "status": "rejected",
                            "resolved_at": datetime.now(
                                timezone.utc
                            ).isoformat(),
                        }
                    ).eq(
                        "id", sub["id"]
                    ).execute()
                    total_resolved += 1
                    continue

                image_data = download_image(
                    fresh_attachment["url"]
                )
                img = PILImage.open(
                    io.BytesIO(image_data)
                ).convert("RGB")
                resized = img.resize(
                    (TARGET_WIDTH, TARGET_HEIGHT),
                    PILImage.LANCZOS,
                )
                buf = io.BytesIO()
                resized.save(
                    buf, format="JPEG", quality=90
                )
                jpeg_data = buf.getvalue()

                level_number = next(
                    (
                        l["level_number"]
                        for l in levels
                        if l["id"] == sub["level_id"]
                    ),
                    None,
                )
                storage_path = (
                    f"{level_number}/{sub['id']}.jpg"
                )

                supabase.storage.from_(
                    "level-images"
                ).upload(
                    storage_path,
                    jpeg_data,
                    {
                        "content-type": "image/jpeg",
                        "upsert": "false",
                    },
                )

                remove_reaction(
                    sub["channel_id"],
                    sub["message_id"],
                    "👀",
                )
                add_reaction(
                    sub["channel_id"],
                    sub["message_id"],
                    "✅",
                )
                send_message(
                    sub["channel_id"],
                    "✅ **Submission approved!** "
                    "Added to gallery.",
                )
                close_thread(sub["channel_id"])

                supabase.table("image_submissions").update(
                    {
                        "status": "approved",
                        "storage_path": storage_path,
                        "resolved_at": datetime.now(
                            timezone.utc
                        ).isoformat(),
                    }
                ).eq("id", sub["id"]).execute()

                total_resolved += 1
                total_approved_this_cycle += 1
                print(
                    f"[resolve] ✓ Approved #{sub['id']} "
                    f"— {up}👍 {down}👎"
                )

            else:
                supabase.table("image_submissions").update(
                    {
                        "status": "rejected",
                        "resolved_at": datetime.now(
                            timezone.utc
                        ).isoformat(),
                    }
                ).eq("id", sub["id"]).execute()
                remove_reaction(
                    sub["channel_id"],
                    sub["message_id"],
                    "👀",
                )
                add_reaction(
                    sub["channel_id"],
                    sub["message_id"],
                    "❌",
                )
                send_message(
                    sub["channel_id"],
                    "❌ **Submission rejected** by vote.",
                )
                close_thread(sub["channel_id"])
                total_resolved += 1
                print(
                    f"[resolve] ✗ Rejected #{sub['id']} "
                    f"— {up}👍 {down}👎"
                )

        except Exception as e:
            print(
                f"[resolve] Unexpected error for "
                f"#{sub['id']}: {e}"
            )
            try:
                supabase.table("image_submissions").update(
                    {
                        "status": "rejected",
                        "resolved_at": datetime.now(
                            timezone.utc
                        ).isoformat(),
                    }
                ).eq("id", sub["id"]).execute()
                close_thread(sub["channel_id"])
            except Exception:
                pass
            total_resolved += 1

    # --- PHASE 2.5: Stale ---
    two_days_ago = (
        datetime.now(timezone.utc) - timedelta(days=2)
    ).isoformat()
    stale_res = (
        supabase.table("image_submissions")
        .select("*")
        .eq("status", "pending")
        .lt("created_at", two_days_ago)
        .order("created_at", desc=False)
        .execute()
    )
    stale = stale_res.data or []
    print(
        f"[stale] {len(stale)} stale submission(s) to "
        f"archive"
    )

    for sub in stale:
        try:
            time.sleep(1)
            remove_reaction(
                sub["channel_id"], sub["message_id"], "👀"
            )
            send_message(
                sub["channel_id"],
                "⏰ **Submission expired** — This thread "
                "has been open for over 2 days without "
                "reaching the vote threshold. Feel free "
                "to resubmit!",
            )
            close_thread(sub["channel_id"])
            supabase.table("image_submissions").update(
                {
                    "status": "expired",
                    "resolved_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                }
            ).eq("id", sub["id"]).execute()
            print(f"[stale] ✓ Archived #{sub['id']}")
            total_stale += 1
            total_resolved += 1
        except Exception as e:
            print(f"[stale] Error for #{sub['id']}: {e}")

    # --- PHASE 3: Summary ---
    approved_res = (
        supabase.table("image_submissions")
        .select("id, level_id, discord_user_id")
        .eq("status", "approved")
        .execute()
    )
    approved_subs = approved_res.data or []

    pending_count_res = (
        supabase.table("image_submissions")
        .select("id", count="exact")
        .eq("status", "pending")
        .execute()
    )
    pending_count = pending_count_res.count or 0

    if approved_subs:
        total_approved = len(approved_subs)
        level_counts: dict[str, int] = {}
        user_counts: dict[str, int] = {}

        for s in approved_subs:
            level_counts[s["level_id"]] = (
                level_counts.get(s["level_id"], 0) + 1
            )
            user_counts[s["discord_user_id"]] = (
                user_counts.get(s["discord_user_id"], 0)
                + 1
            )

        level_stats = [
            {
                "level_number": l["level_number"],
                "name": l["level_name"],
                "count": level_counts.get(l["id"], 0),
            }
            for l in levels
        ]
        sorted_asc = sorted(
            level_stats, key=lambda x: x["count"]
        )
        sorted_desc = sorted(
            level_stats,
            key=lambda x: x["count"],
            reverse=True,
        )
        least10 = sorted_asc[:10]
        most10 = sorted_desc[:10]

        top_users = sorted(
            user_counts.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:5]
        user_names: dict[str, str] = {}
        for user_id, _ in top_users:
            try:
                res = discord_fetch(
                    f"{DISCORD_API}/users/{user_id}"
                )
                if res.ok:
                    u = safe_json(res)
                    if u:
                        user_names[user_id] = (
                            u.get("global_name")
                            or u["username"]
                        )
                    else:
                        user_names[user_id] = (
                            f"Unknown User ({user_id})"
                        )
                else:
                    user_names[user_id] = (
                        f"Unknown User ({user_id})"
                    )
            except Exception:
                user_names[user_id] = (
                    f"Unknown User ({user_id})"
                )

        def fmt(lst):
            return "\n".join(
                f"{i + 1}. **{l['level_number']}** — "
                f"{l['name']} ({l['count']})"
                for i, l in enumerate(lst)
            )

        fmt_users = "\n".join(
            f"{i + 1}. **{user_names.get(uid)}** — "
            f"{count} approved submission"
            f"{'s' if count != 1 else ''}"
            for i, (uid, count) in enumerate(top_users)
        )

        summary = "\n".join(
            [
                "📊 **Submission Summary**",
                "",
                f"**Approved this cycle:** "
                f"{total_approved_this_cycle}",
                f"**Total approved:** {total_approved}",
                f"**Expired this cycle:** {total_stale}",
                f"**Currently pending:** {pending_count}",
                "",
                "📉 **Levels with fewest submissions:**",
                fmt(least10),
                "",
                "📈 **Levels with most submissions:**",
                fmt(most10),
                "",
                "🏆 **Top contributors:**",
                fmt_users,
            ]
        )
        send_message(REPORT_CHANNEL_ID, summary)
    else:
        send_message(
            REPORT_CHANNEL_ID,
            f"📊 **Submission Summary** — No approved "
            f"submissions yet.\n"
            f"**Expired this cycle:** {total_stale}\n"
            f"**Currently pending:** {pending_count}",
        )

    elapsed = round((time.time() - start) * 1000)
    print(
        f"[poll-submissions] Done in {elapsed}ms — "
        f"ingested: {total_ingested}, "
        f"resolved: {total_resolved}, "
        f"stale: {total_stale}"
    )

    return JSONResponse(
        {
            "ok": True,
            "ingested": total_ingested,
            "resolved": total_resolved,
            "stale": total_stale,
        }
    )


@app.get("/health")
def health():
    return {"ok": True}
