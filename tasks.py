import time
import base64
import gc
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client

from config import (
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY,
    REPORT_CHANNEL_ID,
    SUBMISSIONS_REPORT_CHANNEL_ID,
    SUBMISSIONS_BATCH_SIZE,
    DAILY_COMPONENTS,
)
from utils import _send_message, _call_edge, safe_json
from image_renderer import _fetch_all_avatars, _render_daily_image, _circular_avatar
from message_formatter import _format_header, _format_classic_section, _format_inferno_section


def _fetch_all_submissions(sb):
    """Fetches all records using RPC for speed, falls back to paginated select."""
    try:
        # Use the new RPC for efficiency
        res = sb.rpc("get_submission_stats", {}).execute()
        if res.data and "all_submissions" in res.data:
            return res.data["all_submissions"]
    except Exception as e:
        print(f"[debug] RPC failed, falling back to paginated select: {e}")

    all_data = []
    limit = 1000
    offset = 0
    while True:
        res = (
            sb.from_("image_submissions")
            .select("id, level_id, submitter_id, status")
            .range(offset, offset + limit - 1)
            .execute()
        )
        data = res.data or []
        all_data.extend(data)
        if len(data) < limit:
            break
        offset += limit
    return all_data


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

    # Get all existing profile IDs to refresh their data from Discord
    res = (
        sb.from_("submitter_profiles")
        .select("discord_user_id")
        .execute()
    )

    all_uids = [row["discord_user_id"] for row in (res.data or [])]

    if not all_uids:
        print("[refetch] No submitters found in records")
        return

    print(f"[refetch] Syncing {len(all_uids)} profiles from Discord API")

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
            uid = s["submitter_id"]
            user_counts[uid] = user_counts.get(uid, 0) + 1

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

        profile_ids = [uid for uid, _ in top_users]
        profiles = (
            sb.from_("submitter_profiles")
            .select("id, discord_name")
            .in_("id", profile_ids)
            .execute()
            .data
            or []
        )
        name_map = {
            p["id"]: p["discord_name"] for p in profiles
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
        uid = s["submitter_id"]
        user_counts[uid] = user_counts.get(uid, 0) + 1

    top_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    profile_ids = [uid for uid, _ in top_users]
    profiles = (
        sb.from_("submitter_profiles")
        .select("id, discord_name")
        .in_("id", profile_ids)
        .execute()
        .data
        or []
    )
    name_map = {p["id"]: p["discord_name"] for p in profiles}

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

    profile_ids = [uid for uid, _ in top_users]
    profiles = (
        sb.from_("submitter_profiles")
        .select("id, discord_name")
        .in_("id", profile_ids)
        .execute()
        .data
        or []
    )
    name_map = {p["id"]: p["discord_name"] for p in profiles}

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
