from datetime import datetime, timezone, timedelta

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
