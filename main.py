import requests
from fastapi import FastAPI, Request, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from supabase import create_client
from utils import get_r2_client
from image_renderer import apply_effects
from pydantic import BaseModel

# Re-exports for backwards compatibility with external scripts, tests, and task environments
from config import (
    BOT_TOKEN,
    BOT_USER_ID,
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY,
    REACTION_THRESHOLD,
    INGEST_BATCH_SIZE,
    RESOLVE_BATCH_SIZE,
    DISCORD_API,
    TARGET_WIDTH,
    TARGET_HEIGHT,
    ASPECT_TOLERANCE,
    LEVEL_PATTERN,
    REPORT_CHANNEL_ID,
    DISCORD_HEADERS,
    SUPABASE_FUNCTIONS_URL,
    SUBMISSIONS_BATCH_SIZE,
    SUBMISSIONS_REPORT_CHANNEL_ID,
    DAILY_COMPONENTS,
)
from utils import safe_json, _call_edge, _send_message
from image_renderer import (
    _font,
    _fetch_avatar,
    _fetch_all_avatars,
    _circular_avatar,
    _render_classic_canvas,
    _render_inferno_canvas,
    _render_daily_image,
)
from message_formatter import (
    _format_header,
    _format_classic_section,
    _format_inferno_section,
)
from tasks import (
    _fetch_all_submissions,
    _rpc_guild_combined_summary,
    _run_daily_notifications,
    _run_refetch_submitters,
    _run_poll_submissions,
    _send_stats_report,
)

app = FastAPI()

class CybergrindSetupRequest(BaseModel):
    source_path: str
    target_path: str
    effects: list[str] = []

@app.post("/admin/setup-cybergrind-image")
def setup_cybergrind_image(
    request: Request,
    data: CybergrindSetupRequest,
    background_tasks: BackgroundTasks,
):
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {SUPABASE_SERVICE_ROLE_KEY}":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, 401)

    background_tasks.add_task(_process_cybergrind_transfer, data)
    return {"ok": True}

def _process_cybergrind_transfer(data: CybergrindSetupRequest):
    import io
    from PIL import Image as PILImage
    from utils import get_r2_client
    # from image_renderer import apply_effects # Uncomment when you add apply_effects

    s3 = get_r2_client()
    try:
        # Download from level-images
        source_url = f"https://images.ultrakidle.online/{data.source_path}"
        res = requests.get(source_url, timeout=30)
        if not res.ok:
            return

        img = PILImage.open(io.BytesIO(res.content))
        
        # Apply effects (currently returns original if list is empty)
        # img = apply_effects(img, data.effects) 

        buf = io.BytesIO()
        ext = data.target_path.split(".")[-1].lower()
        fmt = "PNG" if ext == "png" else "JPEG"
        img.save(buf, format=fmt, optimize=True)
        buf.seek(0)

        s3.put_object(
            Bucket="inferno-cybergrind",
            Key=data.target_path,
            Body=buf,
            ContentType=f"image/{ext}",
        )
    except Exception as e:
        print(f"[cybergrind] Processing error: {e}")


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
    exclude_channel: str | None = Query(
        None, description="Skip this channel"
    ),
):
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {SUPABASE_SERVICE_ROLE_KEY}":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, 401)

    background_tasks.add_task(
        _run_daily_notifications, filter_channel, test_channel, exclude_channel
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

@app.post("/cron/cybergrind-cleanup")
def cybergrind_cleanup(
    request: Request,
    background_tasks: BackgroundTasks,
):
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {SUPABASE_SERVICE_ROLE_KEY}":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, 401)

    from tasks import _run_cybergrind_cleanup
    background_tasks.add_task(_run_cybergrind_cleanup)
    return {"ok": True, "status": "cleanup started"}


@app.get("/health")
def health():
    return {"ok": True}
