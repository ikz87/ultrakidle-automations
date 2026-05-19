import time
import requests
from config import SUPABASE_FUNCTIONS_URL, SUPABASE_SERVICE_ROLE_KEY

def safe_json(res: requests.Response):
    try:
        return res.json()
    except Exception:
        print(
            f"[debug] Non-JSON response ({res.status_code}): {res.text[:200]}"
        )
        return None


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
