import os
import re
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
