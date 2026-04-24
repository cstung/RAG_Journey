import sys
from pathlib import Path

# Make `backend/` importable so tests can do `from utils...` and `from rag...`
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

