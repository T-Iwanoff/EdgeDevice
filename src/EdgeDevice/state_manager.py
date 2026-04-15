from pathlib import Path

# Base data directory (shared convention with file manager)
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
MODEL_DIR = DATA_DIR / "model"
TRAINING_DIR = DATA_DIR / "training_data"

def has_files(path: Path) -> bool:
    """
    Returns True if directory exists and contains at least one file.
    """
    return path.exists() and any(path.iterdir())

