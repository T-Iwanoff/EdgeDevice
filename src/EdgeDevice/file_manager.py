from pathlib import Path

# Base data directory (shared convention with file manager)
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
MODEL_DIR = DATA_DIR / "model"
TRAINING_DIR = DATA_DIR / "training_data"
PROGRAM_DIR = DATA_DIR / "program"
DEVICE_PATH = Path("device.json")

async def clear_old_files(path: Path, file_id: str):
    for file in path.iterdir():
        if file.name != file_id:
            file.unlink()

def has_files(path: Path) -> bool:
    """
    Returns True if the directory exists and contains at least one file.
    """
    if path.name == "program":
        return path.exists() and any(path.glob("*.py"))
    return path.exists() and any(path.iterdir())

