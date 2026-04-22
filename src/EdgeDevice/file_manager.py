from pathlib import Path

# Base data directory (shared convention with file manager)
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
MODEL_DIR = DATA_DIR / "model"
TRAINING_DIR = DATA_DIR / "training_data"

async def clear_old_files(path: Path, file_id: str):
    for file in path.iterdir():
        if file.name != file_id:
            file.unlink()