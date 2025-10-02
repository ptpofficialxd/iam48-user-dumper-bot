import requests
import csv
import signal
import sys
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, BarColumn, TimeRemainingColumn, TimeElapsedColumn, TaskProgressColumn, MofNCompleteColumn, SpinnerColumn
from rich.console import Console

console = Console()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------- CONFIG ----------------
load_dotenv()
USER_DATA = os.getenv("USER_DATA")
OUTPUT_FILE = Path(os.getenv("OUTPUT_FILE"))
START_ID = int(os.getenv("START_ID"))
END_ID = int(os.getenv("END_ID"))
SAVE_EVERY = int(os.getenv("SAVE_EVERY"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS"))
# ----------------------------------------

# Setup session
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

batch = []

def fetch_user(user_id: int):
    """Fetch user profile by ID"""
    url = USER_DATA.format(user_id)
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = session.get(url, headers=headers, timeout=5, verify=False)
        if r.status_code == 200:
            data = r.json()
            if "displayName" in data:
                return {"id": data["id"], "displayName": data["displayName"]}
    except Exception:
        return None
    return None

def load_cache():
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [{"id": int(row["id"]), "displayName": row["displayName"]} for row in reader]
    return []

def save_cache_append(batch_to_save):
    if not batch_to_save:
        return
    file_exists = OUTPUT_FILE.exists()
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "displayName"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(batch_to_save)
    console.print(f"[yellow][💾] Auto-saved {len(batch_to_save)} iAM48 users (Total: {len(load_cache())})[/yellow]")

def handle_sigint(sig, frame):
    console.print("\n[red][⚠️] Stopped by user (Ctrl+C): Flushing remaining iAM48 users...[/red]")
    save_cache_append(batch)
    console.print(f"[green][📂] Progress saved: {len(load_cache())} iAM48 users in database[/green]")
    os._exit(0)

signal.signal(signal.SIGINT, handle_sigint)

def is_real_user(name: str) -> bool:
    if not name:
        return False
    return not name.startswith("แฟนคลับหมายเลข")

def main():
    cache = load_cache()
    if cache:
        last_id = max(u["id"] for u in cache)
        start_id = last_id + 1
        console.print(f"[cyan][🔎] Loaded {len(cache)} iAM48 users. Resuming from ID {start_id}[/cyan]")
    else:
        start_id = START_ID
        console.print(f"[cyan][🔎] No cache found. Starting from ID {start_id}[/cyan]")

    try:
        with Progress(
            SpinnerColumn(),
            "[bold green][🔄] Fetching",
            BarColumn(bar_width=None, complete_style="green"),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("", total=END_ID - start_id + 1)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(fetch_user, uid): uid for uid in range(start_id, END_ID + 1)}

                for count, future in enumerate(as_completed(futures), start=1):
                    user = future.result()
                    if user and is_real_user(user.get("displayName")):
                        console.print(f"[green][✅] Found: {user['id']} → {user['displayName']}[/green]")
                        batch.append(user)

                    if len(batch) >= SAVE_EVERY:
                        save_cache_append(batch)
                        batch.clear()

                    progress.update(task, advance=1)

    finally:
        save_cache_append(batch)
        console.print(f"[green][📂] Progress saved: {len(load_cache())} iAM48 users in database[/green]")

if __name__ == "__main__":
    main()
