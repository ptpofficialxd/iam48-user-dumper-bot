import requests
import signal
import os
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, BarColumn, TimeRemainingColumn, TimeElapsedColumn, TaskProgressColumn, MofNCompleteColumn, SpinnerColumn
from rich.console import Console
from supabase import create_client, Client

console = Console()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------- CONFIG ----------------
load_dotenv()
USER_DATA = os.getenv("USER_DATA")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
START_ID = int(os.getenv("START_ID"))
END_ID = int(os.getenv("END_ID"))
SAVE_EVERY = int(os.getenv("SAVE_EVERY"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 20))
# ----------------------------------------

# Setup session
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

batch = []

def fetch_user(user_id: int):
    url = USER_DATA.format(user_id)
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = session.get(url, headers=headers, timeout=5, verify=False)
        if r.status_code == 200:
            data = r.json()
            if "displayName" in data:
                return {"id": data["id"], "displayname": data["displayName"]}
    except Exception:
        return None
    return None

def save_batch(batch_to_save):
    if not batch_to_save:
        return
    try:
        supabase.table("users").upsert(batch_to_save).execute()
        console.print(f"[yellow][💾] Saved {len(batch_to_save)} users to Supabase[/yellow]")
    except Exception as e:
        console.print(f"[red][❌] Error saving batch: {e}[/red]")

def get_last_id():
    try:
        res = supabase.table("users").select("id").order("id", desc=True).limit(1).execute()
        if res.data and len(res.data) > 0:
            return res.data[0]["id"]
    except Exception as e:
        console.print(f"[red][❌] Error fetching last ID: {e}[/red]")
    return None

def handle_sigint(sig, frame):
    console.print("\n[red][⚠️] Stopped by user (Ctrl+C): Flushing remaining users...[/red]")
    save_batch(batch)
    console.print(f"[green][📂] Progress saved to Supabase[/green]")
    os._exit(0)

signal.signal(signal.SIGINT, handle_sigint)

def is_real_user(name: str) -> bool:
    if not name:
        return False
    return not name.startswith("แฟนคลับหมายเลข")

def main():
    # ✅ Resume จาก Supabase
    last_id = get_last_id()
    if last_id:
        start_id = last_id + 1
        console.print(f"[cyan][🔎] Resuming from ID {start_id} (last in Supabase: {last_id})[/cyan]")
    else:
        start_id = START_ID
        console.print(f"[cyan][🔎] No existing users found. Starting from ID {start_id}[/cyan]")

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
                    if user and is_real_user(user.get("displayname")):
                        console.print(f"[green][✅] Found: {user['id']} → {user['displayname']}[/green]")
                        batch.append(user)

                    if len(batch) >= SAVE_EVERY:
                        save_batch(batch)
                        batch.clear()

                    progress.update(task, advance=1)

    finally:
        save_batch(batch)
        console.print(f"[green][📂] Progress saved to Supabase[/green]")

if __name__ == "__main__":
    main()
