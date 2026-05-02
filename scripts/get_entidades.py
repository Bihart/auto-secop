import aiohttp
import asyncio
import sqlite3
import logging
import sys
from contextlib import asynccontextmanager
from typing import List, Tuple, Optional, AsyncGenerator
from parsel import Selector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

URL = "https://operaciones.colombiacompra.gov.co/compradores/beneficios-del-secop-ii-para-compradores/consulte-en-el-secop-ii/entidades-secop-ii"
DB_PATH = "db.sqlite"
SLEEP_INTERVAL = 30
CONCURRENCY = 10
MAX_RETRIES = 3
RETRY_DELAY = 50

@asynccontextmanager
async def lifespan() -> AsyncGenerator[Tuple[aiohttp.ClientSession, sqlite3.Connection, asyncio.Lock], None]:
    """
    Manages the lifecycle of shared resources (Session and DB Connection).
    This acts as a singleton provider for the duration of the script.
    """
    logger.info("Initializing resources...")
    conn = sqlite3.connect(DB_PATH)
    lock = asyncio.Lock()

    conn.execute(
        "CREATE TABLE IF NOT EXISTS entity (nit TEXT, name TEXT, no_users INTEGER)"
    )
    conn.commit()

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        try:
            yield session, conn, lock
        finally:
            conn.close()
            logger.info("Resources closed.")

async def fetch_page(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    """Fetches the content of a single page."""
    try:
        async with session.get(url, timeout=30) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return None

def parse_data(raw_data: str) -> List[Tuple[str, str, int]]:
    """Parses entity data from the HTML content."""
    def casting(cells):
        nit = cells[0]
        name = cells[1]
        no_users_str = cells[2].replace(",", "").replace(".", "")
        no_users = int(no_users_str) if no_users_str.isdigit() else 0
        return (nit, name, no_users)

    selector = Selector(text=raw_data)
    data = []
    for row in selector.css("tr.odd, tr.even"):
        cells = [
            "".join(td.xpath(".//text()").getall()).strip()
            for td in row.xpath("./td")
        ]

        if len(cells) >= 3:
            try:
                data.append(casting(cells))
            except (ValueError, IndexError) as e:
                logger.warning(f"Skipping malformed row: {cells} - Error: {e}")

    return data

def store_data(conn: sqlite3.Connection, data: List[Tuple[str, str, int]]):
    """Stores the parsed data in the database using the shared connection."""
    if not data:
        return

    cursor = conn.cursor()
    cursor.executemany(
        "INSERT INTO entity (nit, name, no_users) VALUES (?, ?, ?)",
        data
    )
    conn.commit()
    logger.info(f"Stored {len(data)} records.")

async def process_page(session: aiohttp.ClientSession, conn: sqlite3.Connection, lock: asyncio.Lock, page_index: int) -> bool:
    """Fetches, parses, and stores data with retries. Returns True if data was found."""
    page_url = URL + (f"?page={page_index}" if page_index > 0 else "")

    for attempt in range(MAX_RETRIES):
        logger.info(f"Processing page {page_index} (Attempt {attempt + 1}/{MAX_RETRIES})...")
        raw_html = await fetch_page(session, page_url)

        if raw_html is not None:
            data = parse_data(raw_html)
            if not data:
                return False

            async with lock:
                store_data(conn, data)
            return True

        if attempt < MAX_RETRIES - 1:
            logger.warning(f"Failed to fetch page {page_index}. Retrying in {RETRY_DELAY}s...")
            await asyncio.sleep(RETRY_DELAY)

    logger.error(f"Max retries reached for page {page_index}.")
    return False

async def worker(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    lock: asyncio.Lock,
    state: dict,
    stop_event: asyncio.Event,
):
    """Worker task that fetches pages concurrently."""
    while not stop_event.is_set():
        async with state["lock"]:
            page_index = state["current_page"]
            state["current_page"] += 1

        success = await process_page(session, conn, lock, page_index)
        if not success:
            logger.info(f"No data on page {page_index}. Signaling stop.")
            stop_event.set()
            break

        await asyncio.sleep(SLEEP_INTERVAL)

async def main():
    # Get starting page index from CLI args
    try:
        start_page = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    except ValueError:
        logger.error("Invalid page index provided. Starting from 0.")
        start_page = 0

    async with lifespan() as (session, conn, lock):
        state = {
            "current_page": start_page,
            "lock": asyncio.Lock(),  # Lock for state['current_page']
        }
        stop_event = asyncio.Event()

        logger.info(f"Starting {CONCURRENCY} workers...")
        workers = [
            asyncio.create_task(worker(session, conn, lock, state, stop_event))
            for _ in range(CONCURRENCY)
        ]

        await asyncio.gather(*workers)
        logger.info("All workers finished.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scraper stopped by user.")
