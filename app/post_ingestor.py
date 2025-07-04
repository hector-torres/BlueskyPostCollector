# bluesky_ingest.py
import os
import time
import dotenv
import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
from data.database import DatabaseClient

class BlueskyIngest:
    def __init__(self, accounts: list[str] | None = None, debug: bool | None = None):
        # Load configuration and set up logger
        self.cfg = self._load_config()
        if debug is not None:
            self.cfg["DEBUG"] = bool(debug)
        self.logger = self._setup_logger(self.cfg["DEBUG"])

        # Initialize DatabaseClient with schema only; path comes from env
        schema = {
            "uuid":         "string",
            "author":       "string",
            "display_name": "string",
            "title":        "string",
            "text":         "string",
            "timestamp":    "datetime64[ns]",
            "external_url": "string",
            "page_title":   "string",
            "meta_description": "string",
        }
        self.db = DatabaseClient(schema, self.logger)
        self.existing_uuids = self.db.get_existing_uuids()

        # Authenticate with Bluesky
        self.token = self._create_session()
        self.headers = {"Authorization": f"Bearer {self.token}"}

        # Load accounts from file or use provided list
        self.accounts = accounts or self._load_accounts_file()

    def run(self) -> None:
        # Refresh existing UUIDs to avoid duplicates across runs
        self.existing_uuids = self.db.get_existing_uuids()

        raw_df = self._collect_posts()
        # short-circuit if nothing was fetched (or uuid column missing)
        if raw_df.empty or "uuid" not in raw_df.columns:
            self.logger.info("No posts collected.")
            return

        new_df = raw_df[~raw_df["uuid"].isin(self.existing_uuids)]
        self.logger.info(f"Found {len(new_df)} new posts")

        enriched = self._enrich_dataframe(new_df)
        if enriched.empty:
            self.logger.info("No new posts to insert.")
            return

        self.db.append_dataframe(enriched)

    def run_forever(self, interval_minutes: int = 5) -> None:
        """
        Run self.run() in perpetuity, sleeping interval_minutes between runs.
        Press Ctrl-C to stop.
        """
        self.logger.info(f"Starting perpetual ingest every {interval_minutes} minutes.")
        try:
            while True:
                start = time.time()
                self.run()
                elapsed = time.time() - start
                to_sleep = interval_minutes * 60 - elapsed
                if to_sleep > 0:
                    self.logger.debug(f"Sleeping for {to_sleep:.1f}s before next run.")
                    time.sleep(to_sleep)
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal. Exiting ingest loop.")

    @staticmethod
    def _load_config() -> dict:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        dotenv.load_dotenv(os.path.join(base_dir, ".env"))
        return {
            "BASE_DIR": base_dir,
            "DATA_DIR": os.path.join(base_dir, "data"),
            "BLUESKY_USER": os.getenv("BLUESKY_HANDLE", ""),
            "BLUESKY_PW": os.getenv("BLUESKY_APP_PASSWORD", ""),
            "DEBUG": os.getenv("DEBUG", "0").lower() in ("1", "true", "yes"),
        }

    @staticmethod
    def _setup_logger(debug_enabled: bool) -> logging.Logger:
        level = logging.DEBUG if debug_enabled else logging.INFO
        logging.basicConfig(
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            level=level
        )
        # suppress verbose logs from HTTP libraries
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        return logging.getLogger("bluesky_ingest")

    def _create_session(self) -> str:
        resp = requests.post(
            "https://bsky.social/xrpc/com.atproto.server.createSession",
            json={"identifier": self.cfg["BLUESKY_USER"], "password": self.cfg["BLUESKY_PW"]},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["accessJwt"]

    def _fetch_author_feed(self, actor: str, limit: int = 10) -> dict:
        url = "https://bsky.social/xrpc/app.bsky.feed.getAuthorFeed"
        params = {"actor": actor, "limit": limit}
        while True:
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            if resp.status_code != 429:
                resp.raise_for_status()
                return resp.json()
            self.logger.warning("Rate-limited — sleeping 5s")
            time.sleep(5)

    def _load_accounts_file(self) -> list[str]:
        accounts_path = os.path.join(self.cfg["DATA_DIR"], "accounts.txt")
        if not os.path.isfile(accounts_path):
            self.logger.error("accounts.txt not found at %s", accounts_path)
            raise FileNotFoundError(f"Missing required file: {accounts_path}")
        with open(accounts_path, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        self.logger.debug("Loaded %d accounts from %s", len(lines), accounts_path)
        return lines

    def _collect_posts(self) -> pd.DataFrame:
        posts = []
        for acct in self.accounts:
            try:
                feed = self._fetch_author_feed(acct).get("feed", [])
                for item in feed:
                    post = item.get("post", {})
                    rec = post.get("record", {})
                    auth = post.get("author", {})
                    if rec.get("$type") != "app.bsky.feed.post":
                        continue
                    uuid = post.get("uri", "")
                    if uuid in self.existing_uuids:
                        continue
                    posts.append({
                        "uuid": uuid,
                        "author": acct,
                        "display_name": auth.get("displayName", ""),
                        "title": rec.get("embed", {}).get("external", {}).get("title", ""),
                        "text": rec.get("text", "").strip(),
                        "timestamp": rec.get("createdAt", ""),
                        "external_url": rec.get("embed", {}).get("external", {}).get("uri", ""),
                    })
                # self.logger.debug("%s: collected %s posts so far", acct, len(posts))
                time.sleep(5)
            except Exception as e:
                self.logger.error("Error fetching %s: %s", acct, e)
        return pd.DataFrame(posts).drop_duplicates(subset="uuid")

    def _enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        # parse timestamps
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df[["page_title", "meta_description"]] = df["external_url"].apply(
            lambda u: pd.Series(self._fetch_metadata(u))
        )
        return df

    @staticmethod
    def _fetch_metadata(url: str) -> tuple[str, str]:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            title = soup.title.string.strip() if soup.title else ""
            meta = soup.find("meta", attrs={"property": "og:description"}) or \
                   soup.find("meta", attrs={"name": "description"})
            desc = meta["content"].strip() if meta and "content" in meta.attrs else ""
            return title, desc
        except Exception:
            return "", ""