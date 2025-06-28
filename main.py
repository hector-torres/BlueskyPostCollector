# main.py
from app.post_ingestor import BlueskyIngest


def main():
    job = BlueskyIngest()
    job.run_forever(interval_minutes=5)


if __name__ == "__main__":
    main()