from post_ingestor import PostCollector

def main():
    job = PostCollector()
    job.run()

if __name__ == "__main__":
    main()