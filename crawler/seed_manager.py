# crawler/seed_manager.py

def add_seed_urls(redis_client, seed_urls):
    for url in seed_urls:
        redis_client.sadd("pending_urls", url)
    print("✅ 种子 URL 已存入 Redis")
