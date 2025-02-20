# crawler/db_connection.py
import redis

def get_redis_client(host="localhost", port=6379, decode_responses=True):
    redis_client = redis.StrictRedis(host=host, port=port, decode_responses=decode_responses)
    try:
        if redis_client.ping():
            print("✅ Redis 连接成功！")
    except redis.exceptions.ConnectionError:
        print("❌ 无法连接 Redis，请检查 Redis 是否在运行！")
    return redis_client
