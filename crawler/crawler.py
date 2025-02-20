# crawler/crawler.py
import time
import requests
from bs4 import BeautifulSoup
from crawler.robots import can_crawl
from crawler.db_connection import get_redis_client

# 初始化 Redis 客户端
redis_client = get_redis_client()

# 伪装成真实浏览器的请求头
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

# 定义爬取最大深度，避免无限递归
MAX_DEPTH = 3

def crawl(depth=0):
    if depth > MAX_DEPTH:
        print("⛔ 爬取深度超限，停止递归")
        return

    # 从 Redis 中取出一个待爬取 URL
    url_to_crawl = redis_client.spop("pending_urls")
    if not url_to_crawl:
        print("🚀 没有新的 URL 需要爬取")
        return

    # 在请求前先检查 robots.txt 是否允许爬取
    if not can_crawl(url_to_crawl, "MyCrawler"):
        print(f"❌ {url_to_crawl} 不允许爬取 (robots 协议禁止)")
        return

    print(f"📌 爬取: {url_to_crawl} (深度: {depth})")

    try:
        response = requests.get(url_to_crawl, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            # 提取页面中所有链接
            for link in soup.find_all("a", href=True):
                new_url = link["href"]
                # 如果允许爬取，并且新 URL 是我们关注的页面，就加入 Redis（自动去重）
                if can_crawl(new_url, "MyCrawler") and new_url.startswith("https://www.douban.com/group/topic/"):
                    redis_client.sadd("pending_urls", new_url)
                    print(f"➕ 发现新 URL: {new_url}")
            print(f"✅ 爬取完成: {url_to_crawl}")
        else:
            print(f"❌ 访问失败，状态码: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ 爬取出错: {e}")

    # 控制爬取速度，防止被封 IP
    time.sleep(3)
    # 递归调用，爬取下一层
    crawl(depth + 1)
