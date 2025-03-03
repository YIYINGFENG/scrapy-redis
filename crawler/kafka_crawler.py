import os
import time
import json
import requests
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from urllib import robotparser

# Kafka-python 库，用于生产 & 消费 Kafka 消息
from kafka import KafkaConsumer, KafkaProducer

# =========== DNS解析相关(新增) ===========
# 假设 dns_resolver.py 与本文件在同级目录下
from dns_resolver import extract_domain, resolve_hostname

# =============== 1. 加载 .env ===============
load_dotenv(dotenv_path="C:\\Users\\Einstein\\Documents\\GitHub\\scrapy-redis\\.env", override=True)  # 会读取当前目录下的 .env 文件，你也可指定绝对路径
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "crawler_db")

print("检查 MySQL:", MYSQL_HOST, MYSQL_USER)

# =============== 2. 连接 MySQL ===============
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)
cursor = conn.cursor()

# 建表（若无则创建）
cursor.execute("""
CREATE TABLE IF NOT EXISTS crawled_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    url TEXT,
    title TEXT,
    content LONGTEXT,
    crawled_time DATETIME,
    UNIQUE KEY url_unique (url(255))
);
""")

def save_to_mysql(url, title, content):
    """
    将抓取到的 (url, title, content) 写入 MySQL。
    使用INSERT IGNORE避免重复插入（需url上有UNIQUE索引）。
    """
    sql = """
        INSERT IGNORE INTO crawled_data (url, title, content, crawled_time)
        VALUES (%s, %s, %s, %s)
    """
    cursor.execute(sql, (url, title, content, datetime.now()))
    conn.commit()

def already_crawled(url):
    """
    简单地查询 MySQL 是否已存在该URL。
    若找到，则返回 True。否则 False。
    """
    sql = "SELECT id FROM crawled_data WHERE url=%s"
    cursor.execute(sql, (url,))
    row = cursor.fetchone()
    return row is not None

# =============== 3. 机器人协议检查 ===============
def can_crawl(url, user_agent="MyCrawler"):
    # 你原先写死: "https://www.douban.com/robots.txt"
    # 如果你爬多站点，需要先解析url获取domain再拼robots.txt
    robots_url = "https://www.douban.com/robots.txt"
    rp = robotparser.RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    return rp.can_fetch(user_agent, url)

# =============== 4. 伪装 & Cookies ===============
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

cookies = {
    "dbcl2": "287079700:Fc39rI5JVZs",
    "ck": "FIRV"
}

# =============== 5. Kafka Producer ===============
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],  # 替换为你的 Kafka broker 地址
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_url_task(url, depth=0):
    """
    往Kafka的 'url_task' 主题发送新的url任务: {url, depth}
    """
    msg = {"url": url, "depth": depth}
    producer.send("url_task", msg)
    producer.flush()
    print(f"【Producer】已发送 URL={url}, depth={depth}")

# =============== 6. 爬虫逻辑 ===============
MAX_DEPTH = 3

def process_url_task(task):
    """
    处理一条来自Kafka的消息, 格式 { "url":..., "depth":...}
    1. 检查深度
    2. 检查 robots
    3. 检查是否已爬
    4. 发请求并解析 -> 写MySQL
    5. 发现新链接 -> 发送回Kafka
    """
    url = task.get("url")
    depth = task.get("depth", 0)

    if not url:
        return
    if depth > MAX_DEPTH:
        print(f"⛔ 超过最大深度, 跳过: {url}")
        return

    # 先检查 robots
    if not can_crawl(url):
        print(f"❌ 机器人协议禁止: {url}")
        return

    # 检查是否已经爬过
    if already_crawled(url):
        print(f"⚠️ 已爬过, 跳过: {url}")
        return
    
# =========== DNS解析开始 (核心新增) ===========
    domain = extract_domain(url)
    try:
        ip_list = resolve_hostname(domain)
        print(f"🌐 DNS解析成功: {domain} -> {ip_list}")
    except Exception as e:
        print(f"❌ DNS解析失败: {domain}, 错误: {e}")
        # 可以选择return，也可以继续尝试发送请求，看你需求。
        # return
    # =========== DNS解析结束 ===========


    print(f"📌 正在爬取: {url} (depth={depth})")
    try:
        resp = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        if resp.status_code == 200:
            # 提取标题和正文
            soup = BeautifulSoup(resp.text, "html.parser")
            title = soup.title.get_text(strip=True) if soup.title else "无标题"
            body = soup.find("body")
            content_text = body.get_text("\n", strip=True) if body else "无内容"

            # 写进MySQL
            save_to_mysql(url, title, content_text)

            # 提取所有子链接
            for link in soup.find_all("a", href=True):
                new_url = link["href"].strip()
                # 根据你的需求, 只爬指定范围. 这里示例豆瓣 group topic
                if new_url.startswith("https://www.douban.com/group/topic/"):
                    # 把它发送回Kafka, depth+1
                    send_url_task(new_url, depth+1)

            print(f"✅ 爬取完成: {url}")
        else:
            print(f"❌ 状态码={resp.status_code}, url={url}")
    except requests.exceptions.RequestException as e:
        print(f"❌ 请求异常: {e}")

# =============== 7. Kafka Consumer 循环消费 ===============
consumer = KafkaConsumer(
    "url_task",
    bootstrap_servers=["localhost:9092"],  # 替换为你的Kafka broker地址
    group_id="my_crawler_group",           # 多节点时相同group可并行分配
    auto_offset_reset="earliest",          # 无offset时, 从头开始消费
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# =============== 8. 程序入口 ===============
if __name__ == "__main__":
    # 1) 发送一些种子URL (可选: 只需第一次启动时加种子)
    #    你可注释掉或只在某台机器执行一次
    seed_urls = [
        "https://www.douban.com/group/586674/"
    ]
    for su in seed_urls:
        send_url_task(su, 0)

    # 2) 不断从Kafka消费消息 -> 爬取
    print("=== 开始消费 'url_task' 主题 ===")
    try:
        for msg in consumer:
            task_data = msg.value  # {url, depth}
            process_url_task(task_data)
    except KeyboardInterrupt:
        print("=== 停止消费 ===")
    finally:
        consumer.close()
        producer.close()
        cursor.close()
        conn.close()

