# run.py
# 一个简洁的入口脚本

from crawler.kafka_crawler import consumer, process_url_task, send_url_task

def main():
    # 可先发种子URL:
    seed_urls = ["https://www.douban.com/group/586674/"]
    for su in seed_urls:
        send_url_task(su, 0)
    
    print("=== 开始消费 'url_task' 主题 ===")
    try:
        for msg in consumer:
            task_data = msg.value  # {url, depth}
            process_url_task(task_data)
    except KeyboardInterrupt:
        print("=== 停止消费 ===")
    finally:
        consumer.close()
if __name__ == "__main__":
    main()
