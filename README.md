# 简易分布式爬虫系统
字节青训营大数据项目-分布式爬虫系统

本项目构建了一个分布式的网络爬虫系统，通过Kafka消息队列分发爬取任务，将爬取的豆瓣小组数据存储到 MySQL 数据库中，并使用 Flask 提供查询接口。同时，系统集成了反反爬虫机制，以提高爬取稳定性，并引入监控系统确保运行状态可视化和故障快速恢复。
## 项目结构
```
scrapy-redis/
├── .env                 # 环境配置文件
├── requirements.txt     # 项目依赖
└── crawler/
    ├── run.py          # 入口脚本
    ├── kafka_crawler.py # 爬虫核心逻辑
    └── dns_resolver.py  # DNS解析模块
```
## 技术选型
- 语言:  Python 3.x
- 任务调度 & URL 分发：Kafka 
- 分布式爬取： 
  - Requests
  - Kafka Consumer Groups
- 数据解析 & 处理：BeautifulSoup4
- 数据存储：MySQL
- 数据查询 & API： Flask
- 反反爬虫： 代理池 / User-Agent 轮换 / 动态验证码处理 / 访问频率控制
- 监控 & 高可用： Prometheus / Kubernetes / 日志分析（ELK）

## 项目架构
[![pEY1q8U.jpg](https://s21.ax1x.com/2025/03/05/pEY1q8U.jpg)](https://imgse.com/i/pEY1q8U)
爬取豆瓣小组数据时，数据规模较大，每个小组大约有 1000+ 个帖子，每个帖子可能包含 10-20 个新的链接，而其中约 5% 的链接为高价值内容，如热门帖子，因此需要保证抓取的效率和精度。其次，访问频率受到严格限制，爬虫的访问频率必须控制在每秒 2 次以内，以避免因过度请求而被封禁。部分页面需要模拟登录才能访问，而豆瓣对 IP 访问有频率限制，因此需要采用代理池进行 IP 轮换。

针对这些问题，当前的架构设计采用了 Kafka 消息队列来解耦 URL 发现与爬取过程，确保任务的异步处理。Kafka 支持多机器分布式部署，可以大大提升爬虫的处理能力，避免任务丢失，并确保任务的可靠性。同时，爬虫通过 Kafka 任务队列的生产者和消费者模式分发爬取任务，确保任务的有序执行。

爬取的数据存储在 MySQL 中，以便支持去重判断，防止重复抓取相同的内容，并为后续的数据分析提供数据支持。为了应对豆瓣的反爬虫机制，架构设计中还包括了 Cookie 池和请求头伪装，模拟真实用户行为，避免被识别为爬虫。此外，通过限制访问频率，进一步规避了反爬虫策略带来的风险。


## 运行说明
1.安装依赖:
```bash
pip install -r requirements.txt
```
2.配置环境变量:

- 复制 .env.example 为 .env
- 修改数据库连接配置

3.初始化数据库:
```bash
mysql -u root -p < init_db.sql
```

4.启动Kafka:
```bash
# 启动zookeeper
zkServer
# 启动kafka
bin/kafka-server-start.sh config/server.properties
```

5.运行爬虫:
```bash
python crawler/run.py
```
