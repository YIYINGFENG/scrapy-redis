# crawler/robots.py
from urllib import robotparser

def can_crawl(url, user_agent="MyCrawler"):
    """
    检查目标 URL 是否允许用指定的 User-Agent 爬取
    """
    robots_url = "https://www.douban.com/robots.txt"
    rp = robotparser.RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    return rp.can_fetch(user_agent, url)
