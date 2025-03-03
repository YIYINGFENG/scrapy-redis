import dns.resolver
from urllib.parse import urlparse


def extract_domain(url: str) -> str:
    """
    从一个完整的 URL 中解析出域名(Host)，例如:
      https://www.douban.com/group/586674/
    将返回: 'www.douban.com'
    
    如果 URL 中还包含端口，比如 'localhost:8080'，
    这里 netloc 则是 'localhost:8080'，可按需再拆分 ':'。
    """
    parsed = urlparse(url)
    domain = parsed.netloc  # 例： 'www.douban.com' 或 'localhost:8080'
    # 如果带有端口(如 'xxx.com:8080')，可再按需求处理。
    return domain

def resolve_hostname(domain: str) -> list[str]:
    """
    使用 dnspython 显式地进行 DNS 解析，获取该域名的所有 IPv4 地址。

    参数:
      domain: 域名字符串, 例如 'www.douban.com'

    返回:
      IPv4 地址列表，例如 ['123.45.67.89', '98.76.54.32']

    如果需要解析AAA或其他类型的记录，可在resolve()中改成 'AAAA' / 'MX' / 'CNAME' 等。
    """
    # 解析 A 记录
    answers = dns.resolver.resolve(domain, 'A')
    ip_list = [a.to_text() for a in answers]
    return ip_list

def main():
    """
    测试入口。仅当你用 "python dns_resolver.py" 直接运行此文件时，会执行这里。
    """
    test_url = "https://www.douban.com/group/586674/"
    domain = extract_domain(test_url)
    print(f"测试 URL: {test_url}")
    print(f"提取出的域名: {domain}")

    try:
        ip_addresses = resolve_hostname(domain)
        print(f"DNS 解析成功 -> IP列表: {ip_addresses}")
    except Exception as e:
        print(f"DNS 解析失败: {e}")


# 如果用 "python dns_resolver.py" 运行本文件，就执行 main() 做测试
if __name__ == "__main__":
    main()
