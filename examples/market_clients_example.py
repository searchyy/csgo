from cs2_tradeup import (
    BrowserExtractionConfig,
    BuffMarketAPI,
    PlaywrightPriceFallback,
    RandomizedRateLimiter,
    UUMarketAPI,
)


def build_buff_client() -> BuffMarketAPI:
    cookie_string = "session=YOUR_SESSION_COOKIE; csrf_token=YOUR_CSRF_TOKEN"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Referer": "https://buff.163.com/market/csgo",
    }
    proxies = {
        "http": "http://127.0.0.1:7890",
        "https": "http://127.0.0.1:7890",
    }

    rate_limiter = RandomizedRateLimiter(min_delay_seconds=2.0, max_delay_seconds=5.0)

    # Cookie 建议从你自己已登录的浏览器会话中导出。
    # 真实 Cookie 不要提交到仓库；建议放到 .env 或本地配置文件。
    browser_fallback = PlaywrightPriceFallback(
        BrowserExtractionConfig(
            search_url_template="https://buff.163.com/market/csgo#tab=selling&page_num=1&search={query}",
            wait_selector="[data-testid='market-list'], .market-card",
            lowest_price_selector=".sell-order-price, .market-card .price",
            recent_average_selector=".reference-price, .market-card .avg-price",
            headless=True,
        ),
        base_url="https://buff.163.com",
        headers=headers,
        cookies={},
        proxies=proxies,
    )

    return BuffMarketAPI(
        headers=headers,
        cookie_string=cookie_string,
        proxies=proxies,
        rate_limiter=rate_limiter,
        browser_fallback=browser_fallback,
    )


def build_uu_client() -> UUMarketAPI:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.youpin898.com/",
    }
    return UUMarketAPI(
        headers=headers,
        cookies={"uu_session": "YOUR_SESSION_COOKIE"},
        rate_limiter=RandomizedRateLimiter(min_delay_seconds=2.5, max_delay_seconds=5.5),
    )


if __name__ == "__main__":
    buff = build_buff_client()
    print("BUFF:", buff.get_item_price("AK-47 | Case Hardened", "Factory New"))

    uu = build_uu_client()
    print("UU:", uu.get_item_price("AK-47 | Case Hardened", "Factory New"))
