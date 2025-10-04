import os
import json
import time
import random
import asyncio
import aiohttp
from datetime import datetime
from upstash_redis import Redis

# ============ 🔧 CONFIG ============

# 🔐 Render Environment Variables
REDIS_URL = os.getenv("REDIS_URL")
REDIS_TOKEN = os.getenv("REDIS_TOKEN")

if not REDIS_URL or not REDIS_TOKEN:
    raise RuntimeError("🚨 Redis bağlantısı için REDIS_URL ve REDIS_TOKEN ENV değişkenlerini tanımla kral!")

# 🔌 Redis bağlantısı
r = Redis(url=REDIS_URL, token=REDIS_TOKEN)

# 🕵️ User-Agent havuzu (fazla olması iyidir)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.141 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/126.0.0.0 Safari/537.36"
]

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

# ============ 🧮 HELPERS ============

def calculate_age(pair_created_at):
    if not pair_created_at:
        return None
    try:
        created = datetime.fromtimestamp(int(pair_created_at) / 1000)
        now = datetime.utcnow()
        diff = now - created
        days = diff.days
        seconds = diff.seconds
        years = days // 365
        months = days // 30
        if years > 0: return f"{years}y"
        elif months > 0: return f"{months}m"
        elif days > 0: return f"{days}d"
        else:
            hours = seconds // 3600
            if hours > 0: return f"{hours}h"
            minutes = (seconds % 3600) // 60
            if minutes > 0: return f"{minutes}m"
            return f"{seconds}s"
    except Exception:
        return None

async def fetch_json(session, url, headers=None):
    try:
        async with session.get(url, headers=headers or get_headers(), timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                print(f"\033[33m[WARN]\033[0m {url} → HTTP {resp.status}")
    except Exception as e:
        print(f"\033[31m[ERROR]\033[0m fetch_json hata: {e}")
    return None

# ============ 🌐 API FETCHERS ============

async def fetch_from_dexscreener(session, address, chain):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{address}"
    data = await fetch_json(session, url)
    if not data: return None
    pairs = [p for p in data.get("pairs", []) if p.get("chainId") == chain]
    if not pairs: return None
    best = max(pairs, key=lambda p: ((p.get("liquidity") or {}).get("usd") or 0))
    txns = ((best.get("txns", {}) or {}).get("h24", {}) or {})
    return {
        "priceUsd": best.get("priceUsd"),
        "priceChange": best.get("priceChange", {}),
        "liquidityUsd": ((best.get("liquidity") or {}).get("usd")),
        "fdv": best.get("fdv"),
        "marketCap": best.get("marketCap"),
        "pairCreatedAt": best.get("pairCreatedAt"),
        "txns": (txns.get("buys", 0) or 0) + (txns.get("sells", 0) or 0)
    }

async def fetch_from_geckoterminal(session, address, chain):
    url = f"https://api.geckoterminal.com/api/v2/networks/{chain}/tokens/{address}"
    data = await fetch_json(session, url)
    if not data or "data" not in data: return None
    attr = data["data"]["attributes"]
    txs = (attr.get("transactions") or {}).get("h24", {})
    return {
        "priceUsd": attr.get("price_usd"),
        "priceChange": {
            "h6": attr.get("price_change_percentage", {}).get("h6"),
            "h24": attr.get("price_change_percentage", {}).get("h24")
        },
        "liquidityUsd": attr.get("liquidity_usd"),
        "fdv": attr.get("fdv_usd"),
        "marketCap": attr.get("market_cap_usd"),
        "pairCreatedAt": attr.get("pool_created_at"),
        "txns": (txs.get("buys", 0) or 0) + (txs.get("sells", 0) or 0)
    }

async def fetch_from_coingecko(session, address, chain):
    url = f"https://api.coingecko.com/api/v3/onchain/networks/{chain}/tokens/{address}/pools"
    data = await fetch_json(session, url)
    if not data or "data" not in data or not data["data"]: return None
    pool = data["data"][0]["attributes"]
    txns = pool.get("transactions", {}).get("h24", {})
    return {
        "priceUsd": pool.get("base_token_price_usd"),
        "priceChange": {
            "h6": pool.get("price_change_percentage", {}).get("h6"),
            "h24": pool.get("price_change_percentage", {}).get("h24")
        },
        "liquidityUsd": pool.get("reserve_in_usd"),
        "fdv": pool.get("fdv_usd"),
        "marketCap": pool.get("market_cap_usd"),
        "pairCreatedAt": pool.get("pool_created_at"),
        "txns": (txns.get("buys", 0) or 0) + (txns.get("sells", 0) or 0)
    }

async def fetch_token_data(session, address, chain):
    for name, func in [
        ("Dexscreener", fetch_from_dexscreener),
        ("GeckoTerminal", fetch_from_geckoterminal),
        ("CoinGecko", fetch_from_coingecko)
    ]:
        result = await func(session, address, chain)
        if result:
            print(f"\033[36m[SOURCE]\033[0m {name} → {chain}:{address[:6]}...")
            return result
    return None

# ============ 💾 REDIS SAVE ============

def save_to_redis(token_data, new_data):
    field = f"{token_data['chain']}:{token_data['contract'].lower()}"
    existing = r.hget("bmcnewtokens", field)
    existing = json.loads(existing) if existing else {}

    if not new_data:
        print(f"\033[33m[SKIP]\033[0m {token_data['name']} ({token_data['chain']}) veri yok, eski veri korundu.")
        return

    age = calculate_age(new_data.get("pairCreatedAt")) or existing.get("age")

    merged = {
        "name": token_data["name"],
        "chain": token_data["chain"],
        "contract": token_data["contract"],
        "priceUsd": new_data.get("priceUsd") or existing.get("priceUsd"),
        "priceChange": new_data.get("priceChange") or existing.get("priceChange", {}),
        "liquidityUsd": new_data.get("liquidityUsd") or existing.get("liquidityUsd"),
        "fdv": new_data.get("fdv") or existing.get("fdv"),
        "marketCap": new_data.get("marketCap") or existing.get("marketCap"),
        "pairCreatedAt": new_data.get("pairCreatedAt") or existing.get("pairCreatedAt"),
        "age": age,
        "txns": new_data.get("txns") or existing.get("txns", 0),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
    }

    if merged != existing:
        r.hset("bmcnewtokens", field, json.dumps(merged, ensure_ascii=False))
        print(f"\033[32m[UPDATED]\033[0m {token_data['name']} ({token_data['chain']})")
    else:
        print(f"\033[90m[NO CHANGE]\033[0m {token_data['name']} ({token_data['chain']})")

# ============ ⚙️ MAIN LOOP ============

async def process_token(session, token_data, sem):
    async with sem:
        new_data = await fetch_token_data(session, token_data["contract"], token_data["chain"])
        save_to_redis(token_data, new_data)
        await asyncio.sleep(0.001)

async def main_loop():
    sem = asyncio.Semaphore(5)
    async with aiohttp.ClientSession() as session:
        while True:
            start = time.time()
            all_tokens = r.hgetall("bmcnewtokens")

            if not all_tokens:
                print("\033[33m[INIT]\033[0m Redis boş, token eklemen gerekiyor kral.")
                await asyncio.sleep(10)
                continue

            tasks = []
            for field, val in all_tokens.items():
                try:
                    token_data = json.loads(val)
                    tasks.append(process_token(session, token_data, sem))
                except Exception as e:
                    print(f"\033[31m[ERROR]\033[0m Token parse hata: {e}")

            await asyncio.gather(*tasks, return_exceptions=True)
            duration = time.time() - start
            print(f"\n🔄 Tur tamamlandı ({duration:.2f}s) → tekrar başlıyor...\n")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main_loop())