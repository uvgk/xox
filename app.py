#!/usr/bin/env python3
"""
Xox Sniper Mobile Web App — Clean Backend
"""

import os
import sys
import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
import uvicorn
import aiohttp
import requests
from aiohttp import ClientSession, TCPConnector, ClientTimeout

app = FastAPI(title="Xox Mobile Sniper")

PLATFORMS: Dict[str, Dict[str, Any]] = {
    "twitter":      {"url": "https://x.com/{username}", "nf": ["nothing to see here", "this account doesn't exist", "account suspended"]},
    "instagram":    {"url": "https://www.instagram.com/{username}/", "nf": ["page not found", "sorry, this page isn"]},
    "tiktok":       {"url": "https://www.tiktok.com/@{username}", "nf": ["couldn't find this account", "account not found"]},
    "reddit":       {"url": "https://www.reddit.com/user/{username}/about.json", "custom_check": "reddit"},
    "discord":      {"url": "https://discord.com/api/v9/users/{username}", "custom_check": "discord"},
    "guns.lol":     {"url": "https://guns.lol/{username}", "custom_check": "guns.lol"}
}

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"]

@dataclass
class CheckResult:
    platform: str
    username: str
    status: str
    http_status: int = 0
    latency_ms: float = 0.0
    profile_url: str = ""

@dataclass
class PlatformConfig:
    name: str
    url_template: str
    not_found_patterns: List[str] = field(default_factory=list)
    timeout_s: float = 5.0
    custom_check: str = ""

    @classmethod
    def from_raw(cls, name: str, d: Dict[str, Any]) -> "PlatformConfig":
        return cls(name=name, url_template=d.get("url", ""), not_found_patterns=d.get("nf", []), custom_check=d.get("custom_check", ""))

class TokenBucket:
    def __init__(self, rate: float = 5.0, capacity: int = 5) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, stop_event: asyncio.Event) -> bool:
        async with self._lock:
            while True:
                if stop_event.is_set():
                    return False
                now = time.monotonic()
                elapsed = now - self._last_refill
                if elapsed > 0:
                    self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                    self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                else:
                    await asyncio.sleep(0.05)

class ProxyManager:
    def __init__(self):
        self.proxies: List[str] = []
        self.valid_proxies: List[str] = []
        self.load_proxies_file()
        self.valid_proxies = list(self.proxies)

    def load_proxies_file(self):
        if not os.path.exists("proxies.txt"):
            with open("proxies.txt", "w", encoding="utf-8") as f:
                f.write("# Paste proxies here\n")
            self.proxies = []
            return
        raw = []
        with open("proxies.txt", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if not line.startswith("http://") and not line.startswith("https://"):
                        line = f"http://{line}"
                    raw.append(line)
        self.proxies = raw

    def get_proxy(self, use_proxies_flag: bool) -> str | None:
        if not use_proxies_flag or not self.valid_proxies:
            return None
        return random.choice(self.valid_proxies)

proxy_engine = ProxyManager()

class ResponseAnalyzer:
    @classmethod
    def analyze(cls, config: PlatformConfig, status: int, text: str) -> str:
        body = (text or "").lower()
        if config.custom_check in ["guns.lol", "discord", "reddit"]:
            return "Available" if status in (404, 410) or "not found" in body else "Taken"
        if status in (404, 410):
            return "Available"
        if status == 200:
            for pat in config.not_found_patterns:
                if pat in body:
                    return "Available"
            return "Taken"
        return "Taken"

async def check_username(session: ClientSession, config: PlatformConfig, username: str,
                         sem: asyncio.Semaphore, bucket: TokenBucket, stop_event: asyncio.Event, use_proxies: bool) -> CheckResult | None:
    if stop_event.is_set():
        return None
    url = config.url_template.format(username=username)
    display_url = f"https://discord.com/users/{username}" if config.custom_check == "discord" else url
    async with sem:
        if stop_event.is_set():
            return None
        if not await bucket.acquire(stop_event):
            return None
        t0 = time.monotonic()
        proxy_url = proxy_engine.get_proxy(use_proxies)
        try:
            async with session.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, proxy=proxy_url, timeout=ClientTimeout(total=config.timeout_s), allow_redirects=True) as resp:
                latency = (time.monotonic() - t0) * 1000
                text = await resp.text()
                verdict = ResponseAnalyzer.analyze(config, resp.status, text)
                return CheckResult(platform=config.name, username=username, status=verdict, http_status=resp.status, latency_ms=latency, profile_url=display_url)
        except Exception:
            return CheckResult(platform=config.name, username=username, status="Error", profile_url=display_url)

@app.get("/")
async def get_index():
    return FileResponse("index.html")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    stop_event = asyncio.Event()
    try:
        data = await websocket.receive_json()
        platform = data.get("platform", "discord")
        raw_users = data.get("usernames", "")
        use_proxy = bool(data.get("use_proxy", 1))
        
        usernames = [u.strip() for u in raw_users.split(",") if u.strip()]
        config = PlatformConfig.from_raw(platform, PLATFORMS[platform])
        sem = asyncio.Semaphore(10)
        bucket = TokenBucket(rate=5.0, capacity=5)
        
        checked = 0
        hits = 0
        failed = 0
        t0 = time.perf_counter()
        
        connector = TCPConnector(limit=100, ssl=False)
        async with ClientSession(connector=connector) as session:
            tasks = [check_username(session, config, u, sem, bucket, stop_event, use_proxy) for u in usernames]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res:
                    checked += 1
                    if res.status == "Available":
                        hits += 1
                    else:
                        failed += 1
                    elapsed = max(time.perf_counter() - t0, 0.001)
                    rps = int(checked / elapsed)
                    await websocket.send_json({"type": "stats", "checked": checked, "hits": hits, "taken": checked - hits, "failed": failed, "rps": rps})
                    await websocket.send_json({"type": "result", "username": res.username, "status": res.status})
        await websocket.send_json({"type": "done"})
    except WebSocketDisconnect:
        stop_event.set()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
