#!/usr/bin/env python3
"""
Xox Sniper Mobile Web App — Name Generation & Production Edition
"""

import os
import sys
import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List

try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse
    import uvicorn
    import aiohttp
    import requests
    from aiohttp import ClientSession, TCPConnector, ClientTimeout
except ImportError:
    print("[-] Installing required web and networking dependencies...")
    os.system(f'"{sys.executable}" -m pip install fastapi uvicorn websockets wsproto aiohttp requests -q')
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse
    import uvicorn
    import aiohttp
    import requests
    from aiohttp import ClientSession, TCPConnector, ClientTimeout

app = FastAPI(title="Xox Mobile Sniper")

WEBHOOKS: Dict[str, str] = {
    "discord": "https://discord.com/api/webhooks/1545146230609420380/Rjk-FiYGrp1QBoC-swVDU8tkeuF4gUJVNZ1Sba9zhj_IfMzivg1-Lh3gIJQdVt4fctD7"
}

PLATFORMS: Dict[str, Dict[str, Any]] = {
    "twitter":      {"url": "https://x.com/{username}", "nf": ["nothing to see here", "this account doesn't exist", "account suspended"], "cat": "Social"},
    "instagram":    {"url": "https://www.instagram.com/{username}/", "nf": ["page not found", "sorry, this page isn", "the link you followed may be broken"], "cat": "Social"},
    "tiktok":       {"url": "https://www.tiktok.com/@{username}", "nf": ["couldn't find this account", "account not found", "no user found"], "cat": "Social"},
    "reddit":       {"url": "https://www.reddit.com/user/{username}/about.json", "custom_check": "reddit", "cat": "Social"},
    "telegram":     {"url": "https://t.me/{username}", "nf": ["if you have telegram", "channels", "view in telegram"], "cat": "Social"},
    "pinterest":    {"url": "https://www.pinterest.com/{username}/", "nf": ["sorry! we couldn't find that", "page not found"], "cat": "Social"},
    "snapchat":     {"url": "https://www.snapchat.com/add/{username}", "nf": ["sorry, no page found", "add on snapchat"], "cat": "Social"},
    "discord":      {"url": "https://discord.com/api/v9/users/{username}", "custom_check": "discord", "cat": "Social"},
    "guns.lol":     {"url": "https://guns.lol/{username}", "custom_check": "guns.lol", "cat": "Bio Site"},
    "linktree":     {"url": "https://linktr.ee/{username}", "nf": ["page not found", "linktree / create"], "cat": "Bio Site"},
    "stab":         {"url": "https://stab.fund/{username}", "custom_check": "stab", "cat": "Bio Site"},
    "minecraft":    {"url": "https://api.mojang.com/users/profiles/minecraft/{username}", "custom_check": "minecraft", "cat": "Gaming"},
    "github":       {"url": "https://github.com/{username}", "nf": ["not found", "get started with github"], "cat": "Dev"},
    "youtube":      {"url": "https://www.youtube.com/@{username}", "nf": ["this page isn't available", "not found"], "cat": "Video"},
    "twitch":       {"url": "https://www.twitch.tv/{username}", "nf": ["content is unavailable", "sorry. unless you've got a time machine"], "cat": "Video"},
    "steam":        {"url": "https://steamcommunity.com/id/{username}", "nf": ["the specified profile could not be found"], "cat": "Gaming"}
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

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
        return cls(
            name=name,
            url_template=d.get("url", ""),
            not_found_patterns=d.get("nf", []),
            custom_check=d.get("custom_check", ""),
        )

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
        self.blacklisted_proxies: Dict[str, float] = {}
        self.load_proxies_file()

    def load_proxies_file(self):
        if not os.path.exists("proxies.txt"):
            with open("proxies.txt", "w", encoding="utf-8") as f:
                f.write("# Paste your proxies here (e.g., http://ip:port or ip:port)\n")
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

    def validate_single_proxy(self, p):
        try:
            r = requests.get("https://httpbin.org/ip", proxies={"http": p, "https": p}, timeout=2.0)
            if r.status_code == 200:
                self.valid_proxies.append(p)
        except:
            pass

    def validate_all(self):
        self.load_proxies_file()
        self.valid_proxies = []
        for p in self.proxies:
            self.validate_single_proxy(p)
        return len(self.valid_proxies), len(self.proxies)

    def get_proxy(self, use_proxies_flag: bool) -> str | None:
        if not use_proxies_flag or not self.valid_proxies:
            return None
        return random.choice(self.valid_proxies)

proxy_engine = ProxyManager()
proxy_engine.validate_all()

class ResponseAnalyzer:
    @classmethod
    def analyze(cls, config: PlatformConfig, status: int, text: str) -> str:
        body = (text or "").lower()
        if config.custom_check in ["guns.lol", "discord", "stab", "reddit"]:
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

@app.get("/", response_class=HTMLResponse)
async def get_index():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Xox Mobile Sniper</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-[#050505] text-[#f1f5f9] font-sans p-4">
    <div class="max-w-md mx-auto space-y-4">
        <h1 class="text-xl font-bold text-white">⊙ XOX MOBILE SNIPER</h1>
        
        <!-- Metrics Grid -->
        <div class="grid grid-cols-5 gap-2 text-center">
            <div class="bg-[#090909] border border-[#27272a] p-2 rounded-lg"><div class="text-[9px] text-gray-400">CHK</div><div id="m-chk" class="text-sm font-bold">0</div></div>
            <div class="bg-[#090909] border border-[#27272a] p-2 rounded-lg"><div class="text-[9px] text-gray-400">HITS</div><div id="m-hits" class="text-sm font-bold text-emerald-400">0</div></div>
            <div class="bg-[#090909] border border-[#27272a] p-2 rounded-lg"><div class="text-[9px] text-gray-400">TAKEN</div><div id="m-taken" class="text-sm font-bold text-red-400">0</div></div>
            <div class="bg-[#090909] border border-[#27272a] p-2 rounded-lg"><div class="text-[9px] text-gray-400">FAIL</div><div id="m-fail" class="text-sm font-bold">0</div></div>
            <div class="bg-[#090909] border border-[#27272a] p-2 rounded-lg"><div class="text-[9px] text-gray-400">RPS</div><div id="m-rps" class="text-sm font-bold text-amber-400">0</div></div>
        </div>

        <!-- Controls Card -->
        <div class="bg-[#090909] border border-[#27272a] p-4 rounded-xl space-y-3">
            <div>
                <label class="text-xs text-gray-400 font-bold">Platform</label>
                <select id="platform" class="w-full bg-[#111111] border border-[#27272a] text-white p-2 rounded-lg mt-1 text-sm">
                    <option value="discord">Discord</option>
                    <option value="instagram">Instagram</option>
                    <option value="tiktok">TikTok</option>
                    <option value="twitter">Twitter</option>
                    <option value="guns.lol">Guns.lol</option>
                </select>
            </div>

            <!-- Name Generator Box -->
            <div class="bg-[#111111] border border-[#27272a] p-3 rounded-lg space-y-2">
                <label class="text-xs text-gray-400 font-bold">Name Generator</label>
                <div class="grid grid-cols-2 gap-2">
                    <select id="gen-type" class="bg-[#050505] border border-[#27272a] text-white p-1.5 rounded text-xs">
                        <option value="3l">3 Letters (3L)</option>
                        <option value="4l">4 Letters (4L)</option>
                        <option value="5l">5 Letters (5L)</option>
                        <option value="3c">3 Chars (Mixed)</option>
                        <option value="4c">4 Chars (Mixed)</option>
                    </select>
                    <input type="number" id="gen-qty" value="30" class="bg-[#050505] border border-[#27272a] text-white p-1.5 rounded text-xs" placeholder="Qty">
                </div>
                <button onclick="generateNames()" class="w-auto bg-[#27272a] text-white font-bold py-1.5 px-3 rounded text-xs hover:bg-[#3f3f46]">Generate & Insert Names</button>
            </div>

            <div>
                <label class="text-xs text-gray-400 font-bold">Usernames (Comma Separated)</label>
                <textarea id="usernames" rows="3" class="w-full bg-[#111111] border border-[#27272a] text-white p-2 rounded-lg mt-1 text-sm" placeholder="test1, test2, test3"></textarea>
            </div>
            <div class="flex items-center space-x-2">
                <input type="checkbox" id="use-proxy" checked class="w-4 h-4 accent-white">
                <label class="text-xs text-gray-300">Enable Smart Proxy Rotation</label>
            </div>
            <button onclick="startScan()" id="scan-btn" class="w-full bg-white text-black font-bold py-2.5 rounded-lg text-sm hover:bg-gray-200">LAUNCH SNIPER</button>
        </div>

        <!-- Live Output Feed -->
        <div class="bg-[#090909] border border-[#27272a] p-4 rounded-xl">
            <h2 class="text-xs font-bold text-gray-400 mb-2">LIVE FEED</h2>
            <div id="feed" class="space-y-2 max-h-64 overflow-y-auto pr-1"></div>
        </div>
    </div>

    <script>
        let ws;
        function generateNames() {
            const type = document.getElementById('gen-type').value;
            const qty = parseInt(document.getElementById('gen-qty').value) || 30;
            
            let letters = "abcdefghijklmnopqrstuvwxyz";
            let alphanumeric = "abcdefghijklmnopqrstuvwxyz0123456789";
            let generated = new Set();
            
            while(generated.size < qty) {
                let val = "";
                let k = type.startsWith('3') ? 3 : (type.startsWith('4') ? 4 : 5);
                let chars = type.endsWith('c') ? alphanumeric : letters;
                for(let i=0; i<k; i++) {
                    val += chars.charAt(Math.floor(Math.random() * chars.length));
                }
                generated.add(val);
            }
            
            document.getElementById('usernames').value = Array.from(generated).join(', ');
        }

        function startScan() {
            const platform = document.getElementById('platform').value;
            const usernames = document.getElementById('usernames').value;
            const useProxy = document.getElementById('use-proxy').checked ? 1 : 0;
            
            if (!usernames) return alert('Enter or generate usernames first!');
            
            document.getElementById('feed').innerHTML = '';
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            
            ws.onopen = () => {
                ws.send(JSON.stringify({platform, usernames, use_proxy: useProxy}));
                document.getElementById('scan-btn').innerText = "STOP";
                document.getElementById('scan-btn').className = "w-full bg-red-600 text-white font-bold py-2.5 rounded-lg text-sm";
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.type === 'stats') {
                    document.getElementById('m-chk').innerText = data.checked;
                    document.getElementById('m-hits').innerText = data.hits;
                    document.getElementById('m-taken').innerText = data.taken;
                    document.getElementById('m-fail').innerText = data.failed;
                    document.getElementById('m-rps').innerText = data.rps;
                } else if (data.type === 'result') {
                    const feed = document.getElementById('feed');
                    const color = data.status === 'Available' ? 'text-emerald-400' : 'text-red-400';
                    feed.innerHTML += `<div class="bg-[#111111] border border-[#27272a] p-2.5 rounded-lg flex justify-between items-center text-xs">
                        <span class="font-bold text-white">${data.username}</span>
                        <span class="${color} font-bold">${data.status}</span>
                    </div>`;
                    feed.scrollTop = feed.scrollHeight;
                } else if (data.type === 'done') {
                    document.getElementById('scan-btn').innerText = "LAUNCH SNIPER";
                    document.getElementById('scan-btn').className = "w-full bg-white text-black font-bold py-2.5 rounded-lg text-sm";
                }
            };
        }
    </script>
</body>
</html>
"""

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