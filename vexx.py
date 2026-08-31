r"""
       ⸸  ⛧  ⸸                                          ⸸  ⛧  ⸸
  _  __  __   __  ___  _  __    _       ____  _   _ ___ ____ _____ _____ ____  _   _ 
 | |/ /  \ \ / / / _ \| |/ /  / \     / ___|| | | |_ _/ ___| ____|_   _/ ___|| | | |
 | ' /    \ V / | | | | ' /  / _ \    \___ \| | | || || |  _|  _|   | | \___ \| | | |
 |  <      | |  | |_| |  <  / ___ \    ___) | |_| || || |_| | |___  | |  ___) | |_| |
 |_|\_\    |_|   \___/|_|\_/_/   \_\  |____/ \___/|___|\____|_____| |_| |____/ \___/ 
      666 ⸸          KYOKA SUIGETSU          ⸸ 666         
"""

import discord
from discord.ext import commands
import asyncio
import os
import time
import random
import psutil
import logging
import json
import aiohttp
import requests
import threading
import base64
import hashlib
import warnings
import gc
from typing import Dict, List, Optional
from datetime import datetime
from collections import deque
import sys
from colorama import Fore, Style
import fade
import aiofiles
import shutil
import tempfile

class StateManager:
    lock = asyncio.Lock()
    _initialized = False

    @staticmethod
    def ensure_file_exists(filepath):

        if not os.path.exists(filepath):
            try:
                with open(filepath, 'w') as f:
                    f.write('{}')
                    f.flush()
                    os.fsync(f.fileno())
                logging.info(f"statemanager: created {filepath}")
                return True
            except Exception as e:
                logging.error(f"statenanager: failed to create {filepath}: {e}")
                return False
        return True

    @staticmethod
    async def initialize(filepath):

        async with StateManager.lock:
            if not StateManager._initialized:
                StateManager.ensure_file_exists(filepath)
                StateManager._initialized = True
                logging.info(f"statemanager: initialized with {filepath}")

    @staticmethod
    async def load(filepath):

        StateManager.ensure_file_exists(filepath)

        try:
            async with aiofiles.open(filepath, 'r') as f:
                content = await f.read()
                if not content or not content.strip():
                    return {}
                data = json.loads(content)
                logging.info(f"statemanager: loaded state from {filepath}")
                return data
        except json.JSONDecodeError as e:
            logging.error(f"statemanager: JSON decode error in {filepath}: {e}")
        except Exception as e:
            logging.error(f"statemanager: load error for {filepath}: {e}")

        backup = filepath + ".bak"
        if os.path.exists(backup):
            try:
                async with aiofiles.open(backup, 'r') as f:
                    content = await f.read()
                    if content and content.strip():
                        data = json.loads(content)
                        logging.info(f"statemanager: restored from backup {backup}")
                        await StateManager.save(filepath, data)
                        return data
            except Exception as e:
                logging.error(f"statemanager: backup load failed: {e}")

        return {}

    @staticmethod
    async def save(filepath, data):

        tmp_path = filepath + ".tmp"
        backup_path = filepath + ".bak"

        try:
            json_content = json.dumps(data, indent=2)

            with open(tmp_path, 'w') as f:
                f.write(json_content)
                f.flush()
                os.fsync(f.fileno())

            if os.path.exists(filepath):
                try:
                    shutil.copy2(filepath, backup_path)
                except Exception as e:
                    logging.warning(f"statemanager: backup creation failed: {e}")

            os.replace(tmp_path, filepath)
            logging.info(f"statenanager: saved state to {filepath} ({len(data)} entries)")
            return True

        except Exception as e:
            logging.error(f"statemanager: save error: {e}")
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except:
                    pass

            try:
                with open(filepath, 'w') as f:
                    f.write(json.dumps(data, indent=2))
                    f.flush()
                    os.fsync(f.fileno())
                logging.info(f"statenanager: fallback save succeeded")
                return True
            except Exception as e2:
                logging.error(f"statemanager: fallback save also failed: {e2}")
                return False

PROXIES_FILE = "proxies.txt"

CMD_PREFIX = ""

RPC_APPLICATION_IDS = [
    ("1467114807516856499", "1000020561"),  # app_id, asset_name default
    ("1485380417598263326", "1000020528"),  # app_id, asset_name default
]
try:
    import proxy_manager as _pm_module
    _pm_instance = _pm_module.start_proxy_manager()
    def load_proxies() -> List[str]:
        return _pm_module.load_proxies()
    def get_random_proxy() -> Optional[str]:
        return _pm_module.get_random_proxy()
except ImportError:
    proxy_file_lock = threading.Lock()
    _proxy_cache: List[str] = []
    _proxy_cache_time: float = 0.0
    _PROXY_CACHE_TTL = 30.0
    def load_proxies() -> List[str]:
        global _proxy_cache, _proxy_cache_time
        now = time.time()
        if _proxy_cache and now - _proxy_cache_time < _PROXY_CACHE_TTL:
            return _proxy_cache
        with proxy_file_lock:
            if os.path.exists(PROXIES_FILE):
                try:
                    with open(PROXIES_FILE, 'r') as f:
                        _proxy_cache = [line.strip() for line in f if line.strip()]
                        _proxy_cache_time = now
                        return _proxy_cache
                except Exception:
                    pass
        return _proxy_cache or []
    def get_random_proxy() -> Optional[str]:
        proxies = load_proxies()
        if proxies:
            return random.choice(proxies)
        return None
    _pm_instance = None

warnings.simplefilter("ignore")
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=UserWarning)
import os as _os
_os.environ["PYTHONWARNINGS"] = "ignore"

text = r"""
       ⸸  ⛧  ⸸                                          ⸸  ⛧  ⸸
  _  __  __   __  ___  _  __    _       ____  _   _ ___ ____ _____ _____ ____  _   _ 
 | |/ /  \ \ / / / _ \| |/ /  / \     / ___|| | | |_ _/ ___| ____|_   _/ ___|| | | |
 | ' /    \ V / | | | | ' /  / _ \    \___ \| | | || || |  _|  _|   | | \___ \| | | |
 |  <      | |  | |_| |  <  / ___ \    ___) | |_| || || |_| | |___  | |  ___) | |_| |
 |_|\_\    |_|   \___/|_|\_/_/   \_\  |____/ \___/|___|\____|_____| |_| |____/ \___/ 
      666 ⸸          KYOKA SUIGETSU          ⸸ 666         
"""

loading = r"""
       ⸸  ⛧  ⸸                                          ⸸  ⛧  ⸸
  _  __  __   __  ___  _  __    _       ____  _   _ ___ ____ _____ _____ ____  _   _ 
 | |/ /  \ \ / / / _ \| |/ /  / \     / ___|| | | |_ _/ ___| ____|_   _/ ___|| | | |
 | ' /    \ V / | | | | ' /  / _ \    \___ \| | | || || |  _|  _|   | | \___ \| | | |
 |  <      | |  | |_| |  <  / ___ \    ___) | |_| || || |_| | |___  | |  ___) | |_| |
 |_|\_\    |_|   \___/|_|\_/_/   \_\  |____/ \___/|___|\____|_____| |_| |____/ \___/ 
      666 ⸸            Loading...            ⸸ 666         
"""

def loading_animation():
    l = ['|', '/', '-', '\\']
    for i in l + l + l:
        sys.stdout.write(f'\\r[\\x1b[95m+\\x1b[95m\\x1b[37m] Preparing packages... [{i}]')
        sys.stdout.flush()
        time.sleep(0.2)

def clear_terminal_event():
    if os.name == "nt":
        os.system("cls")
    else:
        os.system("clear")

class LoginFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage().lower()
        return (
            "logged in" in msg or
            "valid" in msg or
            "invalid" in msg or
            "token invalid" in msg or
            "removed invalid" in msg or
            "login failed" in msg or
            "401" in msg or
            "unauthorized" in msg or
            "stopped instance" in msg
        )

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s', datefmt='%H:%M'))
handler.addFilter(LoginFilter())

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler]
)

for logger_name in ['discord', 'discord.client', 'discord.gateway', 'discord.http', 'discord.state', 'websockets', 'asyncio']:
    logging.getLogger(logger_name).setLevel(logging.ERROR)

async def patched_static_login(self, token, *, bot):
    self._HTTPClient__session = aiohttp.ClientSession()
    old_token = self.token
    self.token = token
    self.bot_token = bot

    try:
        data = await self.request(discord.http.Route('GET', '/users/@me'))
    except discord.HTTPException as exc:
        self.token = old_token
        if exc.response.status == 401:
            raise discord.LoginFailure('Improper token has been passed.') from exc
        raise

    return data

_rpc_asset_cache = {}
_rpc_asset_cache_fetch_time: Dict[str, float] = {}
_RPC_ASSET_CACHE_TTL = 300.0
CURRENT_PLATFORM = {"value": "desktop"}
PLATFORM_PROPS = {
    "desktop": {"$os": "Windows",     "$browser": "Discord Client",   "$device": ""},
    "mobile":  {"$os": "Android",     "$browser": "Discord Android",  "$device": "Android"},
    "web":     {"$os": "Windows",     "$browser": "Chrome",           "$device": ""},
    "console": {"$os": "PlayStation", "$browser": "Discord Embedded", "$device": "PlayStation"},
}
def load_rpc_apps():
    return RPC_APPLICATION_IDS

async def _rpc_fetch_assets_for_app(app_id_str, force=False):
    now = time.time()
    last_fetch = _rpc_asset_cache_fetch_time.get(app_id_str, 0.0)
    if not force and (now - last_fetch) < _RPC_ASSET_CACHE_TTL:
        return
    url = f"https://discord.com/api/v10/oauth2/applications/{app_id_str}/assets"
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        for asset in await resp.json():
                            name = asset.get("name")
                            aid = str(asset.get("id"))
                            _rpc_asset_cache[f"{app_id_str}:{name}"] = aid
                        _rpc_asset_cache_fetch_time[app_id_str] = time.time()
                        return
                    elif resp.status == 429:
                        await asyncio.sleep(2 + attempt)
                        continue
                    else:
                        break
        except asyncio.TimeoutError:
            if attempt < 2:
                await asyncio.sleep(1)
            continue
        except Exception as e:
            logging.error(f"rpc_fetch_assets error for {app_id_str} (attempt {attempt+1}): {e}")
            if attempt < 2:
                await asyncio.sleep(1)
            continue

async def rpc_get_asset_id(asset_name, app_id=None):
    if not asset_name:
        return None
    apps = load_rpc_apps()

    if app_id:
        cache_key = f"{app_id}:{asset_name}"
        if cache_key not in _rpc_asset_cache:
            await _rpc_fetch_assets_for_app(app_id)
        if cache_key not in _rpc_asset_cache:
            await _rpc_fetch_assets_for_app(app_id, force=True)
        return _rpc_asset_cache.get(cache_key)

    for _ai, _an in apps:
        cache_key = f"{_ai}:{asset_name}"
        if cache_key not in _rpc_asset_cache:
            await _rpc_fetch_assets_for_app(_ai)
        if cache_key not in _rpc_asset_cache:
            await _rpc_fetch_assets_for_app(_ai, force=True)
        result = _rpc_asset_cache.get(cache_key)
        if result:
            return result

    return None

class CustomWebSocket(discord.gateway.DiscordWebSocket):
    async def identify(self):
        payload = {
            'op': 2,
            'd': {
                'token': self.token,
                'properties': {
                    '$os': PLATFORM_PROPS.get(CURRENT_PLATFORM.get("value","desktop"),PLATFORM_PROPS["desktop"])["$os"],
                    '$browser': PLATFORM_PROPS.get(CURRENT_PLATFORM.get("value","desktop"),PLATFORM_PROPS["desktop"])["$browser"],
                    '$device': PLATFORM_PROPS.get(CURRENT_PLATFORM.get("value","desktop"),PLATFORM_PROPS["desktop"])["$device"],
                    '$referrer': '',
                    '$referring_domain': '',
                },
                'compress': True,
                'large_threshold': 250,
                'v': 3,
            },
        }

        if self.shard_id is not None and self.shard_count is not None:
            payload['d']['shard'] = [self.shard_id, self.shard_count]

        state = self._connection
        if state._activity is not None or state._status is not None:
            payload['d']['presence'] = {
                'status': state._status,
                'game': state._activity,
                'since': 0,
                'afk': False,
            }

        if state.intents is not None:
            payload['d']['intents'] = state.intents.value

        await self.send_as_json(payload)

discord.gateway.DiscordWebSocket.identify = CustomWebSocket.identify

discord.http.HTTPClient.static_login = patched_static_login

STATE_FILE = "bot_state.json"
TOKENS_FILE = "tokens.txt"
MESSAGES_FILE = "messages.txt"
POLA_FILE = "test2.txt"
REPLAY_FILE = "test.txt"
BOS_FILE = "test1.txt"
DUME4_FILE = "test.txt"

_SCRIPT_START_TIME = time.time()

running_instances = []
invalid_tokens = set()
token_lock = threading.Lock()
state_file_lock = threading.Lock()
_script_processes: dict = {}
_login_lock = asyncio.Lock()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0"
]

def get_super_properties():
    properties = {
        "os": "Windows",
        "browser": "Discord Client",
        "device": "",
        "system_locale": "en-US",
        "browser_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "browser_version": "131.0.0.0",
        "os_version": "10",
        "referrer": "",
        "referring_domain": "",
        "referrer_current": "",
        "referring_domain_current": "",
        "release_channel": "stable",
        "client_build_number": 346580,
        "client_event_source": None,
        "design_id": 0
    }
    return base64.b64encode(json.dumps(properties).encode()).decode()

def convert_to_mp_external(url: str) -> Optional[str]:
    if not url:
        return None
    if not url.startswith("http"):
        return url
    try:
        return f"mp:external/{url.replace('https://', 'https/')}"
    except:
        return None

async def resolve_image_url(url: str) -> Optional[str]:
    if not url:
        return None
    trimmed = url.strip()

    try:
        from urllib.parse import urlparse
        url_obj = urlparse(trimmed)

        if 'cdn.discordapp.com' in url_obj.hostname or 'media.discordapp.net' in url_obj.hostname:
            return trimmed

        import re
        if re.search(r'\.(png|jpg|jpeg|gif|webp)(\?|$)', trimmed, re.IGNORECASE):
            return trimmed

        if url_obj.hostname == 'i.imgur.com':
            return trimmed

        return trimmed
    except:
        return trimmed

def remove_token_from_file(token: str):
    global invalid_tokens, running_instances
    with token_lock:
        try:
            if not os.path.exists(TOKENS_FILE):
                return

            with open(TOKENS_FILE, "r") as f:
                tokens = [x.strip() for x in f if x.strip()]

            if token in tokens:
                tokens.remove(token)
                invalid_tokens.add(token)

                with open(TOKENS_FILE, "w") as f:
                    f.write("\n".join(tokens))

                print(f"\033[91m[TOKEN INVALID] token removed from {TOKENS_FILE}\033[0m", flush=True)
                logging.warning(f"removed invalid token from {TOKENS_FILE}")

                for inst in running_instances:
                    if inst.token == token:
                        inst.is_valid = False
                        inst.is_running = False
                        print(f"\033[91m[TOKEN INVALID] stopped instance index {inst.token_index}\033[0m", flush=True)
                        logging.warning(f"stopped instance for invalid token (index {inst.token_index})")

                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                asyncio.run_coroutine_threadsafe(inst.bot.close(), loop)
                            else:
                                loop.run_until_complete(inst.bot.close())
                        except Exception:
                            pass

                        try:
                            running_instances.remove(inst)
                        except ValueError:
                            pass

                        break
        except Exception as e:
            logging.error(f"error removing token from file: {e}")

class RateLimitPenalty:
    def __init__(self):
        self.penalty_levels = [2, 5, 10]
        self.current_level = 0
        self.last_rate_limit = 0
        self.recovery_time = 30
        self.consecutive_success = 0
        self.success_threshold = 5
        self.auto_reset_time = 120

    def get_penalty_delay(self) -> float:
        now = time.time()

        if self.current_level > 0 and now - self.last_rate_limit > self.auto_reset_time:
            self.reset()
            logging.info("rate limit penalty auto-reset after inactivity")
            return 0

        if now - self.last_rate_limit > self.recovery_time:
            if self.consecutive_success >= self.success_threshold:
                self.decrease_penalty()
                self.consecutive_success = 0

        if self.current_level <= 0:
            return 0
        if self.current_level > len(self.penalty_levels):
            return self.penalty_levels[-1]
        return self.penalty_levels[self.current_level - 1]

    def on_rate_limit(self):
        self.last_rate_limit = time.time()
        self.consecutive_success = 0
        if self.current_level < len(self.penalty_levels):
            self.current_level += 1
            logging.warning(f"rate limit penalty increased to level {self.current_level} ({self.get_penalty_delay()}s delay)")

    def on_success(self):
        self.consecutive_success += 1
        if self.consecutive_success >= self.success_threshold:
            self.decrease_penalty()
            self.consecutive_success = 0

    def decrease_penalty(self):
        if self.current_level > 0:
            self.current_level -= 1
            logging.info(f"rate limit penalty decreased to level {self.current_level} ({self.get_penalty_delay()}s delay)")

    def reset(self):
        self.current_level = 0
        self.consecutive_success = 0
        self.last_rate_limit = 0

class BypassClient:
    def __init__(self, token: str, token_id: str):
        self.token = token
        self.token_id = token_id
        self.cooldown = {}
        self.queue = deque()
        self.results = {}
        self.result_lock = threading.Lock()
        self.local_reset = 0
        self.global_reset = 0
        self.session = requests.Session()
        self.is_valid = True
        self.running = True
        self.request_id = 0
        self.rate_limit_penalty = RateLimitPenalty()
        self.current_proxy = get_random_proxy()
        self.proxy_retries = 0
        self.max_proxy_retries = 3
        if self.current_proxy:
            logging.info(f"{self.token_id} using proxy: {self.current_proxy}")

    def switch_proxy(self):
        old_proxy = self.current_proxy
        self.current_proxy = get_random_proxy()
        self.proxy_retries = 0
        if self.current_proxy:
            logging.info(f"{self.token_id} switched proxy from {old_proxy} to {self.current_proxy}")
        else:
            logging.warning(f"{self.token_id} no proxies available, using direct connection")

    def get_proxy_dict(self):
        if self.current_proxy:
            return {
                "http": f"http://{self.current_proxy}",
                "https": f"http://{self.current_proxy}"
            }
        return None

    def add_job(self, method, endpoint, json_data=None, data=None, callback=None):
        self.request_id += 1
        req_id = self.request_id
        self.queue.append((method, endpoint, json_data, data, req_id, callback))
        return req_id

    def build_headers(self):
        return {
            "host": "discord.com",
            "connection": "keep-alive",
            "user-agent": random.choice(USER_AGENTS),
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "authorization": self.token,
            "content-type": "application/json",
            "origin": "https://discord.com",
            "referer": "https://discord.com/channels/@me",
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-discord-locale": "en-US",
            "x-discord-timezone": "Europe/Bucharest",
            "x-super-properties": get_super_properties()
        }

    def run(self):
        backoff = 1
        max_backoff = 60

        while self.running:
            if not self.is_valid:
                time.sleep(1)
                continue

            if not self.queue:
                time.sleep(0.05)
                backoff = 1
                continue

            now = time.time()
            if now < self.local_reset or now < self.global_reset:
                wait_time = max(self.local_reset, self.global_reset) - now
                time.sleep(min(wait_time + 0.1, 5))
                continue

            penalty_delay = self.rate_limit_penalty.get_penalty_delay()
            if penalty_delay > 0:
                time.sleep(penalty_delay)

            job = self.queue.popleft()
            method, endpoint, json_data, data, req_id, callback = job

            url = "https://discord.com/api/v10" + endpoint
            headers = self.build_headers()

            try:
                kwargs = {
                    'json': json_data,
                    'data': data,
                    'headers': headers,
                    'timeout': random.uniform(8, 15),
                    'proxies': self.get_proxy_dict()
                }

                r = self.session.request(method, url, **kwargs)

                if "x-ratelimit-reset-after" in r.headers:
                    reset_after = float(r.headers["x-ratelimit-reset-after"])
                    self.local_reset = time.time() + reset_after

                if "x-ratelimit-global" in r.headers:
                    retry_after = float(r.headers.get("retry-after", "1"))
                    self.global_reset = time.time() + retry_after

                if r.status_code == 401:
                    logging.error(f"{self.token_id} token invalid (401)")
                    self.is_valid = False
                    remove_token_from_file(self.token)
                    return

                if r.status_code == 403:
                    self.proxy_retries += 1
                    if self.proxy_retries >= self.max_proxy_retries:
                        self.switch_proxy()
                    logging.warning(f"{self.token_id} forbidden (403) - switching proxy")
                    self.queue.appendleft(job)
                    time.sleep(0.5)
                    continue

                if r.status_code == 429:
                    self.proxy_retries += 1
                    if self.proxy_retries >= self.max_proxy_retries:
                        self.switch_proxy()
                    retry_after = float(r.headers.get("retry-after", "5"))
                    self.local_reset = time.time() + retry_after
                    self.rate_limit_penalty.on_rate_limit()
                    logging.warning(f"{self.token_id} rate limited (429) - switching proxy")
                    self.queue.appendleft(job)
                    time.sleep(min(retry_after, 2))
                    continue

                backoff = 1
                self.proxy_retries = 0
                self.rate_limit_penalty.on_success()

                if callback:
                    try:
                        callback(r.status_code, r.text)
                    except:
                        pass

            except requests.exceptions.Timeout:
                self.proxy_retries += 1
                if self.proxy_retries >= self.max_proxy_retries:
                    self.switch_proxy()
                logging.warning(f"{self.token_id} request timeout - switching proxy")
                self.queue.appendleft(job)
                time.sleep(0.5)
                continue

            except requests.exceptions.ConnectionError:
                self.proxy_retries += 1
                if self.proxy_retries >= self.max_proxy_retries:
                    self.switch_proxy()
                logging.warning(f"{self.token_id} connection error - switching proxy")
                self.queue.appendleft(job)
                time.sleep(0.5)
                continue

            except Exception as e:
                logging.error(f"{self.token_id} request error: {e}")
                time.sleep(0.5)
                continue

    def stop(self):
        self.running = False

class BypassManager:
    def __init__(self, tokens: list):
        self.clients = {}
        for i, token in enumerate(tokens):
            client = BypassClient(token, f"bypass_{i+1}")
            self.clients[token] = client

    def add_token(self, token: str):
        if token not in self.clients:
            idx = len(self.clients)
            client = BypassClient(token, f"bypass_{idx+1}")
            self.clients[token] = client
            threading.Thread(target=client.run, daemon=True).start()

    def remove_token(self, token: str):
        if token in self.clients:
            self.clients[token].stop()
            del self.clients[token]

    def start_all(self):
        for token, client in self.clients.items():
            threading.Thread(target=client.run, daemon=True).start()

    def get_client(self, token: str) -> Optional[BypassClient]:
        return self.clients.get(token)

    def is_token_valid(self, token: str) -> bool:
        client = self.clients.get(token)
        return client.is_valid if client else False

bypass_manager: Optional[BypassManager] = None

async def validate_token_with_username(token: str, max_retries: int = 3) -> tuple:
    headers = {
        'Authorization': token
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get('https://discord.com/api/v10/users/@me', headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('bot'):
                        logging.warning("token validation: FAILED (Bot token detected)")
                        return (False, None, None)
                    username = data.get('username', 'unknown')
                    user_id = data.get('id')
                    logging.info(f"token validation: SUCCESS (200) - user: {username}")
                    return (True, username, user_id)
                else:
                    logging.warning(f"token validation: FAILED ({resp.status})")
                    return (False, None, None)
    except Exception as e:
        logging.error(f"token validation: ERROR - {e}")
        return (False, None, None)

async def validate_token(token: str, max_retries: int = 3) -> bool:
    result = await validate_token_with_username(token, max_retries)
    return result[0]

async def validate_and_clean_tokens():
    if not os.path.exists(TOKENS_FILE):
        logging.error(f"{TOKENS_FILE} not found!")
        return []

    with token_lock:
        with open(TOKENS_FILE, "r") as f:
            tokens = [x.strip() for x in f if x.strip() and not x.strip().startswith('#')]

    if not tokens:
        logging.error(f"no tokens found in {TOKENS_FILE}!")
        return []

    logging.info(f"validating {len(tokens)} tokens...")

    valid_tokens = []
    seen_user_ids = set()
    invalid_count = 0
    duplicate_count = 0

    for idx, token in enumerate(tokens):
        is_valid, username, user_id = await validate_token_with_username(token)

        if is_valid:
            if user_id and user_id in seen_user_ids:
                duplicate_count += 1
                logging.warning(f"token {idx + 1}/{len(tokens)}: duplicate user {username} ({user_id}) - skipping")
            else:
                valid_tokens.append(token)
                if user_id:
                    seen_user_ids.add(user_id)
                logging.info(f"token {idx + 1}/{len(tokens)}: valid")
        else:
            invalid_count += 1
            logging.warning(f"token {idx + 1}/{len(tokens)}: invalid - will be removed")

        await asyncio.sleep(random.uniform(0.5, 1.5))

    if invalid_count > 0 or duplicate_count > 0:
        with token_lock:
            with open(TOKENS_FILE, "w") as f:
                f.write("\n".join(valid_tokens))
        logging.info(f"removed {invalid_count} invalid tokens and {duplicate_count} duplicates from {TOKENS_FILE}")

    logging.info(f"validation complete: {len(valid_tokens)} valid, {invalid_count} invalid, {duplicate_count} duplicates")
    return valid_tokens

def create_default_files():
    if not os.path.exists(MESSAGES_FILE):
        with open(MESSAGES_FILE, 'w') as f:
            f.write("sa\nte\nft\nin\ngat\nsa\nma\npis\npe\nmt\n")
        logging.info(f"created {MESSAGES_FILE}")

    if not os.path.exists(POLA_FILE):
        with open(POLA_FILE, 'w') as f:
            f.write("sa\nsugi\npula\nmea\n")
        logging.info(f"created {POLA_FILE}")

    if not os.path.exists(REPLAY_FILE):
        with open(REPLAY_FILE, 'w') as f:
            f.write("sa\nma\npis\npe\nmt\n")
        logging.info(f"created {REPLAY_FILE}")

    if not os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, 'w') as f:
            f.write("# baga tokens\n")
        logging.info(f"created {TOKENS_FILE}")

    if not os.path.exists(BOS_FILE):
        with open(BOS_FILE, 'w') as f:
            f.write("# sa\nma\npis\npe\ntn\n")
        logging.info(f"created {BOS_FILE}")

class BotInstance:
    def __init__(self, token: str, token_index: int):
        self.token = token
        self.token_index = token_index

        intents = discord.Intents.default()
        intents.members = True
        intents.presences = True

        self.bot = discord.Client(
            reconnect=True,
            heartbeat_timeout=60.0,
            guild_subscriptions=False,
            chunk_guilds_at_startup=False,
            max_messages=100,
            intents=intents
        )

        self.logger = logging.getLogger(f"bot{token_index}")

        self.rate_limit_penalty = RateLimitPenalty()

        self.state = {
            'loops': {},
            'pola_loops': {},
            'baii_loops': {},
            'fatamea_loops': {},
            'plla_loops': {},
            'loops_files': {},
            'pola_loops_files': {},
            'fatamea_loops_files': {},
            'plla_loops_files': {},
            'stopped_by_command': {},
            'delay': 2.0,
            'random_delay': None,
            'channel_delays': {},
            'loop_delays': {},
            'activity': None,
            'commands_enabled': True,
            'rich_presence': [],
            'current_status': 'online',
            'afk_check_enabled': False,
            'afk_check_text': ''
        }

        self.messages = []
        self.message_index = 0
        self.start_time = _SCRIPT_START_TIME
        self.message_cache = {}
        self.pola_messages = []
        self.pola_counters = {}
        self.plla_counters = {}
        self.plla_shuffled = {}
        self.plla_shuffle_pos = {}
        self.fatamea_shuffled = {}
        self.fatamea_shuffle_pos = {}
        self.pola_normal_count = {}
        self.bos_messages = []
        self.dume4_messages = []
        self.fatamea_counters = {}
        self.dume4_counters = {}
        self.tenplm_counters = {}
        self.loops_counters = {}

        self.reconnect_attempts = 0
        self.max_reconnect_delay = 300
        self.is_running = False
        self.is_valid = True

        self.replay_messages = []
        self.replay_counters = {}
        self.auto_replies = {}
        self.auto_reacts = {}
        self.custom_reacts = {}
        self.group_name_changes = {}

        self.current_voice_channel = None
        self.current_dm_voice_channel = None
        self.voice_self_mute = True
        self.voice_self_deaf = True
        self.state_restored = False

        self.health_check_task = None
        self.last_health_check = time.time()

        self.saved_loop_mentions = {}

        self.status_text_task = None
        self.status_text_data = None

        self.auto_reply_queues = {}
        self.auto_reply_tasks = {}
        self.auto_reply_next_send = {}

        self.rpc_stream_data = None
        self.active_streams = []
        self.status_cleared = False
        self.anti_gc_trap = False

        self.typing_tasks = {}

        self.auto_edit_enabled = False

        self.deleted_messages = {}

        self._ready_fired = False

        self._setup_events()

    def get_bypass_client(self) -> Optional[BypassClient]:
        global bypass_manager
        if bypass_manager:
            return bypass_manager.get_client(self.token)
        return None

    def get_channel_delay(self, channel_id):
        base_delay = 0

        if channel_id in self.state['channel_delays']:
            ch_delay = self.state['channel_delays'][channel_id]
            if ch_delay.get('random'):
                base_delay = random.uniform(ch_delay['min'], ch_delay['max'])
            elif ch_delay.get('delay') is not None:
                base_delay = ch_delay['delay']
        elif self.state['random_delay']:
            base_delay = random.uniform(self.state['random_delay']['min'], self.state['random_delay']['max'])
        else:
            base_delay = self.state['delay']

        penalty_delay = self.rate_limit_penalty.get_penalty_delay()
        return base_delay + penalty_delay

    def load_messages(self):
        try:
            if os.path.exists(MESSAGES_FILE):
                with open(MESSAGES_FILE, "r", encoding="utf-8") as f:
                    self.messages = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            if not self.messages:
                self.messages = ['te fut n gat', 'sugi', 'pis pe mt', 'ma cac pe tn', 'te ft n gat']
        except:
            self.messages = ['sugi o pula', 'ma pis pe mt', 'suge mt plm', 'taci fa', 'cameai pla']

    def load_dume_messages(self):
        try:
            if os.path.exists(DUME4_FILE):
                with open(DUME4_FILE, "r", encoding="utf-8") as f:
                    self.dume4_messages = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            if not self.dume4_messages:
                self.dume4_messages = ['samacacpetn']
        except:
            self.dume4_messages = ['samacacpetn']

    def load_pola_messages(self):
        try:
            if os.path.exists(POLA_FILE):
                with open(POLA_FILE, "r", encoding="utf-8") as f:
                    content = f.read()

                    self.pola_messages = [msg.strip() for msg in content.split('\n\n') if msg.strip()]
            if not self.pola_messages:
                self.pola_messages = ['ma pis pe mt', 'ma cac pe tn', 'pis pe tn']
        except:
            self.pola_messages = ['sugi pla', 'mata i crv mea', 'ma pis pe mt']

    def load_custom_messages(self, file_path):
        try:
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                    return [msg.strip() for msg in content.split('\n\n') if msg.strip()]
            return []
        except:
            return []

    def load_replay_messages(self):
        try:
            if os.path.exists(REPLAY_FILE):
                with open(REPLAY_FILE, "r", encoding="utf-8") as f:
                    self.replay_messages = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            if not self.replay_messages:
                self.replay_messages = ['sugi', 'suge mt', 'te ft', 'sugi coiu', 'bag pulan mata']
        except:
            self.replay_messages = ['sugi', 'te fut', 'sugemi pla', 'te iau n pula', 'ma cac pe mata']

    def load_bos_messages(self):
        try:
            if os.path.exists(BOS_FILE):
                with open(BOS_FILE, "r", encoding="utf-8") as f:
                    content = f.read()
                    self.bos_messages = [msg.strip() for msg in content.split('\n\n') if msg.strip()]
            if not self.bos_messages:
                self.bos_messages = ['gura curvo', 'te fut n gat', 'ma pis pe tn']
        except:
            self.bos_messages = ['ma cac pe mt', 'pis pe tn', 'te trag n pla']

    def load_custom_messages(self, filename, force_line_mode=False):
        try:
            if os.path.exists(filename):
                with open(filename, "r", encoding="utf-8") as f:
                    content = f.read()

                messages = []
                current_msg = []
                has_text = False
                lines = content.split('\n')
                is_ghost_ping = False

                for line in lines:
                    stripped = line.strip()
                    if stripped in ['.', '-', '_', ',', '+', '~', '*']:
                        is_ghost_ping = True

                    is_symbol = len(stripped) > 0 and len(stripped) <= 3 and not stripped[0].isalnum() and (not stripped.startswith('#') or len(stripped) == 1)

                    if has_text and is_symbol and current_msg and not current_msg[-1].strip():
                        if any(x.strip() for x in current_msg):
                            messages.append('\n'.join(current_msg).strip())
                        current_msg = [line]
                        has_text = False
                    else:
                        current_msg.append(line)
                        if stripped and not is_symbol:
                            has_text = True

                if current_msg and any(x.strip() for x in current_msg):
                    messages.append('\n'.join(current_msg).strip())

                if len(messages) > 1:
                    return messages
                elif len(messages) == 1 and is_ghost_ping and '\n\n' in content:
                    return messages

                if force_line_mode:
                    return [line.strip() for line in lines if line.strip()]

                if '\n\n' in content:
                    return [msg.strip() for msg in content.split('\n\n') if msg.strip()]
                else:
                    return [line.strip() for line in lines if line.strip()]
        except:
            pass
        return []

    def get_message(self):
        if not self.messages:
            return "regele"
        msg = self.messages[self.message_index % len(self.messages)]
        self.message_index += 1
        return msg

    def get_pola_combined_message(self, channel_id, mentions="", custom_messages=None):
        messages_source = custom_messages if custom_messages else self.pola_messages
        if not messages_source:
            return "pola"
        
        idx = self.pola_counters.get(channel_id, 0)
        msg_block = messages_source[idx % len(messages_source)]
        self.pola_counters[channel_id] = idx + 1
        
        if mentions:
            msg_block = msg_block + " " + mentions
        return msg_block

    def _next_shuffled(self, channel_id, messages_source, shuffled_dict, pos_dict):
        import random
        if channel_id not in shuffled_dict or not shuffled_dict[channel_id]:
            indices = list(range(len(messages_source)))
            random.shuffle(indices)
            shuffled_dict[channel_id] = indices
            pos_dict[channel_id] = 0
        pos = pos_dict.get(channel_id, 0)
        if pos >= len(shuffled_dict[channel_id]):
            indices = list(range(len(messages_source)))
            random.shuffle(indices)
            shuffled_dict[channel_id] = indices
            pos_dict[channel_id] = 0
            pos = 0
        idx = shuffled_dict[channel_id][pos]
        pos_dict[channel_id] = pos + 1
        return messages_source[idx]

    def get_plla_message(self, channel_id, mentions="", custom_messages=None):
        messages_source = custom_messages if custom_messages else self.pola_messages
        if not messages_source:
            return "plla"

        msg_block = self._next_shuffled(channel_id, messages_source, self.plla_shuffled, self.plla_shuffle_pos)

        if mentions:
            msg_block = msg_block + " " + mentions

        return msg_block

    def get_replay_message(self, user_id):
        if not self.replay_messages:
            return "gura"
        idx = self.replay_counters.get(user_id, 0)
        msg = self.replay_messages[idx % len(self.replay_messages)]
        self.replay_counters[user_id] = idx + 1
        return msg

    def get_fatamea_message(self, channel_id, mentions=""):
        if not self.bos_messages:
            return "# > boss ce sunt"

        msg = self._next_shuffled(channel_id, self.bos_messages, self.fatamea_shuffled, self.fatamea_shuffle_pos)

        lines = msg.split('\n')
        prefixed_lines = [f"# > {line}" for line in lines]
        msg = "\n".join(prefixed_lines)

        if mentions:
            msg = msg + " " + mentions

        return msg

    def _setup_events(self):
        @self.bot.event
        async def on_ready():
            if getattr(self, '_ready_fired', False):
                return
            self._ready_fired = True

            self.logger.info(f"logged in as {self.bot.user}")
            self.reconnect_attempts = 0
            self.last_health_check = time.time()
            self.rate_limit_penalty.reset()

            self.load_messages()
            self.load_dume_messages()
            self.load_pola_messages()
            self.load_replay_messages()
            self.load_bos_messages()

            async def preload_rpc_assets():
                for app_id, asset_name in load_rpc_apps():
                    try:
                        url = f"https://discord.com/api/v10/oauth2/applications/{app_id}/assets"
                        async with aiohttp.ClientSession() as session:
                            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                                if resp.status == 200:
                                    alist = await resp.json()
                                    for asset in alist:
                                        name = asset.get("name")
                                        aid = str(asset.get("id"))
                                        _rpc_asset_cache[f"{app_id}:{name}"] = aid
                    except Exception as e:
                        self.logger.error(f"rpc preload error {app_id}: {e}")
            asyncio.create_task(preload_rpc_assets())

            await StateManager.initialize(STATE_FILE)

            await self.restore_state()
            self.state_restored = True

            if not self.health_check_task or self.health_check_task.done():
                self.health_check_task = asyncio.create_task(self._health_check_loop())

            if not getattr(self, 'memory_cleaner_task', None) or self.memory_cleaner_task.done():
                self.memory_cleaner_task = asyncio.create_task(self._memory_cleaner_loop())

        @self.bot.event
        async def on_disconnect():
            self.logger.warning("disconnected from discord")

        @self.bot.event
        async def on_resumed():
            self.logger.info("connection resumed")
            self.reconnect_attempts = 0
            self.rate_limit_penalty.reset()

        @self.bot.event
        async def on_user_update(before, after):
            uid = after.id
            new_name = after.name
            updated = False
            if uid in self.auto_replies:
                if self.auto_replies[uid].get('user_name') != new_name:
                    self.auto_replies[uid]['user_name'] = new_name
                    updated = True
            if uid in self.auto_reacts:
                if self.auto_reacts[uid].get('user_name') != new_name:
                    self.auto_reacts[uid]['user_name'] = new_name
                    updated = True
            if updated:
                self.logger.info(f"auto-updated username for {uid}: {before.name} -> {new_name}")
                await self.save_state()

        @self.bot.event
        async def on_private_channel_create(channel):
            if not self.anti_gc_trap:
                return
            if not isinstance(channel, discord.GroupChannel):
                return
            try:
                total = len(channel.recipients) + 1
                if total >= 10:
                    self.logger.info(f"anti_gc_trap: full group detected ({total}/10), silently leaving {channel.id}")
                    asyncio.create_task(self._do_silentleave(channel.id))
            except Exception as e:
                self.logger.error(f"anti_gc_trap error: {e}")

        @self.bot.event
        async def on_message_delete(message):
            try:
                deleted_by = None
                deleted_by_id = None
                
                try:
                    guild = message.guild
                    if guild and guild.me.guild_permissions.view_audit_log:
                        async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.message_delete):
                            if entry.target.id == message.author.id and entry.extra.channel.id == message.channel.id:
                                deleted_by = entry.user.name
                                deleted_by_id = entry.user.id
                                break
                except:
                    pass
                
                channel_id = message.channel.id
                text_parts = []
                if message.content:
                    text_parts.append(message.content)
                clean_content = getattr(message, 'clean_content', None)
                if clean_content and clean_content != message.content:
                    text_parts.append(clean_content)
                system_content = getattr(message, 'system_content', None)
                if system_content and system_content not in text_parts:
                    text_parts.append(system_content)

                embed_summaries = []
                for embed in message.embeds:
                    if not embed:
                        continue
                    parts = []
                    if getattr(embed, 'title', None):
                        parts.append(str(embed.title))
                    if getattr(embed, 'description', None):
                        parts.append(str(embed.description))
                    if getattr(embed, 'author', None) and getattr(embed.author, 'name', None):
                        parts.append(str(embed.author.name))
                    if getattr(embed, 'fields', None):
                        for field in embed.fields:
                            if getattr(field, 'name', None) and getattr(field, 'value', None):
                                parts.append(f"{field.name}: {field.value}")
                    if parts:
                        embed_text = " | ".join(parts)
                        embed_summaries.append(embed_text)
                        if embed_text not in text_parts:
                            text_parts.append(embed_text)
                    else:
                        embed_summaries.append("<embed>")

                if getattr(message, 'stickers', None):
                    sticker_names = [sticker.name for sticker in message.stickers if getattr(sticker, 'name', None)]
                    if sticker_names:
                        text_parts.append("stickers: " + ", ".join(sticker_names))

                content_value = " | ".join(text_parts).strip()
                attachment_urls = [att.url for att in message.attachments]
                image_urls = [url for url in attachment_urls if url.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp'))]
                for embed in message.embeds:
                    if getattr(embed, 'image', None) and getattr(embed.image, 'url', None):
                        image_urls.append(embed.image.url)
                    if getattr(embed, 'thumbnail', None) and getattr(embed.thumbnail, 'url', None):
                        image_urls.append(embed.thumbnail.url)

                entry = {
                    'author': message.author.name,
                    'author_id': message.author.id,
                    'content': content_value,
                    'deleted_by': deleted_by or "unknown",
                    'deleted_by_id': deleted_by_id or 0,
                    'timestamp': message.created_at.strftime("%H:%M:%S"),
                    'attachments': attachment_urls,
                    'images': image_urls,
                    'embeds': embed_summaries
                }
                self.deleted_messages[channel_id] = entry
                self.logger.info(f"snipe: saved deleted message from {channel_id}")
            except Exception as e:
                self.logger.error(f"snipe save error: {e}")

        @self.bot.event
        async def on_message(message):
            if message.author.id == self.bot.user.id and self.auto_edit_enabled:
                async def do_auto_edit():
                    try:
                        await asyncio.sleep(0.3)
                        lines = message.content.split('\n')
                        new_content = '\n'.join([f"# {line}" for line in lines])
                        await message.edit(content=new_content)
                    except:
                        pass
                asyncio.create_task(do_auto_edit())

            if message.author.id in self.auto_reacts:
                async def do_react():
                    try:
                        emoji_str = self.auto_reacts[message.author.id].get('emoji', '')

                        import re
                        custom_emoji_match = re.match(r'<(a?):([^:]+):(\d+)>', emoji_str)
                        if custom_emoji_match:
                            animated = custom_emoji_match.group(1) == 'a'
                            emoji_name = custom_emoji_match.group(2)
                            emoji_id = int(custom_emoji_match.group(3))
                            emoji = discord.PartialEmoji(name=emoji_name, id=emoji_id, animated=animated)
                        else:
                            emoji = emoji_str

                        await message.add_reaction(emoji)
                    except Exception as e:
                        error_str = str(e)
                        if '10014' in error_str or 'Emoji not found' in error_str:
                            if message.author.id in self.auto_reacts:
                                del self.auto_reacts[message.author.id]
                                await self.save_state()
                        else:
                            self.logger.error(f"auto-react error for {message.author.id}: {e}")
                asyncio.create_task(do_react())

            if message.author.id != self.bot.user.id:
                if message.channel.id in self.custom_reacts:
                    if message.author.id in self.custom_reacts[message.channel.id]:
                        react_data = self.custom_reacts[message.channel.id][message.author.id]
                        trigger = react_data.get('trigger', '').strip().lower()
                        msg_content = message.content.lower() if message.content else ''

                        should_react = False
                        if not trigger:
                            should_react = True
                        elif trigger in msg_content:
                            should_react = True

                        if should_react:
                            async def do_custom_react():
                                try:
                                    emoji_str = react_data.get('emoji', '🔥')
                                    import re
                                    custom_emoji_match = re.match(r'<(a?):([^:]+):(\d+)>', emoji_str)
                                    if custom_emoji_match:
                                        animated = custom_emoji_match.group(1) == 'a'
                                        emoji_name = custom_emoji_match.group(2)
                                        emoji_id = int(custom_emoji_match.group(3))
                                        emoji = discord.PartialEmoji(name=emoji_name, id=emoji_id, animated=animated)
                                    else:
                                        emoji = emoji_str
                                    await message.add_reaction(emoji)
                                except: pass
                            asyncio.create_task(do_custom_react())

                if message.author.id in self.auto_replies:
                    target_data = self.auto_replies[message.author.id]
                    any_channel = target_data.get('any_channel', False)
                    if any_channel or message.channel.id == target_data.get('channel_id'):
                        await self.queue_auto_reply(message)

                if message.author.id in self.auto_reacts:
                    async def do_react():
                        try:
                            emoji_str = self.auto_reacts[message.author.id].get('emoji', '')
                            import re
                            custom_emoji_match = re.match(r'<(a?):([^:]+):(\d+)>', emoji_str)
                            if custom_emoji_match:
                                animated = custom_emoji_match.group(1) == 'a'
                                emoji_name = custom_emoji_match.group(2)
                                emoji_id = int(custom_emoji_match.group(3))
                                emoji = discord.PartialEmoji(name=emoji_name, id=emoji_id, animated=animated)
                            else:
                                emoji = emoji_str
                            await message.add_reaction(emoji)
                        except: pass
                    asyncio.create_task(do_react())

                if message.author.id in self.auto_replies or message.author.id in self.auto_reacts:
                    if not (running_instances and any(inst.bot.user and message.author.id == inst.bot.user.id for inst in running_instances)):
                        return

            if not message.content:
                return

            if self.state.get('afk_check_enabled', False) and message.author.id != self.bot.user.id:
                if self.bot.user in message.mentions and self.state.get('afk_check_text'):
                    try:
                        await message.reply(self.state['afk_check_text'])
                    except Exception as e:
                        self.logger.error(f"afk check error: {e}")

            if isinstance(message.channel, (discord.DMChannel, discord.GroupChannel)):
                allowed = False
                if message.author.id == self.bot.user.id:
                    allowed = True
                elif running_instances and any(inst.bot.user and message.author.id == inst.bot.user.id for inst in running_instances):
                    allowed = True

                if not allowed:
                    return

            is_our_bot = False
            if message.author.id == self.bot.user.id:
                is_our_bot = True
            elif running_instances and any(inst.bot.user and message.author.id == inst.bot.user.id for inst in running_instances):
                is_our_bot = True

            if not is_our_bot:
                return

            content = message.content.strip()

            if CMD_PREFIX and not content.startswith(CMD_PREFIX):
                return
            if CMD_PREFIX:
                content = content[len(CMD_PREFIX):].strip()

            parts = content.split(maxsplit=1)
            if not parts:
                return

            raw_cmd = parts[0].lower()
            args = parts[1] if len(parts) > 1 else ""

            main_bot_id = None
            if running_instances and running_instances[0].bot.user:
                main_bot_id = running_instances[0].bot.user.id

            cmd = raw_cmd
            target_index = None
            target_all = False

            if raw_cmd == "tarfoo":
                cmd = "start"
            elif raw_cmd == "mil":
                cmd = "stop"
            elif raw_cmd == "morii":
                cmd = "peleu"
            elif raw_cmd == "cacatulee":
                cmd = "cee"

            import re

            match_count = re.match(r"^([a-z]+)(\d+)$", raw_cmd)
            if match_count:
                if main_bot_id and message.author.id != main_bot_id:
                    return

                cmd = match_count.group(1)
                target_index = int(match_count.group(2)) - 1

                if self.token_index != 0:
                    return
            elif raw_cmd.endswith("all") and raw_cmd not in ["stoploops", "cstreamall", "rrall", "schgall", "sujistopall", "faststartall", "stopfastall"]:
                base_cmd = raw_cmd[:-3]
                if main_bot_id and message.author.id != main_bot_id:
                    return
                if self.token_index == 0:
                    target_all = True
                    cmd = base_cmd
            else:
                if message.author.id == self.bot.user.id:
                    target_index = self.token_index

            all_commands = [
                "morii", "peleu", "pizdoo", "jegule", "startnotepad", "cacatulee", "cee", "pola", "startnotepadshift", "tee", "mil",
                "stream", "play",
                "status", "stoploops", "loops", "info",
                "uptime", "mainuptime", "ping", "clear", "silentleave", "setprefix",
                "help", "suji", "sogi", "replynotepad", "taci", "sujilist", "sujilistall", "sujistopall", "sujistopalltks", "r", "rr", "rrall", "rralltks",
                "rlist", "reload", "login", "chg", "schg", "schgall",
                "chglist", "jvc", "lvc", "tkuser", "tklist", "addgc", "rmgc", "crgc", "delay", "rdelay", "dlist",
                "jvcall", "lvcall", "tarfoo", "start", "mil", "stop", "fatamea", "startnotepadspiced", "tragpla", "cleardlist", "vusername", "vtoken",
                "av", "banner", "serverav", "serverbanner",
                "stopnotepad", "stopnotepadshift", "stopnotepadspiced",
                "react", "say", "edit",
                "startscript", "stopscript", "scriptstatus", "stopscriptall",
                "hypesquad",
                "tokenbased", "tokenbasedstop", "faststartall", "stopfastall", "presence", "watch", "competing", "listening",
                "typing",
                "antigctrap",
                "afkcheck", "afkchecktext", "snipe",
                "pula", "gay", "ship", "hack", "addtoken", "cgcn", "lookup", "tsend"
            ]

            no_delete_cmds = [
                "help", "uptime", "mainuptime", "ping", "peleu", "pizdoo", "pola",
                "fatamea", "tragpla", "cee", "tee", "suji", "sogi", "taci",
                "loops", "chglist", "sujilist", "sujilistall", "rlist", "jegule",
                "pula", "gay", "ship", "hack", "lookup"
            ]

            if raw_cmd.endswith("all") and raw_cmd not in ["stoploops", "cstreamall", "rrall", "schgall", "sujistopall", "faststartall", "stopfastall"]:
                base_cmd = raw_cmd[:-3]

                if self.token_index == 0:
                    target_all = True
                    cmd = base_cmd

            if raw_cmd in ["sujistopall", "stoploops"]:
                target_all = False
                cmd = raw_cmd

            if cmd == "suji" and target_index == self.token_index:
                pass

            stop_cmds = ["cee", "tee", "saami", "mil", "jegule", "tragpla", "tfut", "stopnotepad", "stopnotepadshift", "stopnotepadspiced","stop"]

            if target_all:
                if self.token_index != 0:
                    return

                if (cmd in all_commands or any(cmd.startswith(x) and cmd[len(x):].isdigit() for x in ["login"])) and cmd not in no_delete_cmds and raw_cmd not in ["tarfoo", "mil"]:
                    try:
                        await message.delete()
                    except:
                        pass

                await self._handle_command(message, cmd, args)

                class ProxyMessage:
                    def __init__(self, original, ch):
                        self.id, self.content, self.author = original.id, original.content, original.author
                        self.channel, self.guild = ch, getattr(ch, 'guild', None)
                        self.reference, self.attachments = original.reference, original.attachments
                        self.embeds, self.mentions = original.embeds, original.mentions
                        self.created_at = original.created_at
                        self._original = original

                    async def delete(self):
                        try:
                            await self._original.delete()
                        except:
                            pass

                NO_CHANNEL_CMDS = {
                    "r", "rr", "rrall", "rralltks",
                    "suji", "sogi", "taci", "sujistopall", "sujistopalltks",
                    "status", "hypesquad", "reload",
                    "delay", "rdelay", "cleardlist",
                    "startscript", "stopscript", "scriptstatus", "stopscriptall",
                    "tokenbased", "tokenbasedstop", "faststartall", "stopfastall", "presence", "watch", "competing", "listening",
                    "stoploops", "jvc", "lvc", "jvcall", "lvcall",
                    "stream", "play",
                }

                for inst in running_instances:
                    if inst.token_index != 0:
                        try:
                            if cmd in NO_CHANNEL_CMDS:
                                await inst._handle_command(ProxyMessage(message, message.channel), cmd, args)
                                continue
                            _resolved_channel = None
                            _args_parts = args.strip().split(maxsplit=1)
                            _has_explicit_channel = _args_parts and _args_parts[0].isdigit() and len(_args_parts[0]) > 10
                            if _has_explicit_channel:
                                _cid_from_args = int(_args_parts[0])
                                _resolved_channel = inst.bot.get_channel(_cid_from_args)
                                if not _resolved_channel:
                                    try: _resolved_channel = await inst.bot.fetch_channel(_cid_from_args)
                                    except: pass
                                if not _resolved_channel:
                                    continue
                            else:
                                _resolved_channel = inst.bot.get_channel(message.channel.id)
                                if not _resolved_channel:
                                    try: _resolved_channel = await inst.bot.fetch_channel(message.channel.id)
                                    except: pass
                                if not _resolved_channel:
                                    continue
                            await inst._handle_command(ProxyMessage(message, _resolved_channel), cmd, args)
                        except Exception as e:
                            pass
                return

            if target_index != self.token_index:
                if self.token_index != 0:

                    return

                if (cmd in all_commands or any(cmd.startswith(x) and cmd[len(x):].isdigit() for x in ["login"])) and cmd not in no_delete_cmds and raw_cmd not in ["tarfoo", "mil"]:
                    try:
                        await message.delete()
                    except:
                        pass

                target_instance = next((inst for inst in running_instances if inst.token_index == target_index), None)
                if not target_instance:
                    return

                NO_CHANNEL_CMDS = {
                    "r", "rr", "rrall", "rralltks",
                    "suji", "sogi", "taci", "sujistopall", "sujistopalltks",
                    "status", "hypesquad", "reload",
                    "delay", "rdelay", "cleardlist",
                    "startscript", "stopscript", "scriptstatus", "stopscriptall",
                    "tokenbased", "tokenbasedstop", "faststartall", "stopfastall", "presence", "watch", "competing", "listening",
                    "stoploops", "jvc", "lvc", "jvcall", "lvcall",
                    "stream", "play",
                }

                try:
                    if cmd in NO_CHANNEL_CMDS:
                        class ProxyMessage:
                            def __init__(self, original, ch):
                                self.id, self.content, self.author = original.id, original.content, original.author
                                self.channel, self.guild = ch, getattr(ch, 'guild', None)
                                self.reference, self.attachments = original.reference, original.attachments
                                self.embeds, self.mentions = original.embeds, original.mentions
                                self.created_at = original.created_at
                                self._original = original
                            async def delete(self):
                                try:
                                    await self._original.delete()
                                except:
                                    pass
                        await target_instance._handle_command(ProxyMessage(message, message.channel), cmd, args)
                        return
                    _resolved_channel = None
                    _args_parts = args.strip().split(maxsplit=1)
                    _has_explicit_channel = _args_parts and _args_parts[0].isdigit() and len(_args_parts[0]) > 10
                    if _has_explicit_channel:
                        _cid_from_args = int(_args_parts[0])
                        _resolved_channel = target_instance.bot.get_channel(_cid_from_args)
                        if not _resolved_channel:
                            try: _resolved_channel = await target_instance.bot.fetch_channel(_cid_from_args)
                            except: pass
                        if not _resolved_channel:
                            return
                    else:
                        _resolved_channel = target_instance.bot.get_channel(message.channel.id)
                        if not _resolved_channel:
                            try: _resolved_channel = await target_instance.bot.fetch_channel(message.channel.id)
                            except: pass
                        if not _resolved_channel:
                            return

                    class ProxyMessage:
                        def __init__(self, original, ch):
                            self.id, self.content, self.author = original.id, original.content, original.author
                            self.channel, self.guild = ch, getattr(ch, 'guild', None)
                            self.reference, self.attachments = original.reference, original.attachments
                            self.embeds, self.mentions = original.embeds, original.mentions
                            self.created_at = original.created_at
                            self._original = original

                        async def delete(self):
                            try:
                                await self._original.delete()
                            except:
                                pass

                    await target_instance._handle_command(ProxyMessage(message, _resolved_channel), cmd, args)
                    return
                except Exception as e:
                    pass
                    return

            is_valid_cmd = cmd in all_commands or any(cmd.startswith(x) and cmd[len(x):].isdigit() for x in ["login"])

            if not is_valid_cmd:
                return

            if cmd not in no_delete_cmds and raw_cmd not in ["tarfoo", "mil"]:
                try:
                    await message.delete()
                except:
                    pass

            await self._handle_command(message, cmd, args)

    async def queue_auto_reply(self, message):
        user_id = message.author.id

        if user_id not in self.auto_replies:
            return

        target_data = self.auto_replies[user_id]
        command_channel_id = target_data.get('channel_id')
        any_channel = target_data.get('any_channel', False)
        if not any_channel and message.channel.id != command_channel_id:
            return

        current_name = message.author.name
        if target_data.get('user_name') != current_name:
            self.auto_replies[user_id]['user_name'] = current_name
            self.logger.info(f"live username update for {user_id}: {target_data.get('user_name')} -> {current_name}")

        if user_id not in self.auto_reply_queues:
            self.auto_reply_queues[user_id] = deque()

        custom_file = target_data.get('custom_file')
        if custom_file:
            msgs = self.load_custom_messages(custom_file, force_line_mode=True)
            if not msgs:
                reply_msg = self.get_replay_message(user_id)
            else:
                idx = self.replay_counters.get(user_id, 0)
                reply_msg = msgs[idx % len(msgs)]
                self.replay_counters[user_id] = idx + 1
        else:
            reply_msg = self.get_replay_message(user_id)

        self.auto_reply_queues[user_id].append({
            'channel': message.channel,
            'original_message': message,
            'message': reply_msg,
            'queued_at': time.time(),
            'target_send_time': None
        })

        if user_id not in self.auto_reply_tasks or self.auto_reply_tasks[user_id].done():
            self.auto_reply_tasks[user_id] = asyncio.create_task(self.process_auto_reply_queue(user_id))

    async def process_auto_reply_queue(self, user_id):
        while user_id in self.auto_replies:
            try:
                if not self.auto_reply_queues.get(user_id):
                    await asyncio.sleep(0.1)
                    continue

                now = time.time()
                next_send = self.auto_reply_next_send.get(user_id, 0)

                if now < next_send:
                    await asyncio.sleep(next_send - now)

                if user_id not in self.auto_reply_queues or not self.auto_reply_queues[user_id]:
                    continue

                item = self.auto_reply_queues[user_id].popleft()
                channel = item['channel']
                original_message = item.get('original_message')
                reply_msg = item['message']
                queued_at = item.get('queued_at', now)

                delay = self.get_channel_delay(channel.id)
                self.auto_reply_next_send[user_id] = time.time() + delay

                time_behind = time.time() - queued_at
                use_reply = time_behind <= 5

                try:
                    try:
                        async with channel.typing():
                            typing_duration = max(0.5, min(len(reply_msg) * 0.05, 3.0))
                            await asyncio.sleep(typing_duration)
                    except:
                        await asyncio.sleep(max(0.5, min(len(reply_msg) * 0.05, 3.0)))

                    if original_message:
                        if use_reply:
                            try:
                                await original_message.reply(reply_msg, mention_author=True)
                            except:
                                await channel.send(reply_msg)
                        else:
                            try:
                                user_tag = f"<@{original_message.author.id}>"
                                await channel.send(f"{user_tag} {reply_msg}")
                            except:
                                await channel.send(reply_msg)
                    else:
                        await channel.send(reply_msg)
                except Exception as e:
                    self.logger.error(f"auto-reply send error: {e}")

            except Exception as e:
                self.logger.error(f"auto reply loop error: {e}")
                await asyncio.sleep(1)

    async def _health_check_loop(self):
        while self.is_running and self.is_valid:
            try:
                await asyncio.sleep(60)
                self.last_health_check = time.time()

                if not self.bot.is_closed() and self.bot.user:
                    await self.save_state()
                else:
                    self.logger.warning("health check failed - bot appears disconnected")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"health check error: {e}")

    async def _memory_cleaner_loop(self):
        while self.is_running and self.is_valid:
            try:
                await asyncio.sleep(60)

                sbc = self.state['stopped_by_command']
                active_keys = set()
                for cid in self.state['loops']: active_keys.add(cid); active_keys.add(str(cid))
                for cid in self.state['pola_loops']: active_keys.add(f"pola_{cid}")
                for cid in self.state['plla_loops']: active_keys.add(f"plla_{cid}")
                for cid in self.state.get('baii_loops', {}): active_keys.add(f"baii_{cid}")
                for cid in self.state['fatamea_loops']: active_keys.add(f"fatamea_{cid}")
                for cid in self.group_name_changes: active_keys.add(f"chg_{cid}")
                for uid in self.auto_replies: active_keys.add(f"suji_{uid}")
                for k in [k for k in list(sbc.keys()) if k not in active_keys]:
                    del sbc[k]

                bypass_client = self.get_bypass_client()
                if bypass_client and len(bypass_client.results) > 50:
                    with bypass_client.result_lock:
                        bypass_client.results.clear()

                active_channel_ids = (
                    set(self.state['loops'].keys()) | set(self.state['pola_loops'].keys()) |
                    set(self.state['plla_loops'].keys()) | set(self.state['fatamea_loops'].keys()) |
                    set(self.state.get('tenplm_loops', {}).keys()) |
                    set(self.group_name_changes.keys())
                )
                for dname in ('plla_counters','pola_counters','fatamea_counters','loops_counters','tenplm_counters','dume4_counters'):
                    d = getattr(self, dname, None)
                    if d:
                        for k in [k for k in list(d.keys()) if k not in active_channel_ids]:
                            del d[k]

                for dname in ('plla_shuffled', 'plla_shuffle_pos', 'fatamea_shuffled', 'fatamea_shuffle_pos'):
                    d = getattr(self, dname, None)
                    if d:
                        for k in [k for k in list(d.keys()) if k not in active_channel_ids]:
                            del d[k]

                for dname in ('_loop_locks', '_tenplm_locks'):
                    d = getattr(self, dname, None)
                    if d:
                        for k in [k for k in list(d.keys()) if k not in active_channel_ids]:
                            del d[k]

                all_active_mention_keys = set()
                for cid in self.state['loops']: all_active_mention_keys.add(cid)
                for cid in self.state['pola_loops']: all_active_mention_keys.add(f"pola_{cid}")
                for cid in self.state['plla_loops']: all_active_mention_keys.add(f"plla_{cid}")
                for cid in self.state['fatamea_loops']: all_active_mention_keys.add(f"fatamea_{cid}")
                for k in [k for k in list(self.saved_loop_mentions.keys()) if k not in all_active_mention_keys]:
                    del self.saved_loop_mentions[k]

                active_uids = set(self.auto_replies.keys())
                for k in [k for k in list(self.replay_counters.keys()) if k not in active_uids]:
                    del self.replay_counters[k]

                if len(self.pola_normal_count) > 100:
                    self.pola_normal_count.clear()

                for uid in [uid for uid in list(self.auto_reply_queues.keys()) if uid not in self.auto_replies]:
                    self.auto_reply_queues[uid].clear()
                    del self.auto_reply_queues[uid]
                    if uid in self.auto_reply_tasks:
                        t = self.auto_reply_tasks.pop(uid)
                        if not t.done(): t.cancel()
                    self.auto_reply_next_send.pop(uid, None)

                for uid in [uid for uid, t in list(self.auto_reply_tasks.items()) if t.done()]:
                    del self.auto_reply_tasks[uid]

                try:
                    msgs = self.bot._connection._messages
                    if msgs and len(msgs) > 2000:
                        while len(msgs) > 1000: msgs.popleft()
                except Exception:
                    pass

                if len(self.message_cache) > 500:
                    self.message_cache.clear()

                gc.collect()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"memory cleaner error: {e}")
                await asyncio.sleep(60)

    async def restore_state(self):
        if any(len(self.state[k]) > 0 for k in ['loops', 'pola_loops', 'baii_loops', 'fatamea_loops', 'plla_loops']):
            self.logger.info("restore_state: loops already running, skipping duplicate restore")
            return

        self.logger.info("restore_state: Starting state restoration...")

        try:
            all_states = await StateManager.load(STATE_FILE)
            self.logger.info(f"restore_state: Loaded {len(all_states)} token states from file")

            my_user_id = str(self.bot.user.id) if self.bot.user else None
            state_data = None
            state_key = None

            if my_user_id:
                for key, data in all_states.items():
                    if str(data.get('user_id', '')) == my_user_id:
                        state_data = data
                        state_key = key
                        self.logger.info(f"restore_state: found state by user_id {my_user_id} (was at index {key})")
                        break

            if state_data is None:
                fallback_key = str(self.token_index)
                if fallback_key in all_states and not all_states[fallback_key].get('user_id'):
                    state_data = all_states[fallback_key]
                    state_key = fallback_key
                    self.logger.info(f"restore_state: fallback to token_index {fallback_key} (no user_id in state)")

            if state_data is None:
                self.logger.info(f"restore_state: no saved state found for this token, starting fresh")
                return

            default_state = {
                'loops': [], 'loops_mentions': {}, 'loops_files': {},
                'pola_loops': [], 'pola_loops_mentions': {}, 'pola_loops_files': {},
                'plla_loops': [], 'plla_loops_mentions': {}, 'plla_loops_files': {},
                'baii_loops': {}, 'fatamea_loops': [], 'tenplm_loops': {}, 'fatamea_loops_mentions': {},
                'fatamea_loops_files': {}, 'delay': 2.0, 'random_delay': None,
                'channel_delays': {}, 'voice': {}, 'auto_replies': {}, 'auto_reacts': {},
                'group_name_changes': {}, 'typing_tasks': [], 'stopped_by_command': {},
                'loops_counters': {}, 'pola_counters': {}, 'fatamea_counters': {}
            }
            for key, default_val in default_state.items():
                if key not in state_data:
                    state_data[key] = default_val

            self.logger.info(f"restoring state for index {state_key}...")

            if 'script_start_time' in state_data:
                saved_script_start = state_data['script_start_time']
                last_save = state_data.get('last_save', 0)
                downtime = time.time() - last_save
                if downtime < 7 * 60:
                    global _SCRIPT_START_TIME
                    _SCRIPT_START_TIME = saved_script_start
            self.start_time = _SCRIPT_START_TIME

            if 'delay' in state_data: self.state['delay'] = state_data['delay']
            if 'random_delay' in state_data: self.state['random_delay'] = state_data['random_delay']
            if 'channel_delays' in state_data:
                self.state['channel_delays'] = {int(k): v for k, v in state_data['channel_delays'].items()}
            if 'current_status' in state_data: self.state['current_status'] = state_data['current_status']
            if 'afk_check_enabled' in state_data: self.state['afk_check_enabled'] = state_data['afk_check_enabled']
            if 'afk_check_text' in state_data: self.state['afk_check_text'] = state_data['afk_check_text']

            if 'stopped_by_command' in state_data:
                for key_str, value in state_data['stopped_by_command'].items():
                    if key_str.isdigit():
                        self.state['stopped_by_command'][int(key_str)] = value
                    else:
                        self.state['stopped_by_command'][key_str] = value

            if 'loops_counters' in state_data:
                self.loops_counters = {int(k): v for k, v in state_data['loops_counters'].items()}
            if 'pola_counters' in state_data:
                self.pola_counters = {int(k): v for k, v in state_data['pola_counters'].items()}
            if 'fatamea_counters' in state_data:
                self.fatamea_counters = {int(k): v for k, v in state_data['fatamea_counters'].items()}

            if 'loops_files' in state_data:
                self.state['loops_files'] = {int(k): v for k, v in state_data['loops_files'].items()}
            if 'pola_loops_files' in state_data:
                self.state['pola_loops_files'] = {int(k): v for k, v in state_data['pola_loops_files'].items()}
            if 'fatamea_loops_files' in state_data:
                self.state['fatamea_loops_files'] = {int(k): v for k, v in state_data['fatamea_loops_files'].items()}

            if 'baii_loops' in state_data:
                for channel_id_str, text in state_data['baii_loops'].items():
                    try:
                        channel_id = int(channel_id_str)
                        if not self.state['stopped_by_command'].get(f"baii_{channel_id}"):
                            channel = self.bot.get_channel(channel_id)
                            if not channel: channel = await self.bot.fetch_channel(channel_id)
                            if channel:
                                await self.start_baii_loop(channel, text)
                                self.logger.info(f"restored baii loop for {channel_id}")
                    except Exception as e:
                        self.logger.error(f"failed to restore baii loop {channel_id_str}: {e}")

            if 'loops' in state_data:
                loop_mentions = state_data.get('loops_mentions', {})
                for channel_id in state_data['loops']:
                    try:
                        channel_id = int(channel_id) if isinstance(channel_id, str) else channel_id
                        is_stopped = self.state['stopped_by_command'].get(channel_id) or \
                                   self.state['stopped_by_command'].get(str(channel_id))

                        if not is_stopped:
                            channel = self.bot.get_channel(channel_id)
                            if not channel: channel = await self.bot.fetch_channel(channel_id)
                            if channel:
                                mentions = loop_mentions.get(str(channel_id), "")
                                custom_file = self.state['loops_files'].get(channel_id)
                                await self.start_pl_loop(channel, mentions, custom_file=custom_file)
                                self.logger.info(f"restored pl loop for {channel_id}")
                    except Exception as e:
                        self.logger.error(f"failed to restore pl loop {channel_id}: {e}")

            if 'pola_loops' in state_data:
                pola_mentions = state_data.get('pola_loops_mentions', {})
                for channel_id in state_data['pola_loops']:
                    try:
                        channel_id = int(channel_id) if isinstance(channel_id, str) else channel_id
                        if not self.state['stopped_by_command'].get(f"pola_{channel_id}"):
                            channel = self.bot.get_channel(channel_id)
                            if not channel: channel = await self.bot.fetch_channel(channel_id)
                            if channel:
                                mentions = pola_mentions.get(str(channel_id), "")
                                custom_file = self.state['pola_loops_files'].get(channel_id)
                                await self.start_pola_loop(channel, mentions, custom_file=custom_file)
                                self.logger.info(f"restored pola loop for {channel_id}")
                    except Exception as e:
                        self.logger.error(f"failed to restore pola loop {channel_id}: {e}")

            if 'plla_loops' in state_data:
                plla_mentions = state_data.get('plla_loops_mentions', {})
                for channel_id in state_data['plla_loops']:
                    try:
                        channel_id = int(channel_id) if isinstance(channel_id, str) else channel_id
                        if not self.state['stopped_by_command'].get(f"plla_{channel_id}"):
                            channel = self.bot.get_channel(channel_id)
                            if not channel: channel = await self.bot.fetch_channel(channel_id)
                            if channel:
                                mentions = plla_mentions.get(str(channel_id), "")
                                custom_file = state_data.get('plla_loops_files', {}).get(str(channel_id))
                                await self.start_plla_loop(channel, mentions, custom_file=custom_file)
                                self.logger.info(f"restored plla loop for {channel_id}")
                    except Exception as e:
                        self.logger.error(f"failed to restore plla loop {channel_id}: {e}")

            if 'fatamea_loops' in state_data:
                fatamea_mentions = state_data.get('fatamea_loops_mentions', {})
                for channel_id in state_data['fatamea_loops']:
                    try:
                        channel_id = int(channel_id) if isinstance(channel_id, str) else channel_id
                        if not self.state['stopped_by_command'].get(f"fatamea_{channel_id}"):
                            channel = self.bot.get_channel(channel_id)
                            if not channel: channel = await self.bot.fetch_channel(channel_id)
                            if channel:
                                mentions = fatamea_mentions.get(str(channel_id), "")
                                custom_file = self.state['fatamea_loops_files'].get(channel_id)
                                await self.start_fatamea_loop(channel, mentions, custom_file=custom_file)
                                self.logger.info(f"restored fatamea loop for {channel_id}")
                    except Exception as e:
                        self.logger.error(f"failed to restore fatamea loop {channel_id}: {e}")

            if 'group_name_changes' in state_data:
                for channel_id_str, data in state_data['group_name_changes'].items():
                    try:
                        channel_id = int(channel_id_str)
                        if not self.state['stopped_by_command'].get(f"chg_{channel_id}"):
                            names = data.get('names', [])
                            delay = data.get('delay', 10)
                            channel = self.bot.get_channel(channel_id)
                            if channel:
                                await self.start_group_name_loop(channel, names, delay)
                                self.logger.info(f"restored group name change for {channel_id}")
                    except Exception as e:
                        self.logger.error(f"failed to restore group name change {channel_id_str}: {e}")

            if 'typing_tasks' in state_data:
                for channel_id in state_data['typing_tasks']:
                    try:
                        channel_id = int(channel_id)
                        channel = self.bot.get_channel(channel_id)
                        if channel:
                            await self.start_typing_loop(channel)
                            self.logger.info(f"restored typing loop for {channel_id}")
                    except Exception as e:
                        self.logger.error(f"failed to restore typing loop {channel_id}: {e}")

            if 'auto_reacts' in state_data:
                auto_reacts_raw = state_data['auto_reacts']
                for user_id_str, val in auto_reacts_raw.items():
                    try:
                        user_id = int(user_id_str)
                        if isinstance(val, dict):
                            self.auto_reacts[user_id] = val
                        else:
                            self.auto_reacts[user_id] = {'emoji': val, 'user_name': 'unknown'}
                    except:
                        pass

            if 'auto_replies' in state_data:
                for user_id_str, data in state_data['auto_replies'].items():
                    try:
                        user_id = int(user_id_str)
                        if self.state['stopped_by_command'].get(f"suji_{user_id}"):
                            continue
                        self.auto_replies[user_id] = {
                            'channel_id': data.get('channel_id'),
                            'user_name': data.get('user_name', 'unknown'),
                            'start_time': data.get('start_time', time.time()),
                            'any_channel': data.get('any_channel', False),
                            'custom_file': data.get('custom_file')
                        }
                        self.logger.info(f"restored auto_reply for user {user_id} (any_channel={data.get('any_channel', False)})")
                    except Exception as e:
                        self.logger.error(f"failed to restore auto_reply {user_id_str}: {e}")

            if 'custom_reacts' in state_data:
                for ch_id, users in state_data['custom_reacts'].items():
                     try:
                         channel_id = int(ch_id)
                         self.custom_reacts[channel_id] = {}
                         for uid, data in users.items():
                             try:
                                 self.custom_reacts[channel_id][int(uid)] = data
                             except: pass
                     except: pass

            if 'voice' in state_data:
                try:
                    voice_data = state_data['voice']
                    if voice_data.get('guild_id') and voice_data.get('channel_id'):
                        guild_id = voice_data['guild_id']
                        channel_id = voice_data['channel_id']
                        if hasattr(self.bot, 'ws') and self.bot.ws:
                            voice_payload = {
                                'op': 4,
                                'd': {
                                    'guild_id': str(guild_id),
                                    'channel_id': str(channel_id),
                                    'self_mute': True,
                                    'self_deaf': True
                                }
                            }
                            await self.bot.ws.send_as_json(voice_payload)
                            self.current_voice_channel = int(channel_id)
                            self.current_voice_guild = int(guild_id)
                            self.logger.info(f"restored voice connection to {channel_id}")
                    elif voice_data.get('dm_channel_id'):
                        channel_id = voice_data['dm_channel_id']
                        if hasattr(self.bot, 'ws') and self.bot.ws:
                            voice_payload = {
                                'op': 4,
                                'd': {
                                    'guild_id': None,
                                    'channel_id': str(channel_id),
                                    'self_mute': True,
                                    'self_deaf': True
                                }
                            }
                            await self.bot.ws.send_as_json(voice_payload)
                            self.current_dm_voice_channel = int(channel_id)
                            self.logger.info(f"restored dm voice connection to {channel_id}")
                except Exception as e:
                     self.logger.error(f"failed to restore voice: {e}")

            saved_streams = state_data.get('active_streams', [])
            current_status = state_data.get('current_status', 'online')
            if hasattr(self.bot, 'ws') and self.bot.ws:
                try:
                    if saved_streams:
                        self.active_streams = saved_streams
                        self.status_cleared = False
                        await self.bot.ws.send_as_json({
                            'op': 3,
                            'd': {'status': current_status, 'since': 0, 'activities': self.active_streams, 'afk': False}
                        })
                        self.logger.info(f"restored {len(self.active_streams)} activities with status {current_status}")
                    else:
                        await self.bot.ws.send_as_json({
                            'op': 3,
                            'd': {'status': current_status, 'since': 0, 'activities': [], 'afk': False}
                        })
                        self.logger.info(f"restored status: {current_status}")
                except Exception as e:
                    self.logger.error(f"failed to restore status/activities: {e}")

            self.logger.info("state restored")

        except Exception as e:
            self.logger.error(f"error restoring state: {e}")

    async def _handle_command(self, message, cmd, args):
        try:
            if cmd == "peleu":
                mentions = args.strip()
                target_channel = message.channel
                parts = mentions.split(maxsplit=1)
                if parts and parts[0].isdigit():
                    try:
                        cid = int(parts[0])
                        ch = self.bot.get_channel(cid)
                        if not ch:
                            try: ch = await self.bot.fetch_channel(cid)
                            except: ch = None
                        if ch:
                            target_channel = ch
                            mentions = parts[1] if len(parts) > 1 else ""
                        else:
                            return
                    except:
                        return

                if target_channel.id in self.state.get('loops_files', {}):
                    del self.state['loops_files'][target_channel.id]

                await self.start_pl_loop(target_channel, mentions)

            elif cmd == "pizdoo":
                mentions = args.strip()
                target_channel = message.channel
                parts = mentions.split(maxsplit=1)
                if parts and parts[0].isdigit():
                    try:
                        cid = int(parts[0])
                        ch = self.bot.get_channel(cid)
                        if not ch:
                            try: ch = await self.bot.fetch_channel(cid)
                            except: ch = None
                        if ch:
                            target_channel = ch
                            mentions = parts[1] if len(parts) > 1 else ""
                        else:
                            return
                    except:
                        return

                await self.start_tenplm_loop(target_channel, mentions)

            elif cmd == "jegule":
                args_stripped = args.strip()
                if args_stripped:
                    try:
                        channel_id = int(args_stripped)
                    except:
                        channel_id = message.channel.id
                else:
                    channel_id = message.channel.id
                await self.stop_tenplm_loop(channel_id)

            elif cmd == "startnotepad":
                try:
                    await message.delete()
                except:
                    pass

                args_stripped = args.strip()
                if not args_stripped:
                    return

                parts = args_stripped.split(maxsplit=1)
                custom_file = parts[0]
                if not custom_file.endswith(".txt"): return
                rest = parts[1] if len(parts) > 1 else ""

                target_channel = message.channel
                mentions = ""

                if rest:
                    parts2 = rest.split(maxsplit=1)
                    if parts2[0].isdigit():
                        try:
                            cid = int(parts2[0])
                            ch = self.bot.get_channel(cid)
                            if not ch:
                                try: ch = await self.bot.fetch_channel(cid)
                                except: ch = None

                            if ch:
                                target_channel = ch
                                mentions = parts2[1] if len(parts2) > 1 else ""
                            else:
                                return
                        except:
                            return
                    else:
                        mentions = rest

                await self.start_pl_loop(target_channel, mentions, custom_file=custom_file)

            elif cmd == "cee":
                args_stripped = args.strip()
                if args_stripped:
                    try:
                        channel_id = int(args_stripped)
                    except:
                        channel_id = message.channel.id
                else:
                    channel_id = message.channel.id
                await self.stop_pl_loop(channel_id)
                if channel_id in self.loops_counters:
                    self.loops_counters[channel_id] = 0
                self.message_index = 0

            elif cmd == "stopnotepad":
                args_stripped = args.strip()
                if args_stripped:
                    try:
                        channel_id = int(args_stripped)
                    except:
                        channel_id = message.channel.id
                else:
                    channel_id = message.channel.id
                await self.stop_pl_loop(channel_id)
                if channel_id in self.loops_counters:
                    self.loops_counters[channel_id] = 0
                self.message_index = 0

            elif cmd == "pola":
                mentions = args.strip()
                target_channel = message.channel
                parts = mentions.split(maxsplit=1)
                if parts and parts[0].isdigit():
                    try:
                        cid = int(parts[0])
                        ch = self.bot.get_channel(cid)
                        if not ch:
                            try: ch = await self.bot.fetch_channel(cid)
                            except: ch = None
                        if ch:
                            target_channel = ch
                            mentions = parts[1] if len(parts) > 1 else ""
                        else:
                            return
                    except:
                        return

                if target_channel.id in self.state.get('pola_loops_files', {}):
                    del self.state['pola_loops_files'][target_channel.id]

                await self.start_pola_loop(target_channel, mentions)

            elif cmd == "startnotepadshift":
                try:
                    await message.delete()
                except:
                    pass

                args_stripped = args.strip()
                if not args_stripped:
                    return

                parts = args_stripped.split(maxsplit=1)
                custom_file = parts[0]
                if not custom_file.endswith(".txt"): return
                rest = parts[1] if len(parts) > 1 else ""

                target_channel = message.channel
                mentions = ""

                if rest:
                    parts2 = rest.split(maxsplit=1)
                    if parts2[0].isdigit():
                        try:
                            cid = int(parts2[0])
                            ch = self.bot.get_channel(cid)
                            if not ch:
                                try: ch = await self.bot.fetch_channel(cid)
                                except: ch = None

                            if ch:
                                target_channel = ch
                                mentions = parts2[1] if len(parts2) > 1 else ""
                            else:
                                return
                        except:
                            return
                    else:
                        mentions = rest

                await self.start_pola_loop(target_channel, mentions, custom_file=custom_file)

            elif cmd == "tee":
                args_stripped = args.strip()
                if args_stripped:
                    try:
                        channel_id = int(args_stripped)
                    except:
                        channel_id = message.channel.id
                else:
                    channel_id = message.channel.id
                await self.stop_pola_loop(channel_id)

                self.plla_counters[channel_id] = 0

            elif cmd == "stopnotepadshift":
                args_stripped = args.strip()
                if args_stripped:
                    try:
                        channel_id = int(args_stripped)
                    except:
                        channel_id = message.channel.id
                else:
                    channel_id = message.channel.id
                await self.stop_pola_loop(channel_id)

                self.plla_counters[channel_id] = 0

            elif cmd == "start":
                text = args.strip()
                target_channel = message.channel
                parts = text.split(maxsplit=1)
                if parts and parts[0].isdigit():
                    try:
                        cid = int(parts[0])
                        ch = self.bot.get_channel(cid)
                        if not ch:
                            try: ch = await self.bot.fetch_channel(cid)
                            except: ch = None
                        if ch:
                            target_channel = ch
                            text = parts[1] if len(parts) > 1 else ""
                        else:
                            return
                    except:
                        return

                if not text:
                    text = "samacacpetn"

                if text:
                    await self.start_baii_loop(target_channel, text)

            elif cmd == "stop":
                args_stripped = args.strip()
                channel_id = message.channel.id
                if args_stripped:
                    first_arg = args_stripped.split(maxsplit=1)[0]
                    if first_arg.isdigit():
                        channel_id = int(first_arg)
                await self.stop_baii_loop(channel_id)

            elif cmd == "react":
                try:
                    await message.delete()
                except:
                    pass
                args_stripped = args.strip()
                if not args_stripped:
                    return
                parts = args_stripped.split()
                if len(parts) < 2:
                    return
                try:
                    msg_id = int(parts[0])
                    emoji_str = parts[1].strip()

                    if len(parts) >= 3 and parts[2].isdigit():
                        channel_id = int(parts[2])
                    else:
                        channel_id = message.channel.id

                    import urllib.parse
                    import re

                    custom_match = re.match(r'<a?:([^:]+):(\d+)>', emoji_str)
                    if custom_match:
                        emoji_encoded = f"{custom_match.group(1)}:{custom_match.group(2)}"
                    else:
                        emoji_encoded = urllib.parse.quote(emoji_str)

                    url = f"https://discord.com/api/v10/channels/{channel_id}/messages/{msg_id}/reactions/{emoji_encoded}/@me"
                    headers = {
                        'Authorization': self.token,
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }

                    requests.put(url, headers=headers)
                except:
                    pass

            elif cmd == "say":
                args_stripped = args.strip()
                if not args_stripped:
                    return
                parts = args_stripped.split(maxsplit=1)
                target_channel = message.channel
                msg_to_send = args_stripped
                if parts[0].isdigit():
                    try:
                        cid = int(parts[0])
                        ch = self.bot.get_channel(cid)
                        if not ch:
                            try: ch = await self.bot.fetch_channel(cid)
                            except: ch = None
                        if ch and len(parts) > 1:
                            target_channel = ch
                            msg_to_send = parts[1]
                        elif not ch:
                            return
                    except:
                        return
                try:
                    await target_channel.send(msg_to_send)
                except Exception as e:
                    self.logger.error(f"say error: {e}")

            elif cmd == "edit":
                args_stripped = args.strip().lower()
                if args_stripped == "on":
                    self.auto_edit_enabled = True
                elif args_stripped == "off":
                    self.auto_edit_enabled = False

            elif cmd == "fatamea":
                mentions = args.strip()
                target_channel = message.channel
                parts = mentions.split(maxsplit=1)
                if parts and parts[0].isdigit():
                    try:
                        cid = int(parts[0])
                        ch = self.bot.get_channel(cid)
                        if not ch:
                            try: ch = await self.bot.fetch_channel(cid)
                            except: ch = None
                        if ch:
                            target_channel = ch
                            mentions = parts[1] if len(parts) > 1 else ""
                        else:
                            return
                    except:
                        return

                if target_channel.id in self.state.get('fatamea_loops_files', {}):
                    del self.state['fatamea_loops_files'][target_channel.id]

                await self.start_fatamea_loop(target_channel, mentions)

            elif cmd == "startnotepadspiced":
                try:
                    await message.delete()
                except:
                    pass

                args_stripped = args.strip()
                if not args_stripped:
                    return

                parts = args_stripped.split(maxsplit=1)
                custom_file = parts[0]
                if not custom_file.endswith(".txt"): return
                rest = parts[1] if len(parts) > 1 else ""

                target_channel = message.channel
                mentions = ""

                if rest:
                    parts2 = rest.split(maxsplit=1)
                    if parts2[0].isdigit():
                        try:
                            cid = int(parts2[0])
                            ch = self.bot.get_channel(cid)
                            if not ch:
                                try: ch = await self.bot.fetch_channel(cid)
                                except: ch = None

                            if ch:
                                target_channel = ch
                                mentions = parts2[1] if len(parts2) > 1 else ""
                            else:
                                return
                        except:
                            return
                    else:
                        mentions = rest

                await self.start_fatamea_loop(target_channel, mentions, custom_file=custom_file)

            elif cmd == "tragpla":
                args_stripped = args.strip()
                if args_stripped:
                    channel_id = int(args_stripped)
                else:
                    channel_id = message.channel.id
                await self.stop_fatamea_loop(channel_id)

            elif cmd == "stopnotepadspiced":
                args_stripped = args.strip()
                if args_stripped:
                    channel_id = int(args_stripped)
                else:
                    channel_id = message.channel.id
                await self.stop_fatamea_loop(channel_id)

            elif cmd == "delay":
                try:
                    await message.delete()
                except:
                    pass

                parts = args.split()

                if parts:
                    try:
                        val_str = parts[0]
                        channel_id = None

                        if len(parts) > 1:
                            try:
                                channel_id = int(parts[1])
                            except:
                                pass

                        if '-' in val_str:
                            min_d, max_d = map(float, val_str.split('-'))
                            if min_d > max_d:
                                pass
                            else:
                                if channel_id:
                                    if 'channel_delays' not in self.state: self.state['channel_delays'] = {}
                                    self.state['channel_delays'][channel_id] = {'min': min_d, 'max': max_d, 'random': True}
                                    await self.save_state()
                                else:
                                    self.state['random_delay'] = {'min': min_d, 'max': max_d}
                                    self.state['delay'] = 0
                                    await self.save_state()
                        else:
                                val = float(val_str)
                                if channel_id:
                                    if 'channel_delays' not in self.state: self.state['channel_delays'] = {}
                                    self.state['channel_delays'][channel_id] = {'delay': val, 'random': False}
                                    await self.save_state()
                                else:
                                    self.state['delay'] = val
                                    self.state['random_delay'] = None
                                    await self.save_state()
                    except Exception as e:
                        pass
                    return

                if not running_instances:
                    await message.channel.send("```no active tokens```")
                else:
                    lines = ["```"]
                    for i, inst in enumerate(running_instances):
                        username = inst.bot.user.name if inst.bot.user else "unknown"
                        lines.append(f"{i+1}.{username}")
                    lines.append("```")
                    await message.channel.send("\n".join(lines))

            elif cmd == "rdelay":
                try:
                    await message.delete()
                except:
                    pass

                parts = args.split()
                if parts:
                    try:
                        val_str = parts[0]
                        channel_id = None
                        if len(parts) > 1:
                            try:
                                channel_id = int(parts[1])
                            except:
                                pass

                        if '-' in val_str:
                            min_d, max_d = map(float, val_str.split('-'))
                            if min_d <= max_d:
                                if channel_id:
                                    if 'channel_delays' not in self.state: self.state['channel_delays'] = {}
                                    self.state['channel_delays'][channel_id] = {'min': min_d, 'max': max_d, 'random': True}
                                    await self.save_state()
                                else:
                                    self.state['random_delay'] = {'min': min_d, 'max': max_d}
                                    self.state['delay'] = 0
                                    await self.save_state()
                    except:
                        pass

            elif cmd == "dlist":
                try:
                    await message.delete()
                except:
                    pass
                lines = []
                lines.append(f"global delay: {self.state['delay']}s")
                if self.state['random_delay']:
                    lines.append(f"random global delay: {self.state['random_delay']['min']}-{self.state['random_delay']['max']}s")

                if self.state['channel_delays']:
                    lines.append("channel delays:")
                    for ch_id, data in self.state['channel_delays'].items():
                        ch_fmt = f"<#{ch_id}> (`{ch_id}`)"
                        if data.get('random'):
                            lines.append(f"  {ch_fmt} (random) - {data['min']}-{data['max']}s")
                        else:
                            lines.append(f"  {ch_fmt} (normal) - {data['delay']}s")

                await message.channel.send("\n".join(lines) if lines else f"no delays configured")

            elif cmd == "cleardlist":
                try:
                    await message.delete()
                except:
                    pass
                self.state['channel_delays'].clear()
                self.state['random_delay'] = None
                self.state['delay'] = 2.0
                self.rate_limit_penalty.reset()
                await self.save_state()

            elif cmd == "rralltks":
                if self.token_index != 0:
                    return
                try:
                    await message.delete()
                except:
                    pass

                for inst in running_instances:
                    inst.auto_reacts.clear()
                    await inst.save_state()

            elif cmd == "sujistopalltks":
                if self.token_index != 0:
                    return
                try:
                    await message.delete()
                except:
                    pass

                for inst in running_instances:
                    inst.auto_replies.clear()
                    await inst.save_state()

            elif cmd == "stream":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    p = [x.strip() for x in args.strip().split("|")] if args.strip() else []
                    text = p[0] if p and p[0] else " ​"
                    custom_app_id = None
                    apps = load_rpc_apps()
                    large_name = None
                    small_name = None
                    stream_url = None
                    end_unix_ts = None
                    _asset_count = 0
                    for part in p[1:]:
                        if not part: continue
                        if part.startswith("http"):
                            stream_url = part
                        elif part.startswith('<t:') and part.endswith('>'):
                            _ts_val = part.split(':')[1]
                            if _ts_val.isdigit(): end_unix_ts = int(_ts_val)
                        elif part.isdigit() and len(part) >= 17:
                            custom_app_id = part
                        elif _asset_count == 0:
                            large_name = part
                            _asset_count += 1
                        elif _asset_count == 1:
                            small_name = part
                            _asset_count += 1

                    if not hasattr(self, 'active_streams'):
                        self.active_streams = []
                    if custom_app_id:
                        target_app = custom_app_id
                    else:
                        existing_app_ids = {act.get('application_id') for act in self.active_streams}
                        target_app = apps[0][0] if apps else "1467114807516856499"
                        for _aid, _ in apps:
                            if _aid not in existing_app_ids:
                                target_app = _aid
                                break
                    if not large_name:
                        large_name = next((_an for _ai, _an in apps if _ai == target_app), (apps[0][1] if apps else "1000020561"))

                    await _rpc_fetch_assets_for_app(target_app)
                    large_id = _rpc_asset_cache.get(f"{target_app}:{large_name}")
                    if not large_id:
                        for _ai, _an in apps:
                            if _ai == target_app:
                                continue
                            await _rpc_fetch_assets_for_app(_ai)
                            _found = _rpc_asset_cache.get(f"{_ai}:{large_name}")
                            if _found:
                                large_id = _found
                                target_app = _ai
                                break
                    if small_name:
                        small_id = _rpc_asset_cache.get(f"{target_app}:{small_name}")
                        if not small_id:
                            for _ai, _an in apps:
                                if _ai == target_app:
                                    continue
                                await _rpc_fetch_assets_for_app(_ai)
                                _found_s = _rpc_asset_cache.get(f"{_ai}:{small_name}")
                                if _found_s:
                                    small_id = _found_s
                                    break
                    else:
                        small_id = None
                    if not large_id:
                        default_large_name = next((_an for _ai, _an in apps if _ai == target_app), (apps[0][1] if apps else "1000020561"))
                        large_id = _rpc_asset_cache.get(f"{target_app}:{default_large_name}")
                    if large_id and not large_id.isdigit():
                        large_id = None
                    if small_id and not small_id.isdigit():
                        small_id = None
                    assets = {}
                    if large_id:
                        assets['large_image'] = large_id
                        assets['large_text'] = ' ​'
                    if small_id:
                        assets['small_image'] = small_id
                        assets['small_text'] = ' ​'
                    new_activity = {
                        'name': text, 'type': 1,
                        'url': stream_url or 'https://youtube.com/watch?v=QU9Ub8cSSPM&list=RDAMVMQU9Ub8cSSPM',
                        'assets': assets,
                        'timestamps': {**{'start': int(time.time() * 1000)}, **({'end': end_unix_ts * 1000} if end_unix_ts is not None else {})},
                        'application_id': target_app,
                        '_custom_app': True
                    }
                    self.active_streams.append(new_activity)
                    if len(self.active_streams) > 5:
                        self.active_streams.pop(0)
                    self.status_cleared = False
                    current_status = self.state.get('current_status', 'dnd')
                    presence_payload = {
                        'op': 3,
                        'd': {'status': current_status, 'since': 0, 'activities': self.active_streams, 'afk': False}
                    }
                    if hasattr(self.bot, 'ws') and self.bot.ws:
                        await self.bot.ws.send_as_json(presence_payload)
                        await self.save_state()
                    else:
                        await self.bot.change_presence(status=discord.Status.dnd, activities=[discord.Streaming(name=a['name'], url=a['url']) for a in self.active_streams])
                        await self.save_state()
                except Exception as e:
                    self.logger.error(f"stream error: {e}")

            elif cmd in ("play", "watch", "competing", "listening"):
                try:
                    await message.delete()
                except:
                    pass
                try:
                    type_map = {"play": 0, "listening": 2, "watch": 3, "competing": 5}
                    p = [x.strip() for x in args.strip().split("|")] if args.strip() else []
                    text = p[0] if p and p[0] else " \u200b"
                    custom_app_id = None
                    apps = load_rpc_apps()
                    large_name = None
                    small_name = None
                    end_unix_ts = None
                    _asset_count = 0
                    for part in p[1:]:
                        if not part: continue
                        if part.startswith('<t:') and part.endswith('>'):
                            _ts_val = part.split(':')[1]
                            if _ts_val.isdigit(): end_unix_ts = int(_ts_val)
                        elif part.isdigit() and len(part) >= 17:
                            custom_app_id = part
                        elif _asset_count == 0:
                            large_name = part
                            _asset_count += 1
                        elif _asset_count == 1:
                            small_name = part
                            _asset_count += 1

                    if not hasattr(self, 'active_streams'):
                        self.active_streams = []
                    if custom_app_id:
                        target_app = custom_app_id
                    else:
                        existing_app_ids = {act.get('application_id') for act in self.active_streams}
                        target_app = apps[0][0] if apps else "1467114807516856499"
                        for _aid, _ in apps:
                            if _aid not in existing_app_ids:
                                target_app = _aid
                                break
                    if not large_name:
                        large_name = next((_an for _ai, _an in apps if _ai == target_app), (apps[0][1] if apps else "1000020561"))

                    await _rpc_fetch_assets_for_app(target_app)
                    large_id = _rpc_asset_cache.get(f"{target_app}:{large_name}")
                    if not large_id:
                        for _ai, _an in apps:
                            if _ai == target_app:
                                continue
                            await _rpc_fetch_assets_for_app(_ai)
                            _found = _rpc_asset_cache.get(f"{_ai}:{large_name}")
                            if _found:
                                large_id = _found
                                target_app = _ai
                                break
                    if small_name:
                        small_id = _rpc_asset_cache.get(f"{target_app}:{small_name}")
                        if not small_id:
                            for _ai, _an in apps:
                                if _ai == target_app:
                                    continue
                                await _rpc_fetch_assets_for_app(_ai)
                                _found_s = _rpc_asset_cache.get(f"{_ai}:{small_name}")
                                if _found_s:
                                    small_id = _found_s
                                    break
                    else:
                        small_id = None
                    if not large_id:
                        default_large_name = next((_an for _ai, _an in apps if _ai == target_app), (apps[0][1] if apps else "1000020561"))
                        large_id = _rpc_asset_cache.get(f"{target_app}:{default_large_name}")
                    if large_id and not large_id.isdigit():
                        large_id = None
                    if small_id and not small_id.isdigit():
                        small_id = None
                    assets = {}
                    if large_id:
                        assets['large_image'] = large_id
                        assets['large_text'] = ' ​'
                    if small_id:
                        assets['small_image'] = small_id
                        assets['small_text'] = ' ​'
                    _now_ms = int(time.time() * 1000)
                    _ts = {'start': _now_ms}
                    if end_unix_ts is not None:
                        _ts['end'] = end_unix_ts * 1000
                    new_activity = {
                        'name': text, 'type': type_map[cmd],
                        'assets': assets,
                        'timestamps': _ts,
                        'application_id': target_app,
                        '_custom_app': True
                    }
                    self.active_streams.append(new_activity)
                    if len(self.active_streams) > 5:
                        self.active_streams.pop(0)
                    self.status_cleared = False
                    current_status = self.state.get('current_status', 'dnd')
                    presence_payload = {
                        'op': 3,
                        'd': {'status': current_status, 'since': 0, 'activities': self.active_streams, 'afk': False}
                    }
                    if hasattr(self.bot, 'ws') and self.bot.ws:
                        await self.bot.ws.send_as_json(presence_payload)
                        await self.save_state()
                    else:
                        await self.bot.change_presence(activity=discord.Game(name=text))
                        await self.save_state()
                except Exception as e:
                    self.logger.error(f"{cmd} error: {e}")

            elif cmd == "status":
                try:
                    await message.delete()
                except:
                    pass
                status_map = {
                    'online': discord.Status.online,
                    'idle': discord.Status.idle,
                    'dnd': discord.Status.dnd,
                    'invisible': discord.Status.invisible
                }
                status_name = args.strip().lower()
                if status_name in status_map:
                    self.state['current_status'] = status_name
                    self.active_streams = []
                    self.rpc_stream_data = None
                    self.status_cleared = True

                    presence_payload = {
                        'op': 3,
                        'd': {
                            'status': status_name,
                            'since': 0,
                            'activities': [],
                            'afk': False
                        }
                    }

                    if hasattr(self.bot, 'ws') and self.bot.ws:
                        await self.bot.ws.send_as_json(presence_payload)
                    else:
                        await self.bot.change_presence(status=status_map[status_name], activities=[])

                    await self.save_state()

            elif cmd == "presence":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    p = args.strip().lower()
                    if p not in ("desktop", "mobile", "web", "console"):
                        return
                    CURRENT_PLATFORM["value"] = p
                    if hasattr(self.bot, 'ws') and self.bot.ws:
                        props = PLATFORM_PROPS[p]
                        await self.bot.ws.send_as_json({
                            "op": 2,
                            "d": {
                                "token": self.token,
                                "capabilities": 16381,
                                "properties": {
                                    "$os": props["$os"],
                                    "$browser": props["$browser"],
                                    "$device": props["$device"],
                                    "$referrer": "",
                                    "$referring_domain": "",
                                },
                                "compress": True,
                                "large_threshold": 250,
                                "v": 3,
                            }
                        })
                        await asyncio.sleep(1.5)
                        current_status = self.state.get('current_status', 'online')
                        active = self.active_streams if hasattr(self, 'active_streams') and not self.status_cleared else []
                        await self.bot.ws.send_as_json({
                            'op': 3,
                            'd': {'status': current_status, 'since': 0, 'activities': active, 'afk': False}
                        })
                    await self.save_state()
                except Exception as e:
                    self.logger.error(f"presence error: {e}")

            elif cmd == "stoploops":
                try:
                    await message.delete()
                except:
                    pass
                for channel_id in list(self.state['loops'].keys()):
                    await self.stop_pl_loop(channel_id)
                for channel_id in list(self.state['pola_loops'].keys()):
                    await self.stop_pola_loop(channel_id)
                for channel_id in list(self.state['plla_loops'].keys()):
                    await self.stop_plla_loop(channel_id)
                for channel_id in list(self.state['baii_loops'].keys()):
                    await self.stop_baii_loop(channel_id)
                for channel_id in list(self.state['fatamea_loops'].keys()):
                    await self.stop_fatamea_loop(channel_id)
                for channel_id in list(self.state.get('tenplm_loops', {}).keys()):
                    await self.stop_tenplm_loop(channel_id)
                for channel_id in list(self.state['rasamati_loops'].keys()):
                    await self.stop_rasamati_loop(channel_id)
                for channel_id in list(self.typing_tasks.keys()):
                    if channel_id in self.typing_tasks:
                        self.typing_tasks[channel_id].cancel()
                        del self.typing_tasks[channel_id]
                if hasattr(self, 'fastall_running'):
                    self.fastall_running.clear()
                await self.save_state()

            elif cmd.startswith("loops"):
                try:
                    await message.delete()
                except:
                    pass

                target_index = None
                target_instance = self

                if len(cmd) > 5 and cmd[5:].isdigit():
                    target_index = int(cmd[5:])
                    target_instance = next((inst for inst in running_instances if inst.token_index == target_index), None)
                    if not target_instance:
                        await message.channel.send(f"token {target_index} not active")
                        return

                lines = []

                in_voice = False
                voice_info = "voice: not connected"
                if target_instance.current_voice_channel:
                    in_voice = True
                    vc = target_instance.current_voice_channel
                    if hasattr(vc, 'channel') and vc.channel:
                        voice_info = f"voice: connected to {vc.channel.id}"
                    else:
                        voice_info = "voice: connected"
                else:
                    for guild in target_instance.bot.guilds:
                        if guild.me and guild.me.voice:
                            in_voice = True
                            voice_info = f"voice: connected to {guild.me.voice.channel.id} (guild: {guild.id})"
                            break
                lines.append(voice_info)

                status_str = target_instance.state.get('current_status', 'online')
                activity = target_instance.bot.activity
                if activity:
                    if isinstance(activity, discord.Streaming):
                        status_str += " | streaming: " + (activity.name or "unknown")
                    elif isinstance(activity, discord.Game):
                        status_str += " | playing: " + (activity.name or "unknown")
                    elif hasattr(activity, 'name') and activity.name:
                        status_str += " | activity: " + activity.name
                lines.append(f"status: {status_str}")

                def fmt_ch(ch_id):
                    return f"<#{ch_id}> (`{ch_id}`)"

                loop_types = [
                    ('loops',          'peleu'),
                    ('pola_loops',     'pola'),
                    ('plla_loops',     'plla'),
                    ('baii_loops',     'baii'),
                    ('fatamea_loops',  'fatamea'),
                    ('tenplm_loops',   'tenplm'),
                    ('rasamati_loops', 'rasamati'),
                ]
                for state_key, label in loop_types:
                    bucket = target_instance.state.get(state_key)
                    if bucket:
                        lines.append(f"{label} loops: {len(bucket)}")
                        for ch_id in bucket.keys():
                            lines.append(f"  - {fmt_ch(ch_id)}")

                if target_instance.group_name_changes:
                    lines.append(f"chg loops: {len(target_instance.group_name_changes)}")
                    for ch_id, data in target_instance.group_name_changes.items():
                        names = data.get('names', [])
                        lines.append(f"  - {fmt_ch(ch_id)} -> {', '.join(str(n) for n in names)}")

                if target_instance.auto_reacts:
                    lines.append(f"react: {len(target_instance.auto_reacts)}")
                    for uid, data in target_instance.auto_reacts.items():
                        emoji = data.get('emoji', '?')
                        lines.append(f"  - <@{uid}> (`{uid}`) {emoji}")

                anti_trap_status = "on" if getattr(target_instance, 'anti_gc_trap', False) else "off"
                lines.append(f"anti gc trap: {anti_trap_status}")

                penalty = target_instance.rate_limit_penalty.get_penalty_delay()
                if penalty > 0:
                    lines.append(f"\nrate limit penalty: +{penalty}s (level {target_instance.rate_limit_penalty.current_level})")

                if hasattr(target_instance, 'fastall_running') and target_instance.fastall_running:
                    lines.append(f"faststartall loops: {len(target_instance.fastall_running)}")
                    for ch_id in target_instance.fastall_running.keys():
                        lines.append(f"  - {fmt_ch(ch_id)}")

                if len(lines) <= 2:
                    await message.channel.send("no active loops")
                else:
                    full_msg = "\n".join(lines)

                    await self.send_long(message.channel, full_msg)

            elif cmd == "info":
                try:
                    await message.delete()
                except:
                    pass
                try:

                    raw_target = args.strip()

                    if raw_target and not raw_target.isdigit() and not raw_target.startswith('<@'):
                        headers = {
                            "Authorization": raw_target,
                            "User-Agent": random.choice(USER_AGENTS),
                            "Content-Type": "application/json",
                        }

                        async with aiohttp.ClientSession() as session:
                            async with session.get("https://discord.com/api/v10/users/@me", headers=headers) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    user_id = int(data.get("id"))
                                    timestamp = ((user_id >> 22) + 1420070400000) / 1000
                                    created_at = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                                    avatar_hash = data.get("avatar")
                                    avatar_url = f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.png" if avatar_hash else "None"

                                    info = f"**token user info**\n"
                                    info += f"username: {data.get('username')}\n"
                                    info += f"id: {user_id}\n"
                                    info += f"email: {data.get('email', 'N/A')}\n"
                                    info += f"phone: {data.get('phone', 'N/A')}\n"
                                    info += f"verified: {data.get('verified', False)}\n"
                                    info += f"mfa_enabled: {data.get('mfa_enabled', False)}\n"
                                    info += f"created: {created_at}\n"
                                    info += f"avatar: {avatar_url}"

                                    await message.channel.send(info)
                                else:
                                    await message.channel.send("# invalid token")
                        return

                    target_id = None

                    import re
                    mention_match = re.match(r'<@!?(\d+)>', raw_target)
                    if mention_match:
                        target_id = int(mention_match.group(1))
                    elif raw_target.isdigit():
                        target_id = int(raw_target)
                    else:
                        target_id = message.author.id

                    user = self.bot.get_user(target_id)
                    if not user:
                        user = await self.bot.fetch_user(target_id)

                    info = f"**user info**\n"
                    info += f"name: {user.name}\n"
                    info += f"id: {user.id}\n"
                    info += f"created: {user.created_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    info += f"avatar: {user.avatar_url}"

                    await message.channel.send(info)
                except Exception as e:
                    self.logger.error(f"info error: {e}")

            elif cmd == "vusername":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    target_username = args.strip()
                    if not target_username:
                        await message.channel.send("# provide a username")
                    else:
                        payload = {"username": target_username}
                        headers = {
                            "Content-Type": "application/json",
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                            "Origin": "https://discord.com",
                            "Referer": "https://discord.com/"
                        }

                        proxy_str = get_random_proxy()
                        proxy_url = f"http://{proxy_str}" if proxy_str else None

                        async with aiohttp.ClientSession() as session:
                            kwargs = {"json": payload, "headers": headers}
                            if proxy_url:
                                kwargs["proxy"] = proxy_url
                            async with session.post("https://discord.com/api/v9/unique-username/username-attempt-unauthed", **kwargs) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    if data.get("taken"):
                                        await message.channel.send(f"# {target_username} is taken")
                                    else:
                                        await message.channel.send(f"# {target_username} is not taken")
                                elif resp.status == 429:
                                     await message.channel.send("# rate limited check")
                                else:
                                    await message.channel.send(f"# could not check username ({resp.status})")
                except Exception as e:
                    self.logger.error(f"vusername error: {e}")

            elif cmd == "vtoken":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    token_to_validate = args.strip()
                    if not token_to_validate:
                        await message.channel.send("# provide a token to validate.")
                        return

                    headers = {"Authorization": token_to_validate}

                    async with aiohttp.ClientSession() as session:
                        async with session.get("https://discord.com/api/v9/users/@me", headers=headers) as resp:
                            if resp.status == 200:
                                await message.channel.send("# token is valid")
                            else:
                                await message.channel.send("# token is invalid")
                except Exception as e:
                    self.logger.error(f"vtoken error: {e}")

            elif cmd == "uptime":
                uptime = int(time.time() - _SCRIPT_START_TIME)
                days = uptime // (3600 * 24)
                hours = (uptime % (3600 * 24)) // 3600
                minutes = (uptime % 3600) // 60
                seconds = uptime % 60

                await message.channel.send(f"uptime: {days}d {hours}h {minutes}m {seconds}s")

            elif cmd == "mainuptime":
                boot_time = psutil.boot_time()
                uptime = int(time.time() - boot_time)
                days = uptime // (3600 * 24)
                hours = (uptime % (3600 * 24)) // 3600
                minutes = (uptime % 3600) // 60
                seconds = uptime % 60
                await message.channel.send(f"main uptime: {days}d {hours}h {minutes}m {seconds}s")

            elif cmd == "setprefix":
                global CMD_PREFIX
                CMD_PREFIX = args.strip()

            elif cmd == "reload":
                try:
                    await message.channel.send("restarting script...")
                    import sys
                    import os
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                except Exception as e:
                    self.logger.error(f"error in reload command: {e}")

            elif cmd == "pula":
                try:
                    if not message.mentions:
                        await message.channel.send("mention a user")
                        return
                    target = message.mentions[0]
                    length = random.randint(1, 15)
                    pula_visual = "=" * length + ">"
                    await message.channel.send(f"{target.mention} are pula de {length} {pula_visual}")
                except Exception as e:
                    self.logger.error(f"error in pula command: {e}")

            elif cmd == "gay":
                try:
                    if not message.mentions:
                        await message.channel.send("mention a user")
                        return
                    target = message.mentions[0]
                    gay_percentage = random.randint(1, 100)
                    await message.channel.send(f"{target.mention} este {gay_percentage}% gay")
                except Exception as e:
                    self.logger.error(f"error in gay command: {e}")

            elif cmd == "ship":
                try:
                    if not message.mentions:
                        await message.channel.send("mention a user")
                        return
                    target = message.mentions[0]
                    ship_percentage = random.randint(1, 100)
                    await message.channel.send(f"sansa de combinatie dintre {message.author.mention} si {target.mention} este de {ship_percentage}%")
                except Exception as e:
                    self.logger.error(f"error in ship command: {e}")

            elif cmd == "hack":
                try:
                    if not message.mentions:
                        await message.channel.send("mention a user")
                        return
                    target = message.mentions[0]
                    
                    hack_steps = [
                        f"targeting {target.mention}...",
                        f"bypassing firewall...",
                        f"injecting payload...",
                        f"accessing database...",
                        f"extracting data...",
                        f"decrypting passwords...",
                        f"uploading to server...",
                        f"hack complete! {target.mention} has been pwned"
                    ]
                    
                    msg = await message.channel.send(f"🔓 initiating hack on {target.mention}...")
                    await asyncio.sleep(1)
                    
                    for step in hack_steps:
                        await msg.edit(content=step)
                        await asyncio.sleep(random.uniform(0.5, 1.5))
                    
                except Exception as e:
                    self.logger.error(f"error in hack command: {e}")

            elif cmd == "lookup":
                try:
                    if message.mentions:
                        target = message.mentions[0]
                        
                        locations = [
                            "New York, United States",
                            "London, United Kingdom",
                            "Tokyo, Japan",
                            "Paris, France",
                            "Berlin, Germany",
                            "Sydney, Australia",
                            "Toronto, Canada",
                            "Moscow, Russia",
                            "Beijing, China",
                            "São Paulo, Brazil",
                            "Bucharest, Romania",
                            "Madrid, Spain",
                            "Rome, Italy",
                            "Amsterdam, Netherlands",
                            "Seoul, South Korea"
                        ]
                        
                        location = random.choice(locations)
                        lat = random.uniform(-90, 90)
                        lon = random.uniform(-180, 180)
                        isp = random.choice(["Comcast", "AT&T", "Verizon", "Deutsche Telekom", "Orange", "Telekom Romania", "Vodafone", "Telefonica"])
                        device = random.choice(["Windows 11", "Windows 10", "macOS Sonoma", "Ubuntu 22.04", "iPhone 15", "Samsung Galaxy S24"])
                        
                        await message.channel.send(f"🔍 looking up {target.mention}...")
                        await asyncio.sleep(random.uniform(1, 2))
                        
                        await message.channel.send(f"👤 User: {target.name} ({target.id})\n🌍 Location: {location}\n📊 Coordinates: {lat:.4f}, {lon:.4f}\n🏢 ISP: {isp}\n💻 Device: {device}\n⚠️ tracking enabled")
                    else:
                        ip = args.strip()
                        if not ip:
                            await message.channel.send("provide an ip address or mention a user")
                            return
                        
                        locations = [
                            "New York, United States",
                            "London, United Kingdom",
                            "Tokyo, Japan",
                            "Paris, France",
                            "Berlin, Germany",
                            "Sydney, Australia",
                            "Toronto, Canada",
                            "Moscow, Russia",
                            "Beijing, China",
                            "São Paulo, Brazil",
                            "Bucharest, Romania",
                            "Madrid, Spain",
                            "Rome, Italy",
                            "Amsterdam, Netherlands",
                            "Seoul, South Korea"
                        ]
                        
                        location = random.choice(locations)
                        lat = random.uniform(-90, 90)
                        lon = random.uniform(-180, 180)
                        
                        await message.channel.send(f"🔍 looking up {ip}...")
                        await asyncio.sleep(random.uniform(1, 2))
                        
                        await message.channel.send(f"📍 IP: {ip}\n🌍 Location: {location}\n📊 Coordinates: {lat:.4f}, {lon:.4f}")
                except Exception as e:
                    self.logger.error(f"error in lookup command: {e}")

            elif cmd == "addtoken":
                try:
                    token = args.strip()
                    if not token:
                        await message.channel.send("provide a token")
                        return
                    
                    is_valid, username, user_id = await validate_token_with_username(token)
                    if not is_valid:
                        await message.channel.send("token is invalid")
                        return
                    
                    with token_lock:
                        with open(TOKENS_FILE, "r") as f:
                            existing_tokens = f.read()
                        
                        if token in existing_tokens:
                            await message.channel.send("token already exists in tokens.txt")
                            return
                        
                        with open(TOKENS_FILE, "a") as f:
                            if existing_tokens and not existing_tokens.endswith("\n"):
                                f.write(f"\n{token}")
                            else:
                                f.write(token)
                    
                    await message.channel.send(f"added token for {username} to tokens.txt")
                    self.logger.info(f"added token for {username} to tokens.txt")
                except Exception as e:
                    self.logger.error(f"error in addtoken command: {e}")

            elif cmd == "tsend":
                try:
                    index_str = args.strip()
                    if not index_str or not index_str.isdigit():
                        await message.channel.send("provide a token index (e.g., tsend 1)")
                        return
                    
                    index = int(index_str) - 1
                    
                    with token_lock:
                        try:
                            with open(TOKENS_FILE, "r") as f:
                                tokens = [line.strip() for line in f if line.strip()]
                        except FileNotFoundError:
                            await message.channel.send("tokens.txt not found")
                            return
                    
                    if index < 0 or index >= len(tokens):
                        await message.channel.send(f"invalid index. valid range: 1-{len(tokens)}")
                        return
                    
                    token = tokens[index]
                    await message.channel.send(f"```\n{token}\n```")
                    self.logger.info(f"sent token at index {index + 1}")
                except Exception as e:
                    self.logger.error(f"error in tsend command: {e}")

            elif cmd == "cgcn":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    delay = 0.0
                    parts = args.strip().split()
                    if parts and parts[0].replace('.', '', 1).isdigit():
                        delay = float(parts[0])

                    try:
                        with open("test.txt", "r", encoding="utf-8") as f:
                            names = [line.strip() for line in f if line.strip()]
                    except FileNotFoundError:
                        await message.channel.send("test.txt not found")
                        return
                    except Exception as e:
                        await message.channel.send(f"error reading test.txt: {e}")
                        return

                    if not names:
                        await message.channel.send("test.txt is empty")
                        return

                    channel = message.channel
                    await self.start_group_name_loop(channel, names, delay)
                    await message.channel.send(f"started changing group name with {len(names)} names from test.txt (delay: {delay}s)")
                except Exception as e:
                    self.logger.error(f"error in cgcn command: {e}")

            elif cmd == "ping":
                start = time.time()
                await self.bot.http.request(discord.http.Route('GET', '/users/@me'))
                latency = int((time.time() - start) * 1000)
                await message.channel.send(f"ping: {latency}ms")

            elif cmd == "clear":
                parts = args.split()
                try:
                    count = int(parts[0]) if parts else 1
                    target_channel_id = int(parts[1]) if len(parts) > 1 else message.channel.id
                    target_channel = self.bot.get_channel(target_channel_id)

                    if target_channel and count > 0:
                        deleted = 0
                        current_delay = random.uniform(0.3, 0.6)
                        async for msg in target_channel.history(limit=count * 2000):
                            if msg.author.id == self.bot.user.id and deleted < count:
                                try:
                                    await msg.delete()
                                    deleted += 1
                                    await asyncio.sleep(current_delay)
                                except discord.errors.HTTPException as e:
                                    if e.status == 429:
                                        retry_after = getattr(e, 'retry_after', 2.0)
                                        current_delay = min(current_delay * 1.5 + 0.5, 5.0)
                                        self.logger.info(f"clear rate limited, new delay: {current_delay:.2f}s")
                                        await asyncio.sleep(retry_after + 0.5)
                                    else:
                                        pass
                                except:
                                    pass
                        self.logger.info(f"cleared {deleted} messages")
                except Exception as e:
                    self.logger.error(f"clear error: {e}")

            elif cmd.startswith("silentleave"):
                try:
                    await message.delete()
                except:
                    pass
                try:
                    gc_id = None
                    if args:
                        cleaned_args = args.strip()
                        import re
                        match = re.search(r'\d+', cleaned_args)
                        if match:
                            gc_id = int(match.group())

                    if not gc_id:
                        self.logger.error("silentleave: invalid or missing group id")
                        return

                    suffix = cmd[11:].strip()
                    targets = []

                    if not suffix:
                        targets = [self]
                    elif suffix.isdigit():
                        idx = int(suffix)
                        for inst in running_instances:
                            if inst.token_index == (idx - 1):
                                targets = [inst]
                                break
                    
                    if not targets:
                        targets = [self]

                    for inst in targets:
                        base_headers = {
                            "Authorization": inst.token,
                            "User-Agent": random.choice(USER_AGENTS),
                            "X-Super-Properties": get_super_properties(),
                            "X-Discord-Locale": "en-US",
                            "X-Discord-Timezone": "Europe/Bucharest",
                            "X-Debug-Options": "bugReporterEnabled",
                            "Origin": "https://discord.com",
                            "Referer": f"https://discord.com/channels/@me/{gc_id}",
                            "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                            "Sec-Ch-Ua-Mobile": "?0",
                            "Sec-Ch-Ua-Platform": '"Windows"',
                            "Sec-Fetch-Dest": "empty",
                            "Sec-Fetch-Mode": "cors",
                            "Sec-Fetch-Site": "same-origin",
                            "Accept": "*/*",
                            "Accept-Encoding": "gzip, deflate, br",
                            "Accept-Language": "en-US,en;q=0.9",
                            "Connection": "keep-alive",
                            "Host": "discord.com",
                        }

                        urls_to_try = [
                            f"https://discord.com/api/v10/channels/{gc_id}?silent=true",
                            f"https://discord.com/api/v9/channels/{gc_id}?silent=true",
                            f"https://discord.com/api/v10/channels/{gc_id}?silent=true&location=ContextMenu%20Default",
                            f"https://discord.com/api/v9/channels/{gc_id}?silent=true&location=ContextMenu%20Default",
                        ]

                        max_retries = 5
                        success = False
                        
                        for url_idx, delete_url in enumerate(urls_to_try):
                            if success:
                                break
                                
                            for attempt in range(max_retries):
                                if success:
                                    break
                                    
                                proxy_str = get_random_proxy()
                                proxy_url = f"http://{proxy_str}" if proxy_str else None
                                
                                headers = base_headers.copy()
                                if attempt % 2 == 1:
                                    headers["X-Context-Properties"] = base64.b64encode(json.dumps({"location": "Group DM", "location_guild_id": None, "location_channel_id": str(gc_id), "location_channel_type": 3}).encode()).decode()
                                
                                try:
                                    timeout = aiohttp.ClientTimeout(total=15, connect=10)
                                    connector = aiohttp.TCPConnector(ssl=False, limit=1, force_close=True)
                                    
                                    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                                        if attempt == 0 and url_idx == 0:
                                            try:
                                                warmup_url = f"https://discord.com/api/v10/channels/{gc_id}"
                                                async with session.get(warmup_url, headers=headers, proxy=proxy_url) as warmup_resp:
                                                    await warmup_resp.text()
                                                await asyncio.sleep(0.3)
                                            except:
                                                pass
                                        
                                        async with session.delete(delete_url, headers=headers, proxy=proxy_url) as resp:
                                            if resp.status in [200, 204]:
                                                self.logger.info(f"[{inst.token_index}] silently left group {gc_id} (method {url_idx+1})")
                                                success = True
                                                break
                                            elif resp.status == 429:
                                                retry_after = float(resp.headers.get('Retry-After', 2))
                                                self.logger.warning(f"[{inst.token_index}] rate limited, waiting {retry_after}s")
                                                await asyncio.sleep(retry_after + random.uniform(0.5, 1.5))
                                                continue
                                            elif resp.status == 401:
                                                self.logger.error(f"[{inst.token_index}] token invalid")
                                                inst.is_valid = False
                                                remove_token_from_file(inst.token)
                                                break
                                            elif resp.status == 404:
                                                self.logger.info(f"[{inst.token_index}] group {gc_id} not found (maybe already left)")
                                                success = True
                                                break
                                            else:
                                                text = await resp.text()
                                                self.logger.warning(f"[{inst.token_index}] silentleave attempt {attempt+1} method {url_idx+1}: {resp.status}")
                                                if attempt < max_retries - 1:
                                                    await asyncio.sleep(random.uniform(0.5, 1.5))
                                                continue
                                                
                                except asyncio.TimeoutError:
                                    self.logger.warning(f"[{inst.token_index}] timeout attempt {attempt+1}")
                                    await asyncio.sleep(0.5)
                                    continue
                                except aiohttp.ClientError as ce:
                                    self.logger.warning(f"[{inst.token_index}] connection error: {ce}")
                                    await asyncio.sleep(0.5)
                                    continue
                                except Exception as ie:
                                    self.logger.error(f"[{inst.token_index}] error: {ie}")
                                    await asyncio.sleep(0.3)
                                    continue
                            
                            if not success and url_idx < len(urls_to_try) - 1:
                                await asyncio.sleep(random.uniform(0.3, 0.8))
                        
                        if success:
                            self.logger.info(f"[{inst.token_index}] successfully left group {gc_id}")
                        else:
                            self.logger.error(f"[{inst.token_index}] silentleave failed for {gc_id} after all attempts")
                            
                except Exception as e:
                    self.logger.error(f"silentleave error: {e}")

            elif cmd == "addgc":
                try:
                    await message.delete()
                except:
                    pass

                try:
                    if not isinstance(message.channel, discord.GroupChannel):
                        return

                    parts = [p.strip() for p in args.split(',') if p.strip()]
                    for query in parts:
                        user_to_add = None

                        friends = getattr(self.bot.user, 'friends', [])
                        if not friends and hasattr(self.bot, 'friends'):
                            friends = self.bot.friends

                        for friend in friends:
                            if friend.name == query or str(friend.id) == query or getattr(friend, 'display_name', '') == query or getattr(friend, 'global_name', '') == query:
                                user_to_add = friend
                                break

                        if user_to_add:
                            try:
                                await message.channel.add_recipients(user_to_add)
                                await asyncio.sleep(0.5)
                            except:
                                pass
                except Exception as e:
                    self.logger.error(f"addgc error: {e}")

            elif cmd == "rmgc":
                try:
                    await message.delete()
                except:
                    pass

                try:
                    if not isinstance(message.channel, discord.GroupChannel):
                        return

                    users_to_remove = []
                    if message.mentions:
                        users_to_remove.extend(message.mentions)
                    else:
                        parts = args.split()
                        for part in parts:
                            part = part.strip()
                            if part.isdigit():
                                try:
                                    user = await self.bot.fetch_user(int(part))
                                    if user:
                                        users_to_remove.append(user)
                                except:
                                    pass
                            else:
                                for recipient in message.channel.recipients:
                                    if recipient.name.lower() == part.lower() or getattr(recipient, 'global_name', '').lower() == part.lower() or getattr(recipient, 'display_name', '').lower() == part.lower():
                                        users_to_remove.append(recipient)
                                        break

                    for user in users_to_remove:
                        try:
                            await message.channel.remove_recipients(user)
                            await asyncio.sleep(0.5)
                        except:
                            pass
                except Exception as e:
                    self.logger.error(f"rmgc error: {e}")

            elif cmd == "crgc":
                try:
                    await message.delete()
                except:
                    pass

                try:
                    if not isinstance(message.channel, discord.GroupChannel):
                        return

                    args_stripped = args.strip()
                    target_user = None

                    if message.mentions:
                        target_user = message.mentions[0]
                    elif args_stripped.isdigit():
                        try:
                            target_user = await self.bot.fetch_user(int(args_stripped))
                        except:
                            pass
                    else:
                        for recipient in message.channel.recipients:
                            if recipient.name.lower() == args_stripped.lower() or getattr(recipient, 'global_name', '').lower() == args_stripped.lower() or getattr(recipient, 'display_name', '').lower() == args_stripped.lower():
                                target_user = recipient
                                break

                    if target_user:
                        await self.bot.http.request(
                            discord.http.Route('PATCH', '/channels/{channel_id}', channel_id=message.channel.id),
                            json={'owner': str(target_user.id)}
                        )
                except Exception as e:
                    self.logger.error(f"crgc error: {e}")

            elif cmd == "help":
                args_stripped = args.strip().lower()
                
                categories_list = [
                    ("roast", [
                        "morii [channel_id] [@mentions]",
                        "cacatulee [channel_id]",
                        "pizdoo [channel_id] [@mentions]",
                        "jegule [channel_id]",
                        "tarfoo [channel_id] [text]",
                        "mil [channel_id]",
                        "pola [channel_id] [@mentions]",
                        "tee [channel_id]",
                        "fatamea [channel_id] [@mentions]",
                        "tragpla [channel_id]",
                        "tokenbased <msg1,msg2,...> [channel_id]",
                        "tokenbasedstop [channel_id]",
                        "stoploops",
                        "faststartall [channel_id] [text]",
                        "stopfastall [channel_id]"
                    ]),
                    ("voice", [
                        "jvc [channel_id]",
                        "lvc",
                        "jvcall [channel_id]",
                        "lvcall",
                        "mute <on/off>",
                        "deafen <on/off>"
                    ]),
                    ("rpc", [
                        "stream [text][|asset][|small_asset][|url]",
                        "play [text][|asset][|small_asset][|<t:unix:F>]",
                        "watch [text][|asset][|small_asset][|<t:unix:F>]",
                        "competing [text][|asset][|small_asset]",
                        "listening [text][|asset][|small_asset][|<t:unix:F>]",
                        "status <online/idle/dnd/invisible>",
                        "presence <desktop/mobile/web/console>"
                    ]),
                    ("mains", [
                        "delay <delay/sec> [channel_id]",
                        "rdelay <min-max> [channel_id]",
                        "dlist",
                        "cleardlist",
                        "tkuser",
                        "tklist",
                        "vusername <username>",
                        "vtoken <token>",
                        "login <token>",
                        "addtoken <token>",
                        "tsend <index>",
                        "hypesquad <bravery/brilliance/balance> [token_index]",
                        "startscript <filename.py>",
                        "stopscript <filename.py>",
                        "scriptstatus",
                        "stopscriptall",
                        "info [@user/user_id/token]",
                        "av [@user/user_id]",
                        "banner [@user/user_id]",
                        "serverav [server_id]",
                        "serverbanner [server_id]",
                        "afkcheck <on/off>",
                        "afkchecktext <text>"
                    ]),
                    ("reply", [
                        "suji @user [channel_id]",
                        "sogi @user",
                        "replynotepad <file> [channel_id] @user/user_id",
                        "taci @user",
                        "sujilist",
                        "sujilistall",
                        "sujistopall",
                        "sujistopalltks"
                    ]),
                    ("group", [
                        "addgc <user1,user2,...>",
                        "rmgc <@user/username/user_id>",
                        "crgc <@user/username/user_id>",
                        "silentleave <gc_id>",
                        "chg [delay] <name1,name2,...>",
                        "schg [channel_id]",
                        "schgall",
                        "chglist",
                        "cgcn [delay]",
                        "antigctrap <on/off>"
                    ]),
                    ("react", [
                        "r @user <emoji>",
                        "rr @user",
                        "rrall",
                        "rralltks",
                        "rlist",
                        "react <message_id> <emoji> [channel_id]"
                    ]),
                    ("other", [
                        "clear <count> [channel_id]",
                        "say [channel_id] <message>",
                        "edit <on/off>",
                        "typing <on/off> [channel_id]",
                        "ping",
                        "uptime",
                        "mainuptime",
                        "loops",
                        "setprefix <prefix>",
                        "reload"
                    ]),
                    ("fun", [
                        "pula @user",
                        "gay @user",
                        "ship @user",
                        "hack @user",
                        "lookup <ip/@user>"
                    ])
                ]
                
                page_names = {
                    "roast": "roast cmds",
                    "voice": "voice cmds",
                    "rpc": "rpc help",
                    "mains": "mains cmds",
                    "reply": "reply cmds",
                    "group": "group help",
                    "react": "react help",
                    "other": "other cmds",
                    "fun": "fun cmds"
                }

                if not args_stripped:
                    prefix_display = CMD_PREFIX if CMD_PREFIX else "none"
                    pages_text = "\n\n".join([f"**• {page_names.get(name, name)}**" for i, (name, _) in enumerate(categories_list)])
                    index_help = f"""# drugs cmds

*prefix: {prefix_display}*

{pages_text}"""
                    await self.send_long(message.channel, index_help)
                    return

                selected_index = None
                if args_stripped.isdigit():
                    selected_index = int(args_stripped) - 1
                    if selected_index < 0 or selected_index >= len(categories_list):
                        await message.channel.send(f"# invalid page. pages: 1-{len(categories_list)}")
                        return
                else:
                    selected_index = next((i for i, (name, _) in enumerate(categories_list) if name == args_stripped), None)
                    if selected_index is None:
                        await message.channel.send("# invalid argument. use: help [page_name] or help [page_number]")
                        return

                _, cmds = categories_list[selected_index]
                formatted = "\n".join([f"**{cmd}**" for cmd in cmds])
                category_help = f"""{formatted}"""
                await self.send_long(message.channel, category_help)

            elif cmd == "tkuser":
                try:
                    await message.delete()
                except:
                    pass
                if not running_instances:
                    await message.channel.send("```no active tokens```")
                else:
                    lines = ["```"]
                    for i, inst in enumerate(running_instances):
                        username = inst.bot.user.name if inst.bot.user else "unknown"
                        lines.append(f"{i+1}.{username}")
                    lines.append("```")
                    await message.channel.send("\n".join(lines))

            elif cmd == "tklist" or (cmd.startswith("tklist") and cmd[6:].isdigit()):
                try:
                    await message.delete()
                except:
                    pass
                try:
                    if os.path.exists(TOKENS_FILE):
                        with open(TOKENS_FILE, "r") as f:
                            lines = [l.strip() for l in f.readlines() if l.strip() and not l.strip().startswith("#")]
                        count = len(lines)
                        await message.channel.send(f"# `{count} tokens in tokens.txt`")
                    else:
                        await message.channel.send("# `0 tokens in tokens.txt`")
                except Exception as e:
                    await message.channel.send(f"# `error: {e}`")

            elif cmd == "typing":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    args_stripped = args.strip().lower()
                    parts = args_stripped.split()

                    action = None
                    target_channel = message.channel

                    for part in parts:
                        if part in ("on", "off"):
                            action = part
                        elif part.isdigit() and len(part) >= 15:
                            ch = self.bot.get_channel(int(part))
                            if not ch:
                                try: ch = await self.bot.fetch_channel(int(part))
                                except: ch = None
                            if ch:
                                target_channel = ch

                    if action is None:
                        return

                    channel_key = target_channel.id

                    if action == "on":
                        if channel_key in self.typing_tasks and not self.typing_tasks[channel_key].done():
                            return

                        async def typing_loop(ch, key):
                            try:
                                while True:
                                    try:
                                        await ch.trigger_typing()
                                    except Exception:
                                        pass
                                    await asyncio.sleep(8)
                            except asyncio.CancelledError:
                                pass

                        task = asyncio.create_task(typing_loop(target_channel, channel_key))
                        self.typing_tasks[channel_key] = task

                    elif action == "off":
                        if channel_key in self.typing_tasks:
                            self.typing_tasks[channel_key].cancel()
                            del self.typing_tasks[channel_key]

                except Exception as e:
                    self.logger.error(f"typing error: {e}")

            elif cmd == "av":
                try: await message.delete()
                except: pass
                try:
                    import re
                    raw_target = args.strip()
                    target_id = None
                    mention_match = re.match(r'<@!?(\d+)>', raw_target)
                    if mention_match:
                        target_id = int(mention_match.group(1))
                    elif raw_target.isdigit():
                        target_id = int(raw_target)
                    else:
                        target_id = message.author.id
                    user = self.bot.get_user(target_id)
                    if not user:
                        user = await self.bot.fetch_user(target_id)
                    avatar_url = user.avatar_url
                    await message.channel.send(f"\nuser: {user.name}\n\n{avatar_url}")
                except Exception as e:
                    self.logger.error(f"av error: {e}")

            elif cmd == "banner":
                try: await message.delete()
                except: pass
                try:
                    import re
                    raw_target = args.strip()
                    target_id = None
                    mention_match = re.match(r'<@!?(\d+)>', raw_target)
                    if mention_match:
                        target_id = int(mention_match.group(1))
                    elif raw_target.isdigit():
                        target_id = int(raw_target)
                    else:
                        target_id = message.author.id

                    headers = {
                        'Authorization': self.token,
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Content-Type': 'application/json'
                    }

                    async with aiohttp.ClientSession() as session:
                        async with session.get(f'https://discord.com/api/v9/users/{target_id}/profile?with_mutual_guilds=false', headers=headers) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                user_data = data.get('user', {})
                                username = user_data.get('username', 'unknown')
                                banner_hash = user_data.get('banner')
                                if not banner_hash and 'user_profile' in data:
                                    banner_hash = data['user_profile'].get('banner')
                                if banner_hash:
                                    ext = 'gif' if banner_hash.startswith('a_') else 'png'
                                    banner_url = f"https://cdn.discordapp.com/banners/{target_id}/{banner_hash}.{ext}?size=4096"
                                    await message.channel.send(f"\nuser: {username}\n\n{banner_url}")
                                else:
                                    await message.channel.send(f"\nuser: {username}\nuser has no banner\n")
                            else:
                                await message.channel.send(f"# banner error: api status {resp.status}")
                except Exception as e:
                    await message.channel.send(f"# banner error: {e}")
                    self.logger.error(f"banner error: {e}")

            elif cmd == "serverav":
                try: await message.delete()
                except: pass
                try:
                    raw_target = args.strip()
                    if raw_target.isdigit():
                        guild = self.bot.get_guild(int(raw_target))
                    else:
                        guild = message.guild
                    if guild:
                        icon_url = guild.icon_url
                        if icon_url:
                            await message.channel.send(f"\n[ server avatar ]\nserver: {guild.name}\n\n{icon_url}")
                        else:
                            await message.channel.send(f"\nserver: {guild.name}\nno icon set\n")
                    else:
                        await message.channel.send("\nserver not found\n")
                except Exception as e:
                    self.logger.error(f"serverav error: {e}")

            elif cmd == "serverbanner":
                try: await message.delete()
                except: pass
                try:
                    raw_target = args.strip()
                    if raw_target.isdigit():
                        guild = self.bot.get_guild(int(raw_target))
                    else:
                        guild = message.guild
                    if guild:
                        banner_url = guild.banner_url
                        if banner_url:
                            await message.channel.send(f"\nserver: {guild.name}\n\n{banner_url}")
                        else:
                            await message.channel.send(f"\nserver: {guild.name}\nno banner set\n")
                    else:
                        await message.channel.send("\nserver not found\n")
                except Exception as e:
                    self.logger.error(f"serverbanner error: {e}")

            elif cmd == "suji":
                try:
                    args_stripped = args.strip()
                    import re

                    user_id = None
                    channel_id_override = None

                    parts = args_stripped.split()
                    for part in parts:
                        mention_match = re.match(r'<@!?(\d+)>', part)
                        if mention_match:
                            user_id = int(mention_match.group(1))
                        elif part.isdigit() and len(part) >= 17:
                            if user_id is None:
                                user_id = int(part)
                            else:
                                channel_id_override = int(part)
                        elif part.isdigit():
                            if user_id is None:
                                user_id = int(part)

                    if not user_id and message.mentions:
                        user_id = message.mentions[0].id

                    if user_id:
                        user_name = "unknown"
                        try:
                            user_obj = None
                            if message.mentions:
                                for m in message.mentions:
                                    if m.id == user_id:
                                        user_obj = m
                                        break
                            if not user_obj:
                                user_obj = self.bot.get_user(user_id)
                            if not user_obj:
                                user_obj = await self.bot.fetch_user(user_id)
                            if user_obj:
                                user_name = user_obj.name
                        except:
                            pass

                        target_channel_id = channel_id_override if channel_id_override else message.channel.id

                        if self.state['stopped_by_command'].get(f"suji_{user_id}"):
                            del self.state['stopped_by_command'][f"suji_{user_id}"]
                        self.auto_replies[user_id] = {
                            'channel_id': target_channel_id,
                            'user_name': user_name,
                            'any_channel': False
                        }
                        self.replay_counters[user_id] = 0
                        await self.save_state()
                except Exception as e:
                    self.logger.error(f"suji error: {e}")

            elif cmd == "sogi":
                try:
                    args_stripped = args.strip()
                    import re

                    user_id = None
                    parts = args_stripped.split()
                    for part in parts:
                        mention_match = re.match(r'<@!?(\d+)>', part)
                        if mention_match:
                            user_id = int(mention_match.group(1))
                        elif part.isdigit():
                            if user_id is None:
                                user_id = int(part)

                    if not user_id and message.mentions:
                        user_id = message.mentions[0].id

                    if user_id:
                        user_name = "unknown"
                        try:
                            user_obj = None
                            if message.mentions:
                                for m in message.mentions:
                                    if m.id == user_id:
                                        user_obj = m
                                        break
                            if not user_obj:
                                user_obj = self.bot.get_user(user_id)
                            if not user_obj:
                                user_obj = await self.bot.fetch_user(user_id)
                            if user_obj:
                                user_name = user_obj.name
                        except:
                            pass

                        if self.state['stopped_by_command'].get(f"suji_{user_id}"):
                            del self.state['stopped_by_command'][f"suji_{user_id}"]
                        self.auto_replies[user_id] = {
                            'channel_id': message.channel.id,
                            'user_name': user_name,
                            'any_channel': True
                        }
                        self.replay_counters[user_id] = 0
                        await self.save_state()
                except Exception as e:
                    self.logger.error(f"sogi error: {e}")

            elif cmd == "replynotepad":
                try:
                    try:
                        await message.delete()
                    except:
                        pass

                    args_stripped = args.strip()
                    if not args_stripped:
                        return

                    import re
                    parts = args_stripped.split()
                    if not parts:
                        return

                    custom_file = parts[0]
                    target_channel = message.channel
                    user_id = None

                    remaining_args = parts[1:]
                    for arg in remaining_args:
                        if arg.isdigit() and len(arg) > 15:

                            ch = self.bot.get_channel(int(arg))
                            if ch:
                                target_channel = ch
                            else:
                                user_id = int(arg)
                        elif re.match(r'<@!?(\d+)>', arg):
                            user_id = int(re.match(r'<@!?(\d+)>', arg).group(1))
                        elif arg.isdigit():

                             ch = self.bot.get_channel(int(arg))
                             if ch:
                                 target_channel = ch
                             else:
                                 user_id = int(arg)

                    if not user_id and message.mentions:
                        user_id = message.mentions[0].id

                    if user_id:
                        try:
                            user_name = "unknown"
                            user_obj = None
                            if message.mentions:
                                for m in message.mentions:
                                    if m.id == user_id:
                                        user_obj = m
                                        break
                            if not user_obj:
                                user_obj = self.bot.get_user(user_id)
                            if not user_obj:
                                user_obj = await self.bot.fetch_user(user_id)
                            if user_obj:
                                user_name = user_obj.name

                            if self.state['stopped_by_command'].get(f"suji_{user_id}"):
                                del self.state['stopped_by_command'][f"suji_{user_id}"]
                            self.auto_replies[user_id] = {
                                'channel_id': target_channel.id,
                                'user_name': user_name,
                                'custom_file': custom_file
                            }
                            self.replay_counters[user_id] = 0
                            await self.save_state()
                        except Exception as e:
                            self.logger.error(f"sujinotepad inner error: {e}")
                    else:
                        self.logger.warning("sujinotepad: no user_id found")
                except Exception as e:
                    self.logger.error(f"sujinotepad error: {e}")

            elif cmd.startswith("sujilist"):
                try:
                    await message.delete()
                except:
                    pass

                try:
                    target_index = None
                    if cmd == "sujilistall":

                        pass
                    elif len(cmd) > 8 and cmd[8:].isdigit():
                        target_index = int(cmd[8:])

                    if target_index is not None:

                        target_instance = next((inst for inst in running_instances if inst.token_index == target_index), None)
                        if not target_instance:
                            await message.channel.send(f"```token {target_index} not active```")
                            return

                        if not target_instance.auto_replies:
                            await message.channel.send(f"no auto-replies active on token {target_index}")
                        else:
                            lines = [f"**active auto-replies (Token {target_index}):**"]
                            for user_id, data in target_instance.auto_replies.items():
                                user_name = data.get('user_name', 'unknown')
                                lines.append(f"• {user_name} (`{user_id}`)")
                            await message.channel.send("\n".join(lines))
                        return

                    if not self.auto_replies:
                        await message.channel.send("no auto-replies active")
                    else:
                        lines = ["**active auto-replies:**"]
                        for user_id, data in self.auto_replies.items():
                            user_name = data.get('user_name', 'unknown')
                            lines.append(f"• {user_name} (`{user_id}`)")

                        full_msg = "\n".join(lines)
                        if len(full_msg) > 1900:
                            await self.send_long(message.channel, full_msg)
                        else:
                            await message.channel.send(full_msg)
                except Exception as e:
                    self.logger.error(f"sujilist error: {e}")

            elif cmd == "sujistopall":
                try:
                    await message.delete()
                except:
                    pass
                for user_id in list(self.auto_replies.keys()):
                    self.state['stopped_by_command'][f"suji_{user_id}"] = True
                self.auto_replies.clear()

                await self.save_state()

            elif cmd == "taci":
                try:
                    args_stripped = args.strip()
                    import re

                    user_id = None
                    mention_match = re.match(r'<@!?(\d+)>', args_stripped)
                    if mention_match:
                        user_id = int(mention_match.group(1))
                    elif args_stripped.isdigit():
                        user_id = int(args_stripped)
                    elif message.mentions:
                        user_id = message.mentions[0].id

                    if user_id:
                        self.state['stopped_by_command'][f"suji_{user_id}"] = True
                        if user_id in self.auto_replies:
                            del self.auto_replies[user_id]
                            if user_id in self.replay_counters:
                                del self.replay_counters[user_id]
                            if user_id in self.auto_reply_queues:
                                self.auto_reply_queues[user_id].clear()
                                del self.auto_reply_queues[user_id]
                            if user_id in self.auto_reply_tasks:
                                task = self.auto_reply_tasks.pop(user_id)
                                if not task.done():
                                    task.cancel()
                            if user_id in self.auto_reply_next_send:
                                del self.auto_reply_next_send[user_id]
                        await self.save_state()
                except Exception as e:
                    self.logger.error(f"taaci error: {e}")

            elif cmd == "r":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    args_stripped = args.strip()
                    import re

                    user_id = None
                    emoji = ""

                    mention_match = re.match(r'<@!?(\d+)>', args_stripped)
                    if mention_match:
                        user_id = int(mention_match.group(1))
                        remaining = args_stripped[mention_match.end():].strip()
                        if remaining:
                            emoji = remaining.split()[0]
                    elif message.mentions:
                        user_id = message.mentions[0].id
                        parts = args_stripped.split()
                        for part in parts:
                            if not re.match(r'<@!?\d+>', part):
                                emoji = part
                                break
                    else:
                        parts = args_stripped.split()
                        if parts:
                            first_part = parts[0]
                            if first_part.isdigit():
                                user_id = int(first_part)
                                emoji = parts[1] if len(parts) >= 2 else ""
                            else:
                                user_id = self.bot.user.id
                                emoji = first_part
                        else:
                            user_id = self.bot.user.id

                    if user_id:
                        user = self.bot.get_user(user_id)
                        if not user:
                            try:
                                user = await self.bot.fetch_user(user_id)
                            except:
                                user = None
                        self.auto_reacts[user_id] = {
                            'emoji': emoji,
                            'user_name': user.name if user else "unknown"
                        }
                        await self.save_state()
                except Exception as e:
                    self.logger.error(f"r error: {e}")

            elif cmd == "rr":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    args_stripped = args.strip()
                    import re

                    user_id = None
                    mention_match = re.match(r'<@!?(\d+)>', args_stripped)
                    if mention_match:
                        user_id = int(mention_match.group(1))
                    elif args_stripped.isdigit():
                        user_id = int(args_stripped)
                    elif message.mentions:
                        user_id = message.mentions[0].id
                    else:
                        user_id = self.bot.user.id

                    if user_id and user_id in self.auto_reacts:
                        del self.auto_reacts[user_id]
                        await self.save_state()
                except Exception as e:
                    self.logger.error(f"rr error: {e}")

            elif cmd == "rrall" or cmd == "rralltks":
                try:
                    await message.delete()
                except:
                    pass
                self.auto_reacts.clear()
                self.custom_reacts.clear()
                await self.save_state()

            elif cmd == "rlist":
                try:
                    await message.delete()
                except:
                    pass
                if not self.auto_reacts:
                    await message.channel.send(f"no auto-reacts active")
                else:
                    lines = [f"auto-reacts:"]
                    for user_id, data in self.auto_reacts.items():
                        user_name = data.get('user_name', 'unknown')
                        emoji = data.get('emoji', '🔥')
                        lines.append(f"user: {user_name} (id: {user_id}) - emoji: {emoji}")
                    await message.channel.send("\n".join(lines))

            elif cmd == "reload":
                try:
                    await message.delete()
                except:
                    pass

                try:
                    cwd = os.getcwd()
                    txt_files = [
                        f
                        for f in os.listdir(cwd)
                        if f.lower().endswith(".txt") and os.path.isfile(os.path.join(cwd, f))
                    ]

                    for fname in sorted(txt_files):
                        full_path = os.path.join(cwd, fname)
                        content = ""
                        try:
                            with open(full_path, "r", encoding="utf-8") as f:
                                content = f.read()
                        except:
                            continue

                        has_double_newline = "\n\n" in content
                        cleaned_lines = [
                            line.strip()
                            for line in content.split("\n")
                            if line.strip() and not line.strip().startswith("#")
                        ]

                        if fname == MESSAGES_FILE:
                            self.messages = cleaned_lines or self.messages
                        elif fname == REPLAY_FILE:
                            self.replay_messages = cleaned_lines or self.replay_messages
                        elif fname == POLA_FILE:
                            self.pola_messages = (
                                [msg.strip() for msg in content.split("\n\n") if msg.strip()]
                                if has_double_newline
                                else (cleaned_lines or self.pola_messages)
                            )
                        elif fname == BOS_FILE:
                            self.bos_messages = (
                                [msg.strip() for msg in content.split("\n\n") if msg.strip()]
                                if has_double_newline
                                else (cleaned_lines or self.bos_messages)
                            )
                        else:
                            pass
                except Exception:
                    self.load_messages()
                    self.load_pola_messages()
                    self.load_replay_messages()
                    self.load_bos_messages()

                self.message_index = 0
                self.logger.info("reloaded all message files")

            elif cmd.startswith("login"):
                allowed_users = [1395733288911634573,1422353081966006477,244635138863005696,1301932092325888074]
                if message.author.id not in allowed_users:
                    self.logger.warning(f"login command unauthorized for user {message.author.id}")
                    return

                if cmd == "login":
                    token = args.strip()
                else:
                    token = cmd[5:].strip()
                if not token:
                    return

                global _login_lock
                async with _login_lock:
                    is_valid, username, user_id = await validate_token_with_username(token)

                    if not is_valid:
                        self.logger.warning(f"login failed: token is invalid")
                        await message.channel.send("login failed: invalid token")
                        return

                    is_duplicate = any(
                        inst.bot.user and str(inst.bot.user.id) == str(user_id)
                        for inst in running_instances
                    )
                    if is_duplicate:
                        self.logger.info(f"login ignored: user {username} already connected")
                        return

                    self.logger.info(f"logging in as {username}...")

                    try:
                        existing_indices = {inst.token_index for inst in running_instances}
                        new_index = 0
                        while new_index in existing_indices:
                            new_index += 1
                        new_instance = BotInstance(token, new_index)
                        ready_event = asyncio.Event()

                        original_on_ready = new_instance.bot.event.__func__ if hasattr(new_instance.bot.event, '__func__') else None

                        _orig_setup_ready = None
                        for ev_name, ev_func in list(new_instance.bot._listeners.items()) if hasattr(new_instance.bot, '_listeners') else []:
                            pass

                        _ready_wrapper_fired = False
                        _original_on_ready_coro = None

                        for attr in dir(new_instance.bot):
                            pass

                        _ready_event_ref = ready_event

                        original_on_ready_handler = new_instance.bot._events.get('on_ready') if hasattr(new_instance.bot, '_events') else None

                        @new_instance.bot.event
                        async def on_ready():
                            _ready_event_ref.set()
                            if not getattr(new_instance, '_ready_fired', False):
                                new_instance._ready_fired = True
                                new_instance.reconnect_attempts = 0
                                new_instance.last_health_check = time.time()
                                new_instance.rate_limit_penalty.reset()
                                new_instance.load_messages()
                                new_instance.load_dume_messages()
                                new_instance.load_pola_messages()
                                new_instance.load_replay_messages()
                                new_instance.load_bos_messages()
                                await StateManager.initialize(STATE_FILE)
                                await new_instance.restore_state()
                                new_instance.state_restored = True
                                if not new_instance.health_check_task or new_instance.health_check_task.done():
                                    new_instance.health_check_task = asyncio.create_task(new_instance._health_check_loop())
                                if not getattr(new_instance, 'memory_cleaner_task', None) or new_instance.memory_cleaner_task.done():
                                    new_instance.memory_cleaner_task = asyncio.create_task(new_instance._memory_cleaner_loop())

                        async def try_login():
                            try:
                                await new_instance.bot.start(token, bot=False, reconnect=True)
                            except Exception:
                                ready_event.set()

                        login_task = asyncio.create_task(try_login())

                        try:
                            await asyncio.wait_for(ready_event.wait(), timeout=15)
                        except asyncio.TimeoutError:
                            pass

                        if new_instance.bot.user:
                            running_instances.append(new_instance)

                            with token_lock:
                                with open(TOKENS_FILE, "r") as f:
                                    existing = f.read()
                                with open(TOKENS_FILE, "a") as f:
                                    if existing and not existing.endswith("\n"):
                                        f.write(f"\n{token}")
                                    else:
                                        f.write(token)

                            if bypass_manager:
                                bypass_manager.add_token(token)

                            logged_username = new_instance.bot.user.name
                            self.logger.info(f"logged in as {logged_username}")
                            await message.channel.send(f"logged in as {logged_username}")
                        else:
                            login_task.cancel()
                            try:
                                await new_instance.bot.close()
                            except:
                                pass
                    except Exception as e:
                        self.logger.error(f"error in login: {e}")

            elif cmd == "chg":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    parts = args.strip().split()
                    if parts:
                        count = 0
                        delay = 0.0
                        names_start_index = 0

                        first_is_digit = parts[0].replace('.', '', 1).isdigit() and '.' not in parts[0]
                        second_is_number = len(parts) > 1 and parts[1].replace('.', '', 1).isdigit()
                        first_is_number = parts[0].replace('.', '', 1).isdigit()

                        if len(parts) >= 2 and first_is_digit and second_is_number:
                            count = int(parts[0])
                            delay = float(parts[1])
                            names_start_index = 2
                        elif len(parts) >= 1 and first_is_number:
                            delay = float(parts[0])
                            names_start_index = 1

                        names_str = " ".join(parts[names_start_index:])
                        if names_str:
                            names = [n.strip() for n in names_str.split(',') if n.strip()]
                            if not names:
                                names = [n.strip() for n in names_str.split('|') if n.strip()]

                            channel = message.channel

                            if delay <= 0 and len(names) == 1:
                                targets = [self] if count == 0 else running_instances[:count]
                                for inst in targets:
                                    try:
                                        ch = channel if inst == self else inst.bot.get_channel(channel.id)
                                        if not ch:
                                            try: ch = await inst.bot.fetch_channel(channel.id)
                                            except: continue
                                        await ch.edit(name=names[0])
                                    except Exception as e:
                                        self.logger.error(f"chg one-shot error: {e}")
                            else:
                                targets = [self]
                                if count > 0 and 'running_instances' in globals():
                                    targets = running_instances[:count]
                                for inst in targets:
                                    if inst == self:
                                        await inst.start_group_name_loop(channel, names, delay)
                                    else:
                                        try:
                                            ch = inst.bot.get_channel(channel.id)
                                            if not ch:
                                                ch = await inst.bot.fetch_channel(channel.id)
                                            if ch:
                                                await inst.start_group_name_loop(ch, names, delay)
                                        except:
                                            pass
                except Exception as e:
                    self.logger.error(f"error in chg command: {e}")

            elif cmd == "schgall":
                try:
                    await message.delete()
                except:
                    pass
                for channel_id in list(self.group_name_changes.keys()):
                    await self.stop_group_name_loop(channel_id)

            elif cmd.startswith("schg"):
                try:
                    await message.delete()
                except:
                    pass
                try:
                    count = 0
                    channel_id = message.channel.id

                    suffix = cmd[4:]
                    if suffix.isdigit():
                        count = int(suffix)

                    parts = args.strip().split()
                    if parts:
                        if parts[0].isdigit():
                            val = int(parts[0])
                            if count == 0 and val < 100:
                                count = val
                                if len(parts) > 1 and parts[1].isdigit():
                                    channel_id = int(parts[1])
                            else:
                                channel_id = val

                    targets = [self]
                    if count > 0 and 'running_instances' in globals():
                        targets = running_instances[:count]

                    for inst in targets:
                        await inst.stop_group_name_loop(channel_id)
                except Exception as e:
                    self.logger.error(f"error in schg command: {e}")

            elif cmd == "chglist":
                try:
                    await message.delete()
                except:
                    pass
                if not self.group_name_changes:
                    await message.channel.send(f"no active group name changes")
                else:
                    lines = [f"active group name changes:"]
                    for channel_id, data in self.group_name_changes.items():
                        channel = self.bot.get_channel(channel_id)
                        if channel and hasattr(channel, 'recipients') and channel.recipients:
                            recipient_names = ", ".join([u.name for u in channel.recipients[:3]])
                            if len(channel.recipients) > 3:
                                recipient_names += f" +{len(channel.recipients) - 3}"
                            channel_name = f"Group: {recipient_names}"
                        else:
                            channel_name = str(channel_id)

                        name_list = data.get('names', [data.get('name', 'unknown')])
                        if isinstance(name_list, list):
                            name_display = " | ".join(name_list)
                        else:
                            name_display = name_list
                        lines.append(f"  {channel_id} | {channel_name} | delay: {data['delay']}s | names: {name_display}")
                    await message.channel.send("\n".join(lines))

            elif cmd == "jvcall":
                try:
                    await message.delete()
                except:
                    pass

                target_channel_id = None
                args_stripped = args.strip()

                if args_stripped and args_stripped.isdigit():
                    target_channel_id = int(args_stripped)
                else:
                    if isinstance(message.channel, (discord.GroupChannel, discord.DMChannel)):
                         target_channel_id = message.channel.id
                    elif message.author.voice:
                         target_channel_id = message.author.voice.channel.id

                if not target_channel_id:
                     await message.channel.send("could not determine voice channel")
                else:
                    count = 0
                    for inst in running_instances:
                        try:
                            if hasattr(inst.bot, 'ws') and inst.bot.ws:
                                 channel = inst.bot.get_channel(target_channel_id)
                                 if not channel:
                                     try: channel = await inst.bot.fetch_channel(target_channel_id)
                                     except: pass

                                 guild_id = None
                                 if channel and hasattr(channel, 'guild'):
                                     guild_id = str(channel.guild.id)

                                 payload = {
                                    'op': 4,
                                    'd': {
                                        'guild_id': guild_id,
                                        'channel_id': str(target_channel_id),
                                        'self_mute': True,
                                        'self_deaf': True
                                    }
                                 }
                                 await inst.bot.ws.send_as_json(payload)

                                 if guild_id:
                                     inst.current_voice_channel = target_channel_id
                                     inst.current_voice_guild = int(guild_id)
                                 else:
                                     inst.current_dm_voice_channel = target_channel_id

                                 await inst.save_state()
                                 count += 1
                                 await asyncio.sleep(0.5)
                        except Exception as e:
                            self.logger.error(f"jvcall error for token {inst.token_index}: {e}")

                    await message.channel.send(f"joined {count} tokens to {target_channel_id}")

            elif cmd == "lvcall":
                try:
                    await message.delete()
                except:
                    pass

                count = 0
                for inst in running_instances:
                    try:
                        if hasattr(inst, 'voice_keepalive_task') and inst.voice_keepalive_task:
                            inst.voice_keepalive_task.cancel()

                        for vc in list(inst.bot.voice_clients):
                            try:
                                await vc.disconnect(force=True)
                            except: pass

                        if hasattr(inst.bot, 'ws') and inst.bot.ws:
                             payload = {
                                'op': 4,
                                'd': {
                                    'guild_id': None,
                                    'channel_id': None,
                                    'self_mute': False,
                                    'self_deaf': False
                                }
                             }
                             await inst.bot.ws.send_as_json(payload)

                             if hasattr(inst, 'current_dm_voice_channel') and inst.current_dm_voice_channel:
                                  payload_dm = {
                                    'op': 4,
                                    'd': {
                                        'guild_id': None,
                                        'channel_id': None,
                                        'self_mute': False,
                                        'self_deaf': False
                                    }
                                  }
                                  await inst.bot.ws.send_as_json(payload_dm)

                        inst.current_voice_channel = None
                        inst.current_voice_guild = None
                        inst.current_dm_voice_channel = None
                        await inst.save_state()
                        count += 1
                        await asyncio.sleep(0.1)
                    except Exception as e:
                        self.logger.error(f"lvcall error for token {inst.token_index}: {e}")

                await message.channel.send(f"disconnected {count} tokens")

            elif cmd == "jvc":
                try:
                    await message.delete()
                except:
                    pass

                if not hasattr(self, 'voice_keepalive_task') or self.voice_keepalive_task.done():
                    async def voice_keepalive_loop():
                        while True:
                            try:
                                await asyncio.sleep(30)
                                if hasattr(self.bot, 'ws') and self.bot.ws:
                                    if getattr(self, 'current_voice_channel', None) and getattr(self, 'current_voice_guild', None):
                                        payload = {
                                            'op': 4,
                                            'd': {
                                                'guild_id': str(self.current_voice_guild),
                                                'channel_id': str(self.current_voice_channel),
                                                'self_mute': True,
                                                'self_deaf': True
                                            }
                                        }
                                        await self.bot.ws.send_as_json(payload)
                                    elif getattr(self, 'current_dm_voice_channel', None):
                                        payload = {
                                            'op': 4,
                                            'd': {
                                                'guild_id': None,
                                                'channel_id': str(self.current_dm_voice_channel),
                                                'self_mute': True,
                                                'self_deaf': True
                                            }
                                        }
                                        await self.bot.ws.send_as_json(payload)
                            except asyncio.CancelledError:
                                break
                            except Exception as e:
                                await asyncio.sleep(5)
                    self.voice_keepalive_task = asyncio.create_task(voice_keepalive_loop())

                args_stripped = args.strip()
                if not args_stripped:
                    if isinstance(message.channel, discord.DMChannel) or isinstance(message.channel, discord.GroupChannel):
                        try:
                            channel_id = message.channel.id

                            if hasattr(self.bot, 'ws') and self.bot.ws:
                                voice_payload = {
                                    'op': 4,
                                    'd': {
                                        'guild_id': None,
                                        'channel_id': str(channel_id),
                                        'self_mute': True,
                                        'self_deaf': True
                                    }
                                }
                                await self.bot.ws.send_as_json(voice_payload)
                                self.current_dm_voice_channel = channel_id
                                self.logger.info(f"joined voice in DM/group {channel_id} (muted & deafened)")
                                await self.save_state()
                            else:
                                self.logger.error("websocket not available for voice state")
                        except Exception as e:
                            self.logger.error(f"jvc dm/group error: {e}")
                    else:
                        self.logger.warning("jvc without channel_id only works in DM/group DM")
                else:
                    try:
                        channel_id = int(args_stripped)
                        channel = self.bot.get_channel(channel_id)

                        if channel and hasattr(channel, 'guild') and channel.guild:
                            guild_id = channel.guild.id

                            if hasattr(self.bot, 'ws') and self.bot.ws:
                                voice_payload = {
                                    'op': 4,
                                    'd': {
                                        'guild_id': str(guild_id),
                                        'channel_id': str(channel_id),
                                        'self_mute': True,
                                        'self_deaf': True
                                    }
                                }
                                await self.bot.ws.send_as_json(voice_payload)
                                self.current_voice_channel = channel_id
                                self.current_voice_guild = guild_id
                                self.logger.info(f"joined voice channel {channel_id} (muted & deafened)")
                                await self.save_state()
                            else:
                                self.logger.error("websocket not available for voice state")
                        elif channel:
                            if hasattr(self.bot, 'ws') and self.bot.ws:
                                voice_payload = {
                                    'op': 4,
                                    'd': {
                                        'guild_id': None,
                                        'channel_id': str(channel_id),
                                        'self_mute': True,
                                        'self_deaf': True
                                    }
                                }
                                await self.bot.ws.send_as_json(voice_payload)
                                self.current_dm_voice_channel = channel_id
                                self.logger.info(f"joined voice channel {channel_id} (muted & deafened)")
                                await self.save_state()
                        else:
                            self.logger.error(f"channel {channel_id} not found")
                    except Exception as e:
                        self.logger.error(f"jvc error: {e}")

            elif cmd == "lvc":
                try:
                    await message.delete()
                except:
                    pass

                if hasattr(self, 'voice_keepalive_task') and self.voice_keepalive_task:
                    self.voice_keepalive_task.cancel()

                try:
                    left_voice = False

                    for vc in list(self.bot.voice_clients):
                        try:
                            await vc.disconnect(force=True)
                            self.logger.info(f"disconnected from voice client")
                            left_voice = True
                            await asyncio.sleep(0.2)
                        except Exception as e:
                            self.logger.warning(f"voice client disconnect error: {e}")

                    if hasattr(self.bot, 'ws') and self.bot.ws:
                        if hasattr(self, 'current_voice_channel') and self.current_voice_channel and hasattr(self, 'current_voice_guild') and self.current_voice_guild:
                            try:
                                await self.bot.ws.send_as_json({
                                    'op': 4,
                                    'd': {
                                        'guild_id': str(self.current_voice_guild),
                                        'channel_id': None,
                                        'self_mute': False,
                                        'self_deaf': False
                                    }
                                })
                                await asyncio.sleep(0.15)
                                left_voice = True
                            except Exception as e:
                                self.logger.warning(f"guild voice leave error: {e}")

                        for guild in self.bot.guilds:
                            try:
                                if guild.me and guild.me.voice:
                                    await self.bot.ws.send_as_json({
                                        'op': 4,
                                        'd': {
                                            'guild_id': str(guild.id),
                                            'channel_id': None,
                                            'self_mute': False,
                                            'self_deaf': False
                                        }
                                    })
                                    await asyncio.sleep(0.1)
                                    left_voice = True
                            except:
                                pass

                        dm_channel_to_leave = None
                        if hasattr(self, 'current_dm_voice_channel') and self.current_dm_voice_channel:
                            dm_channel_to_leave = self.current_dm_voice_channel

                        if dm_channel_to_leave:
                            try:
                                await self.bot.ws.send_as_json({
                                    'op': 4,
                                    'd': {
                                        'guild_id': None,
                                        'channel_id': None,
                                        'self_mute': False,
                                        'self_deaf': False
                                    }
                                })
                                await asyncio.sleep(0.1)
                                self.logger.info(f"left DM/group voice channel {dm_channel_to_leave}")
                                left_voice = True
                            except Exception as e:
                                self.logger.warning(f"dm voice disconnect error: {e}")

                        try:
                            await self.bot.ws.send_as_json({
                                'op': 4,
                                'd': {'guild_id': None, 'channel_id': None, 'self_mute': False, 'self_deaf': False}
                            })
                        except:
                            pass

                    if hasattr(self, 'current_voice_channel'):
                        self.current_voice_channel = None
                    if hasattr(self, 'current_voice_guild'):
                        self.current_voice_guild = None
                    if hasattr(self, 'current_dm_voice_channel'):
                        self.current_dm_voice_channel = None

                    await self.save_state()

                    if left_voice:
                        self.logger.info("left voice channel")
                    else:
                        self.logger.info("not in any voice channel")
                except Exception as e:
                    self.logger.error(f"lvc error: {e}")

            elif cmd == "startscript":

                main_bot_user = running_instances[0].bot.user if running_instances else None
                if not main_bot_user or message.author.id != main_bot_user.id:
                    return
                try:
                    await message.delete()
                except:
                    pass
                try:
                    filename = args.strip()
                    if not filename:
                        await message.channel.send("# usage: startscript <filename.py>")
                        return
                    if not os.path.exists(filename):
                        await message.channel.send(f"# file not found: {filename}")
                        return
                    if filename in _script_processes and _script_processes[filename].poll() is None:
                        await message.channel.send(f"# {filename} is already running")
                        return
                    import subprocess
                    process = subprocess.Popen(
                        [sys.executable, filename],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    _script_processes[filename] = process
                    self.logger.info(f"started script: {filename} (pid {process.pid})")
                    await message.channel.send(f"# started {filename} (pid: {process.pid})")
                except Exception as e:
                    self.logger.error(f"startscript error: {e}")
                    await message.channel.send(f"# startscript error: {e}")

            elif cmd == "stopscript":

                main_bot_user = running_instances[0].bot.user if running_instances else None
                if not main_bot_user or message.author.id != main_bot_user.id:
                    return
                try:
                    await message.delete()
                except:
                    pass
                try:
                    filename = args.strip()
                    if not filename:
                        await message.channel.send("# usage: stopscript <filename.py>")
                        return
                    if filename not in _script_processes:
                        await message.channel.send(f"# {filename} was never started")
                        return
                    proc = _script_processes[filename]
                    if proc.poll() is not None:
                        await message.channel.send(f"# {filename} is already stopped")
                        del _script_processes[filename]
                        return
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except:
                        proc.kill()
                    del _script_processes[filename]
                    self.logger.info(f"stopped script: {filename}")
                    await message.channel.send(f"# stopped {filename}")
                except Exception as e:
                    self.logger.error(f"stopscript error: {e}")
                    await message.channel.send(f"# stopscript error: {e}")

            elif cmd == "scriptstatus":

                main_bot_user = running_instances[0].bot.user if running_instances else None
                if not main_bot_user or message.author.id != main_bot_user.id:
                    return
                try:
                    await message.delete()
                except:
                    pass
                try:
                    if not _script_processes:
                        await message.channel.send("# no scripts started")
                        return
                    lines = ["```"]
                    for fname, proc in list(_script_processes.items()):
                        status = "running" if proc.poll() is None else "stopped"
                        pid = proc.pid if proc.poll() is None else "exited"
                        lines.append(f"{fname} | {status} | pid: {pid}")
                    lines.append("```")
                    await message.channel.send("\n".join(lines))
                except Exception as e:
                    self.logger.error(f"scriptstatus error: {e}")

            elif cmd == "stopscriptall":

                main_bot_user = running_instances[0].bot.user if running_instances else None
                if not main_bot_user or message.author.id != main_bot_user.id:
                    return
                try:
                    await message.delete()
                except:
                    pass
                try:
                    if not _script_processes:
                        await message.channel.send("# no scripts to stop")
                        return
                    stopped = 0
                    for fname in list(_script_processes.keys()):
                        proc = _script_processes[fname]
                        if proc.poll() is None:
                            proc.terminate()
                            try:
                                proc.wait(timeout=5)
                            except:
                                proc.kill()
                            stopped += 1
                            self.logger.info(f"stopped script: {fname}")
                        del _script_processes[fname]
                    await message.channel.send(f"# stopped {stopped} script(s)")
                except Exception as e:
                    self.logger.error(f"stopscriptall error: {e}")

            elif cmd == "hypesquad":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    parts = args.strip().split()
                    house_name = parts[0].lower() if parts else ""
                    house_map = {
                        "bravery": 1,
                        "brilliance": 2,
                        "balance": 3
                    }
                    if house_name not in house_map:
                        await message.channel.send("# usage: hypesquad <bravery/brilliance/balance>")
                        return

                    target_instances = [self]
                    if len(parts) >= 2 and parts[1].isdigit():
                        idx = int(parts[1]) - 1
                        found = next((inst for inst in running_instances if inst.token_index == idx), None)
                        if found:
                            target_instances = [found]

                    house_id = house_map[house_name]
                    for inst in target_instances:
                        try:
                            headers = {
                                "Authorization": inst.token,
                                "Content-Type": "application/json",
                                "User-Agent": random.choice(USER_AGENTS),
                            }
                            async with aiohttp.ClientSession() as session:
                                async with session.post(
                                    "https://discord.com/api/v10/hypesquad/online",
                                    json={"house_id": house_id},
                                    headers=headers
                                ) as resp:
                                    if resp.status == 204:
                                        self.logger.info(f"token {inst.token_index}: hypesquad set to {house_name}")
                                    else:
                                        self.logger.warning(f"token {inst.token_index}: hypesquad failed ({resp.status})")
                        except Exception as ie:
                            self.logger.error(f"hypesquad inner error token {inst.token_index}: {ie}")
                    await message.channel.send(f"# hypesquad set to {house_name}")
                except Exception as e:
                    self.logger.error(f"hypesquad error: {e}")

            elif cmd == "tokenbased":

                main_bot_user = running_instances[0].bot.user if running_instances else None
                if not main_bot_user or message.author.id != main_bot_user.id:
                    return
                try:
                    await message.delete()
                except:
                    pass
                try:
                    raw = args.strip()
                    if not raw:
                        await message.channel.send("# usage: tokenbased mesaj1,mesaj2,... [channel_id]")
                        return

                    parts = raw.rsplit(' ', 1)
                    channel_id = message.channel.id
                    mesaje_raw = raw
                    if len(parts) == 2 and parts[1].strip().isdigit():
                        channel_id = int(parts[1].strip())
                        mesaje_raw = parts[0].strip()

                    mesaje = [m.strip() for m in mesaje_raw.split(',') if m.strip()]
                    if not mesaje:
                        await message.channel.send("# niciun mesaj valid, separa cu virgula")
                        return

                    valid_instances = [inst for inst in running_instances if inst.is_valid and inst.bot.user]
                    if not valid_instances:
                        await message.channel.send("# no valid instances")
                        return

                    if 'tokenbased_running' not in self.state:
                        self.state['tokenbased_running'] = {}

                    if self.state['tokenbased_running'].get(channel_id):
                        await message.channel.send(f"# tokenbased already running on {channel_id}, use tokenbasedstop {channel_id} first")
                        return

                    assignments = []
                    for i, msg_text in enumerate(mesaje):
                        inst = valid_instances[i % len(valid_instances)]
                        assignments.append((inst, msg_text))

                    self.state['tokenbased_running'][channel_id] = True

                    async def tokenbased_loop(ch_id=channel_id, assigns=assignments):
                        try:
                            tasks = []
                            for inst, msg_text in assigns:
                                async def single_token_loop(instance=inst, text=msg_text, cid=ch_id):
                                    while self.state.get('tokenbased_running', {}).get(cid) and instance.is_valid:
                                        try:
                                            ch = instance.bot.get_channel(cid)
                                            if not ch:
                                                try:
                                                    ch = await instance.bot.fetch_channel(cid)
                                                except:
                                                    ch = None
                                            if ch:
                                                await ch.send(text)
                                                instance.rate_limit_penalty.on_success()
                                            delay = instance.get_channel_delay(cid)
                                            await asyncio.sleep(delay)
                                        except discord.errors.HTTPException as e:
                                            if e.status == 401:
                                                instance.is_valid = False
                                                remove_token_from_file(instance.token)
                                                break
                                            elif e.status == 429:
                                                retry_after = getattr(e, 'retry_after', 5)
                                                instance.rate_limit_penalty.on_rate_limit()
                                                await asyncio.sleep(retry_after)
                                            else:
                                                await asyncio.sleep(5)
                                        except asyncio.CancelledError:
                                            break
                                        except Exception:
                                            await asyncio.sleep(5)
                                tasks.append(asyncio.create_task(single_token_loop()))
                            await asyncio.gather(*tasks)
                        finally:
                            if 'tokenbased_running' in self.state:
                                self.state['tokenbased_running'].pop(ch_id, None)
                            self.logger.info(f"tokenbased loop ended on {ch_id}")

                    asyncio.create_task(tokenbased_loop())

                    assign_info = ", ".join([f"tk{inst.token_index+1}→'{txt}'" for inst, txt in assignments])
                    self.logger.info(f"tokenbased started on {channel_id}: {assign_info}")
                except Exception as e:
                    self.logger.error(f"tokenbased error: {e}")

            elif cmd == "mute":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    arg = args.strip().lower()
                    if arg not in ("on", "off"):
                        await message.channel.send("usage: mute on / mute off")
                    else:
                        self.voice_self_mute = (arg == "on")
                        vc_channel = getattr(self, 'current_voice_channel', None) or getattr(self, 'current_dm_voice_channel', None)
                        if vc_channel and hasattr(self.bot, 'ws') and self.bot.ws:
                            guild_id = str(getattr(self, 'current_voice_guild', None)) if getattr(self, 'current_voice_channel', None) else None
                            payload = {
                                'op': 4,
                                'd': {
                                    'guild_id': guild_id,
                                    'channel_id': str(vc_channel),
                                    'self_mute': self.voice_self_mute,
                                    'self_deaf': self.voice_self_deaf
                                }
                            }
                            await self.bot.ws.send_as_json(payload)
                            self.logger.info(f"self_mute set to {self.voice_self_mute}")
                            await message.channel.send(f"mute {'on' if self.voice_self_mute else 'off'}")
                        else:
                            await message.channel.send(f"mute state saved: {'on' if self.voice_self_mute else 'off'} (not in voice)")
                except Exception as e:
                    self.logger.error(f"mute error: {e}")

            elif cmd == "deafen":
                try:
                    await message.delete()
                except:
                    pass
                try:
                    arg = args.strip().lower()
                    if arg not in ("on", "off"):
                        await message.channel.send("usage: deafen on / deafen off")
                    else:
                        self.voice_self_deaf = (arg == "on")
                        vc_channel = getattr(self, 'current_voice_channel', None) or getattr(self, 'current_dm_voice_channel', None)
                        if vc_channel and hasattr(self.bot, 'ws') and self.bot.ws:
                            guild_id = str(getattr(self, 'current_voice_guild', None)) if getattr(self, 'current_voice_channel', None) else None
                            payload = {
                                'op': 4,
                                'd': {
                                    'guild_id': guild_id,
                                    'channel_id': str(vc_channel),
                                    'self_mute': self.voice_self_mute,
                                    'self_deaf': self.voice_self_deaf
                                }
                            }
                            await self.bot.ws.send_as_json(payload)
                            self.logger.info(f"self_deaf set to {self.voice_self_deaf}")
                            await message.channel.send(f"deafen {'on' if self.voice_self_deaf else 'off'}")
                        else:
                            await message.channel.send(f"deafen state saved: {'on' if self.voice_self_deaf else 'off'} (not in voice)")
                except Exception as e:
                    self.logger.error(f"deafen error: {e}")

            elif cmd == "faststartall":

                main_bot_user = running_instances[0].bot.user if running_instances else None
                if not main_bot_user or message.author.id != main_bot_user.id:
                    return
                try:
                    await message.delete()
                except:
                    pass
                try:
                    raw = args.strip()
                    channel_id = message.channel.id
                    msg_text = None

                    parts = raw.split()
                    for part in parts:
                        if part.isdigit() and len(part) >= 15:
                            channel_id = int(part)
                            raw = raw.replace(part, '', 1).strip()
                            break

                    msg_text = raw if raw else None
                    msg_text = msg_text or "# SAMACACPETN"

                    valid_instances = [inst for inst in running_instances if inst.is_valid and inst.bot.user]
                    if not valid_instances:
                        return

                    if not hasattr(self, 'fastall_running'):
                        self.fastall_running = {}

                    if self.fastall_running.get(channel_id):
                        return

                    self.fastall_running[channel_id] = True
                    burst_duration = 9.0

                    async def fast_burst(instance, cid, text, burst_end):
                        try:
                            ch = instance.bot.get_channel(cid)
                            if not ch:
                                try:
                                    ch = await instance.bot.fetch_channel(cid)
                                except:
                                    return
                            while self.fastall_running.get(cid) and instance.is_valid:
                                try:
                                    await ch.send(text)
                                    if time.time() < burst_end:
                                        await asyncio.sleep(0)
                                    else:
                                        await asyncio.sleep(instance.get_channel_delay(cid))
                                except discord.errors.HTTPException as e:
                                    if e.status == 401:
                                        instance.is_valid = False
                                        remove_token_from_file(instance.token)
                                        break
                                    elif e.status == 429:
                                        retry_after = getattr(e, 'retry_after', 1)
                                        await asyncio.sleep(retry_after)
                                    else:
                                        await asyncio.sleep(0.5)
                                except asyncio.CancelledError:
                                    break
                                except Exception:
                                    await asyncio.sleep(0.5)
                        finally:
                            pass

                    async def faststartall_task(cid=channel_id, text=msg_text):
                        try:
                            burst_end = time.time() + burst_duration
                            tasks = [
                                asyncio.create_task(fast_burst(inst, cid, text, burst_end))
                                for inst in valid_instances
                            ]
                            await asyncio.gather(*tasks)
                        finally:
                            self.fastall_running.pop(cid, None)
                            self.logger.info(f"faststartall ended on {cid}")

                    asyncio.create_task(faststartall_task())
                    self.logger.info(f"faststartall started on {channel_id} with {len(valid_instances)} tokens")
                except Exception as e:
                    self.logger.error(f"faststartall error: {e}")

            elif cmd == "stopfastall":

                main_bot_user = running_instances[0].bot.user if running_instances else None
                if not main_bot_user or message.author.id != main_bot_user.id:
                    return
                try:
                    await message.delete()
                except:
                    pass
                try:
                    raw = args.strip()
                    if not hasattr(self, 'fastall_running'):
                        self.fastall_running = {}
                    if raw.isdigit() and len(raw) >= 15:
                        cid = int(raw)
                        self.fastall_running.pop(cid, None)
                    else:
                        self.fastall_running.clear()
                    self.logger.info("stopfastall executed")
                except Exception as e:
                    self.logger.error(f"stopfastall error: {e}")

            elif cmd == "antigctrap":
                try:
                    await message.delete()
                except:
                    pass
                arg = args.strip().lower()
                if arg == "on":
                    self.anti_gc_trap = True
                elif arg == "off":
                    self.anti_gc_trap = False

            elif cmd == "afkcheck":
                try:
                    await message.delete()
                except:
                    pass
                arg = args.strip().lower()
                if arg == "on":
                    self.state['afk_check_enabled'] = True
                    self.logger.info("afkcheck enabled")
                elif arg == "off":
                    self.state['afk_check_enabled'] = False
                    self.logger.info("afkcheck disabled")
                await self.save_state()

            elif cmd == "afkchecktext":
                try:
                    await message.delete()
                except:
                    pass
                text = args.strip()
                self.state['afk_check_text'] = text
                self.logger.info(f"afkcheck text set to: {text}")
                await self.save_state()

            elif cmd == "snipe":
                try:
                    channel_id = message.channel.id
                    deleted = self.deleted_messages.get(channel_id)
                    if not deleted:
                        await message.channel.send("# no deleted messages to snipe")
                        return
                    content_lines = []
                    content_text = deleted.get('content', '') or ''
                    if content_text:
                        content_lines.append(f"-# **content**: {content_text}")
                    if deleted.get('images'):
                        images_text = " ".join(deleted['images'])
                        content_lines.append(f"-# **images**: {images_text}")
                    if deleted.get('attachments'):
                        attachments_text = " ".join(deleted['attachments'])
                        content_lines.append(f"-# **attachments**: {attachments_text}")
                    if deleted.get('embeds'):
                        embeds_text = " | ".join(deleted['embeds'])
                        content_lines.append(f"-# **embeds**: {embeds_text}")
                    if not content_lines:
                        content_lines.append("-# **content**: [no text content]")
                    content_block = "\n".join(content_lines)
                    snipe_text = f"""-# **sniped:**
-# **index: 1/1**
-# **user**: {deleted['author']} ({deleted['author_id']})
{content_block}"""
                    await self.send_long(message.channel, snipe_text)
                except Exception as e:
                    self.logger.error(f"snipe error: {e}")
                    await message.channel.send(f"# snipe error: {e}")

            elif cmd == "tokenbasedstop":

                main_bot_user = running_instances[0].bot.user if running_instances else None
                if not main_bot_user or message.author.id != main_bot_user.id:
                    return
                try:
                    await message.delete()
                except:
                    pass
                try:
                    raw = args.strip()
                    if 'tokenbased_running' not in self.state:
                        self.state['tokenbased_running'] = {}

                    if raw.isdigit():
                        ch_id = int(raw)
                        if ch_id in self.state['tokenbased_running']:
                            self.state['tokenbased_running'].pop(ch_id, None)
                            self.logger.info(f"tokenbased stopped on {ch_id}")
                        else:
                            await message.channel.send(f"# no tokenbased running on {ch_id}")
                    else:

                        count_stopped = len(self.state['tokenbased_running'])
                        self.state['tokenbased_running'].clear()
                        self.logger.info(f"tokenbased stopped all ({count_stopped})")
                except Exception as e:
                    self.logger.error(f"tokenbasedstop error: {e}")

        except Exception as e:
            self.logger.error(f"error handling command {cmd}: {e}")

    async def send_long(self, channel, text):
        if len(text) <= 2000:
            await channel.send(text)
        else:
            chunks = [text[i:i+1990] for i in range(0, len(text), 1990)]
            for chunk in chunks:
                await channel.send(chunk)

    async def start_status_text_rotation(self):
        if self.status_text_task:
            self.status_text_task.cancel()

        async def rotation_loop():
            while self.status_text_data:
                try:
                    texts = self.status_text_data['texts']
                    idx = self.status_text_data['index']
                    delay = self.status_text_data['delay']

                    text = texts[idx % len(texts)]
                    activity = discord.Game(name=text)
                    await self.bot.change_presence(activity=activity)

                    self.status_text_data['index'] = (idx + 1) % len(texts)
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"status text rotation error: {e}")
                    await asyncio.sleep(5)

        self.status_text_task = asyncio.create_task(rotation_loop())

    async def _do_silentleave(self, gc_id):
        try:
            base_headers = {
                "Authorization": self.token,
                "User-Agent": random.choice(USER_AGENTS),
                "X-Super-Properties": get_super_properties(),
                "X-Discord-Locale": "en-US",
                "X-Discord-Timezone": "Europe/Bucharest",
                "X-Debug-Options": "bugReporterEnabled",
                "Origin": "https://discord.com",
                "Referer": f"https://discord.com/channels/@me/{gc_id}",
                "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Host": "discord.com",
            }
            urls_to_try = [
                f"https://discord.com/api/v10/channels/{gc_id}?silent=true",
                f"https://discord.com/api/v9/channels/{gc_id}?silent=true",
                f"https://discord.com/api/v10/channels/{gc_id}?silent=true&location=ContextMenu%20Default",
            ]
            success = False
            for url_idx, delete_url in enumerate(urls_to_try):
                if success:
                    break
                for attempt in range(3):
                    if success:
                        break
                    proxy_str = get_random_proxy()
                    proxy_url = f"http://{proxy_str}" if proxy_str else None
                    headers = base_headers.copy()
                    if attempt % 2 == 1:
                        headers["X-Context-Properties"] = base64.b64encode(json.dumps({"location": "Group DM", "location_guild_id": None, "location_channel_id": str(gc_id), "location_channel_type": 3}).encode()).decode()
                    try:
                        timeout = aiohttp.ClientTimeout(total=15, connect=10)
                        connector = aiohttp.TCPConnector(ssl=False, limit=1, force_close=True)
                        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                            async with session.delete(delete_url, headers=headers, proxy=proxy_url) as resp:
                                if resp.status in [200, 204, 404]:
                                    self.logger.info(f"anti_gc_trap: silently left group {gc_id}")
                                    success = True
                                    break
                                elif resp.status == 429:
                                    retry_after = float(resp.headers.get("Retry-After", 2))
                                    await asyncio.sleep(retry_after + random.uniform(0.5, 1.5))
                                elif resp.status == 401:
                                    self.is_valid = False
                                    remove_token_from_file(self.token)
                                    return
                                else:
                                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    except Exception as e:
                        await asyncio.sleep(0.5)
            if not success:
                self.logger.error(f"anti_gc_trap: failed to leave group {gc_id}")
        except Exception as e:
            self.logger.error(f"_do_silentleave error: {e}")

    async def save_state(self):
        try:
            loops_to_save = [str(k) for k in self.state['loops'].keys() if k not in self.state['stopped_by_command'] and not self.state['stopped_by_command'].get(str(k))]
            pola_loops_to_save = [str(k) for k in self.state['pola_loops'].keys() if f"pola_{k}" not in self.state['stopped_by_command']]
            plla_loops_to_save = [str(k) for k in self.state['plla_loops'].keys() if f"plla_{k}" not in self.state['stopped_by_command']]
            fatamea_loops_to_save = [str(k) for k in self.state['fatamea_loops'].keys() if f"fatamea_{k}" not in self.state['stopped_by_command']]

            loops_mentions = {}
            for cid_str in loops_to_save:
                cid = int(cid_str)
                loops_mentions[cid_str] = self.saved_loop_mentions.get(cid, "")

            pola_loops_mentions = {}
            for cid_str in pola_loops_to_save:
                pola_loops_mentions[cid_str] = self.saved_loop_mentions.get(f"pola_{int(cid_str)}", "")

            plla_loops_mentions = {}
            for cid_str in plla_loops_to_save:
                plla_loops_mentions[cid_str] = self.saved_loop_mentions.get(f"plla_{int(cid_str)}", "")

            baii_loops_data = {}
            for channel_id in self.state['baii_loops'].keys():
                if f"baii_{channel_id}" not in self.state['stopped_by_command']:
                    baii_loops_data[str(channel_id)] = self.state.get('baii_texts', {}).get(channel_id, "samacacpetn")

            fatamea_loops_mentions = {}
            for cid_str in fatamea_loops_to_save:
                fatamea_loops_mentions[cid_str] = self.saved_loop_mentions.get(f"fatamea_{int(cid_str)}", "")

            channel_delays_data = {}
            for channel_id, data in self.state['channel_delays'].items():
                channel_delays_data[str(channel_id)] = data

            auto_replies_data = {}
            for user_id, data in self.auto_replies.items():
                if f"suji_{user_id}" not in self.state['stopped_by_command']:
                    auto_replies_data[str(user_id)] = {
                        'channel_id': data['channel_id'],
                        'user_name': data.get('user_name', 'unknown'),
                        'start_time': data.get('start_time', time.time()),
                        'any_channel': data.get('any_channel', False),
                        'custom_file': data.get('custom_file')
                    }

            auto_reacts_data = {}
            for user_id, data in self.auto_reacts.items():
                auto_reacts_data[str(user_id)] = {
                    'emoji': data['emoji'],
                    'user_name': data.get('user_name', 'unknown')
                }

            custom_reacts_data = {}
            for channel_id, users in self.custom_reacts.items():
                custom_reacts_data[str(channel_id)] = {}
                for user_id, data in users.items():
                    custom_reacts_data[str(channel_id)][str(user_id)] = data

            group_name_changes_data = {}
            for channel_id, data in self.group_name_changes.items():
                if f"chg_{channel_id}" not in self.state['stopped_by_command']:
                    group_name_changes_data[str(channel_id)] = {
                        'names': data.get('names', [data.get('name', '')]),
                        'delay': data['delay']
                    }

            typing_tasks_data = [str(k) for k in self.typing_tasks.keys()]

            stopped_by_command_data = {}
            for key, value in self.state['stopped_by_command'].items():
                stopped_by_command_data[str(key)] = value

            voice_data = {}
            if self.current_voice_channel and hasattr(self, 'current_voice_guild') and self.current_voice_guild:
                voice_data['guild_id'] = self.current_voice_guild
                voice_data['channel_id'] = self.current_voice_channel
            elif self.current_dm_voice_channel:
                voice_data['dm_channel_id'] = self.current_dm_voice_channel

            state_data = {
                'token_index': self.token_index,
                'user_id': str(self.bot.user.id) if self.bot.user else None,
                'voice': voice_data,
                'loops': loops_to_save,
                'loops_mentions': loops_mentions,
                'loops_files': {str(k): v for k, v in self.state['loops_files'].items()},
                'pola_loops': pola_loops_to_save,
                'pola_loops_mentions': pola_loops_mentions,
                'pola_loops_files': {str(k): v for k, v in self.state['pola_loops_files'].items()},
                'plla_loops': plla_loops_to_save,
                'plla_loops_mentions': plla_loops_mentions,
                'plla_loops_files': {str(k): v for k, v in self.state.get('plla_loops_files', {}).items()},
                'baii_loops': baii_loops_data,
                'fatamea_loops': fatamea_loops_to_save,
                'fatamea_loops_mentions': fatamea_loops_mentions,
                'fatamea_loops_files': {str(k): v for k, v in self.state['fatamea_loops_files'].items()},
                'delay': self.state['delay'],
                'random_delay': self.state['random_delay'],
                'channel_delays': channel_delays_data,
                'activity': self.state['activity'],
                'current_status': self.state['current_status'],
                'active_streams': self.active_streams if not self.status_cleared else [],
                'auto_replies': auto_replies_data,
                'auto_reacts': auto_reacts_data,
                'custom_reacts': custom_reacts_data,
                'group_name_changes': group_name_changes_data,
                'typing_tasks': typing_tasks_data,
                'stopped_by_command': stopped_by_command_data,
                'loops_counters': {str(k): v for k, v in self.loops_counters.items()},
                'pola_counters': {str(k): v for k, v in self.pola_counters.items()},
                'fatamea_counters': {str(k): v for k, v in self.fatamea_counters.items()},
                'afk_check_enabled': self.state.get('afk_check_enabled', False),
                'afk_check_text': self.state.get('afk_check_text', ''),
                'start_time': self.start_time,
                'last_save': time.time(),
                'script_start_time': _SCRIPT_START_TIME
            }

            async with StateManager.lock:
                all_states = await StateManager.load(STATE_FILE)
                all_states[str(self.token_index)] = state_data
                await StateManager.save(STATE_FILE, all_states)

        except Exception as e:
            self.logger.error(f"error saving state: {e}")

    async def start_pl_loop(self, channel, mentions="", custom_file=None):
        if channel.id in self.state['loops']:
            return

        if self.state['stopped_by_command'].get(channel.id):
            del self.state['stopped_by_command'][channel.id]

        self.saved_loop_mentions[channel.id] = mentions

        custom_messages = []
        if custom_file:
            custom_messages = self.load_custom_messages(custom_file, force_line_mode=True)
            if custom_messages:
                self.state['loops_files'][channel.id] = custom_file

        if not custom_file and channel.id in self.state.get('loops_files', {}):
             custom_file = self.state['loops_files'][channel.id]
             custom_messages = self.load_custom_messages(custom_file, force_line_mode=True)

        local_counter = [self.loops_counters.get(channel.id, 0)]
        
        if not hasattr(self, '_loop_locks'):
            self._loop_locks = {}
        if channel.id not in self._loop_locks:
            self._loop_locks[channel.id] = asyncio.Lock()

        async def loop_task():
            while channel.id in self.state['loops']:
                try:
                    messages_src = custom_messages if custom_messages else self.messages
                    if not messages_src:
                        messages_src = ["regele"]

                    async with self._loop_locks[channel.id]:
                        idx = local_counter[0]
                        msg = messages_src[idx % len(messages_src)]
                        local_counter[0] = idx + 1
                        self.loops_counters[channel.id] = local_counter[0]

                    full_msg = f"{msg} {mentions}".strip() if mentions else msg

                    typing_duration = len(full_msg) * 0.02
                    typing_duration = max(0.1, min(typing_duration, 3.0))

                    await self.bot.http.send_typing(channel.id)
                    elapsed = 0
                    while elapsed < typing_duration:
                        await asyncio.sleep(min(5, typing_duration - elapsed))
                        elapsed += 5
                        if elapsed < typing_duration:
                            await self.bot.http.send_typing(channel.id)

                    await channel.send(full_msg)
                    self.rate_limit_penalty.on_success()
                    delay = self.get_channel_delay(channel.id)
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                except discord.errors.HTTPException as e:
                    if e.status == 401:
                        self.is_valid = False
                        remove_token_from_file(self.token)
                        break
                    elif e.status == 429:
                        retry_after = getattr(e, 'retry_after', 5)
                        self.rate_limit_penalty.on_rate_limit()
                        self.logger.warning(f"rate limited in pl loop, waiting {retry_after}s + penalty")
                        await asyncio.sleep(retry_after)
                    else:
                        await asyncio.sleep(5)
                except Exception as e:
                    self.logger.error(f"error in pl loop: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(loop_task())
        self.state['loops'][channel.id] = task
        await self.save_state()

    async def stop_pl_loop(self, channel_id):
        self.state['stopped_by_command'][channel_id] = True
        if channel_id in self.state['loops']:
            self.state['loops'][channel_id].cancel()
            del self.state['loops'][channel_id]
        if channel_id in self.state.get('loops_files', {}):
            del self.state['loops_files'][channel_id]
        if channel_id in self.loops_counters:
            del self.loops_counters[channel_id]
        if hasattr(self, '_loop_locks') and channel_id in self._loop_locks:
            del self._loop_locks[channel_id]
        await self.save_state()

    async def start_tenplm_loop(self, channel, mentions=""):
        if channel.id in self.state.get('tenplm_loops', {}):
            self.state['tenplm_loops'][channel.id].cancel()
            del self.state['tenplm_loops'][channel.id]

        if 'tenplm_loops' not in self.state:
            self.state['tenplm_loops'] = {}
        if 'tenplm_counters' not in self.__dict__:
            self.tenplm_counters = {}

        if not hasattr(self, '_tenplm_locks'):
            self._tenplm_locks = {}
        if channel.id not in self._tenplm_locks:
            self._tenplm_locks[channel.id] = asyncio.Lock()

        local_counter = [self.tenplm_counters.get(channel.id, 0)]

        async def loop_task():
            while channel.id in self.state.get('tenplm_loops', {}):
                try:
                    messages_src = self.dume4_messages if self.dume4_messages else ['samacacpetn']

                    async with self._tenplm_locks[channel.id]:
                        idx = local_counter[0]
                        msg = messages_src[idx % len(messages_src)]
                        local_counter[0] = idx + 1
                        self.tenplm_counters[channel.id] = local_counter[0]

                    full_msg = f"{msg} {mentions}".strip() if mentions else msg

                    typing_duration = len(full_msg) * 0.02
                    typing_duration = max(0.1, min(typing_duration, 3.0))

                    await self.bot.http.send_typing(channel.id)
                    elapsed = 0
                    while elapsed < typing_duration:
                        await asyncio.sleep(min(5, typing_duration - elapsed))
                        elapsed += 5
                        if elapsed < typing_duration:
                            await self.bot.http.send_typing(channel.id)

                    await channel.send(full_msg)
                    self.rate_limit_penalty.on_success()
                    delay = self.get_channel_delay(channel.id)
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                except discord.errors.HTTPException as e:
                    if e.status == 401:
                        self.is_valid = False
                        remove_token_from_file(self.token)
                        break
                    elif e.status == 429:
                        retry_after = getattr(e, 'retry_after', 5)
                        self.rate_limit_penalty.on_rate_limit()
                        self.logger.warning(f"rate limited in tenplm loop, waiting {retry_after}s + penalty")
                        await asyncio.sleep(retry_after)
                    else:
                        await asyncio.sleep(5)
                except Exception as e:
                    self.logger.error(f"error in tenplm loop: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(loop_task())
        self.state['tenplm_loops'][channel.id] = task
        await self.save_state()

    async def stop_tenplm_loop(self, channel_id):
        self.state.setdefault('stopped_by_command', {})[f"tenplm_{channel_id}"] = True
        if channel_id in self.state.get('tenplm_loops', {}):
            self.state['tenplm_loops'][channel_id].cancel()
            del self.state['tenplm_loops'][channel_id]
        if hasattr(self, 'tenplm_counters') and channel_id in self.tenplm_counters:
            del self.tenplm_counters[channel_id]
        if hasattr(self, '_tenplm_locks') and channel_id in self._tenplm_locks:
            del self._tenplm_locks[channel_id]
        await self.save_state()

    async def stop_pola_loop(self, channel_id):
        self.state['stopped_by_command'][f"pola_{channel_id}"] = True
        if channel_id in self.state['pola_loops']:
            self.state['pola_loops'][channel_id].cancel()
            del self.state['pola_loops'][channel_id]
        if channel_id in self.state.get('pola_loops_files', {}):
            del self.state['pola_loops_files'][channel_id]
        if channel_id in self.pola_counters:
            del self.pola_counters[channel_id]
        await self.save_state()

    async def start_pola_loop(self, channel, mentions="", custom_file=None):
        if channel.id in self.state['pola_loops']:
            return

        if self.state['stopped_by_command'].get(f"pola_{channel.id}"):
            del self.state['stopped_by_command'][f"pola_{channel.id}"]

        self.saved_loop_mentions[f"pola_{channel.id}"] = mentions

        custom_messages = []
        if custom_file:
            custom_messages = self.load_custom_messages(custom_file)
            if custom_messages:
                self.state['pola_loops_files'][channel.id] = custom_file

        if not custom_file and channel.id in self.state.get('pola_loops_files', {}):
             custom_file = self.state['pola_loops_files'][channel.id]
             custom_messages = self.load_custom_messages(custom_file)

        async def loop_task():
            while channel.id in self.state['pola_loops']:
                try:
                    full_msg = self.get_pola_combined_message(channel.id, mentions, custom_messages)

                    typing_duration = len(full_msg) * 0.05

                    typing_duration = max(0.5, min(typing_duration, 3.0))

                    await self.bot.http.send_typing(channel.id)

                    elapsed = 0
                    while elapsed < typing_duration:
                        step = min(0.5, typing_duration - elapsed)
                        await asyncio.sleep(step)
                        elapsed += step
                        if elapsed % 3.0 < 0.5 and elapsed < typing_duration:
                             await self.bot.http.send_typing(channel.id)

                    await channel.send(full_msg)
                    self.rate_limit_penalty.on_success()

                    delay = self.get_channel_delay(channel.id)
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                except discord.errors.HTTPException as e:
                    if e.status == 401:
                        self.is_valid = False
                        remove_token_from_file(self.token)
                        break
                    elif e.status == 429:
                        retry_after = getattr(e, 'retry_after', 5)
                        self.rate_limit_penalty.on_rate_limit()
                        self.logger.warning(f"rate limited in pola loop, waiting {retry_after}s + penalty")
                        await asyncio.sleep(retry_after)
                    else:
                        await asyncio.sleep(5)
                except Exception as e:
                    self.logger.error(f"error in pola loop: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(loop_task())
        self.state['pola_loops'][channel.id] = task
        await self.save_state()

    async def stop_pola_loop(self, channel_id):
        self.state['stopped_by_command'][f"pola_{channel_id}"] = True
        if channel_id in self.state['pola_loops']:
            self.state['pola_loops'][channel_id].cancel()
            del self.state['pola_loops'][channel_id]
        if channel_id in self.state.get('pola_loops_files', {}):
            del self.state['pola_loops_files'][channel_id]
        await self.save_state()

    async def start_fatamea_loop(self, channel, mentions="", custom_file=None):
        if channel.id in self.state['fatamea_loops']:
            return

        if self.state['stopped_by_command'].get(f"fatamea_{channel.id}"):
            del self.state['stopped_by_command'][f"fatamea_{channel.id}"]

        self.saved_loop_mentions[f"fatamea_{channel.id}"] = mentions

        custom_messages = []
        if custom_file:
            custom_messages = self.load_custom_messages(custom_file)
            if custom_messages:
                self.state['fatamea_loops_files'][channel.id] = custom_file

        if not custom_file and channel.id in self.state.get('fatamea_loops_files', {}):
             custom_file = self.state['fatamea_loops_files'][channel.id]
             custom_messages = self.load_custom_messages(custom_file)

        async def loop_task():
            while channel.id in self.state['fatamea_loops']:
                try:
                    if custom_messages:
                        idx = self.fatamea_counters.get(channel.id, 0)
                        msg = custom_messages[idx % len(custom_messages)]
                        self.fatamea_counters[channel.id] = idx + 1

                        lines = msg.split('\n')
                        prefixed_lines = [f"# > {line}" for line in lines]
                        msg = "\n".join(prefixed_lines)

                        if mentions:
                            msg = msg + " " + mentions
                        full_msg = msg
                    else:
                        full_msg = self.get_fatamea_message(channel.id, mentions)

                    typing_duration = len(full_msg) * 0.01
                    await self.bot.http.send_typing(channel.id)
                    elapsed = 0
                    while elapsed < typing_duration:
                        await asyncio.sleep(min(5, typing_duration - elapsed))
                        elapsed += 5
                        if elapsed < typing_duration:
                            await self.bot.http.send_typing(channel.id)

                    await channel.send(full_msg)
                    self.rate_limit_penalty.on_success()
                    delay = self.get_channel_delay(channel.id)
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                except discord.errors.HTTPException as e:
                    if e.status == 401:
                        self.is_valid = False
                        remove_token_from_file(self.token)
                        break
                    elif e.status == 429:
                        retry_after = getattr(e, 'retry_after', 5)
                        self.rate_limit_penalty.on_rate_limit()
                        await asyncio.sleep(retry_after)
                    else:
                        await asyncio.sleep(5)
                except Exception as e:
                    self.logger.error(f"fatamea loop error: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(loop_task())
        self.state['fatamea_loops'][channel.id] = task
        await self.save_state()

    async def stop_fatamea_loop(self, channel_id):
        self.state['stopped_by_command'][f"fatamea_{channel_id}"] = True
        if channel_id in self.state['fatamea_loops']:
            self.state['fatamea_loops'][channel_id].cancel()
            del self.state['fatamea_loops'][channel_id]
        if channel_id in self.state.get('fatamea_loops_files', {}):
            del self.state['fatamea_loops_files'][channel_id]
        if channel_id in self.fatamea_counters:
            del self.fatamea_counters[channel_id]
        await self.save_state()

    async def start_plla_loop(self, channel, mentions="", custom_file=None):
        if channel.id in self.state['plla_loops']:
            return

        if self.state['stopped_by_command'].get(f"plla_{channel.id}"):
            del self.state['stopped_by_command'][f"plla_{channel.id}"]

        self.saved_loop_mentions[f"plla_{channel.id}"] = mentions

        custom_messages = []
        if custom_file:
            custom_messages = self.load_custom_messages(custom_file)
            if custom_messages:
                if 'plla_loops_files' not in self.state: self.state['plla_loops_files'] = {}
                self.state['plla_loops_files'][channel.id] = custom_file

        if not custom_file and channel.id in self.state.get('plla_loops_files', {}):
             custom_file = self.state['plla_loops_files'][channel.id]
             custom_messages = self.load_custom_messages(custom_file)

        async def loop_task():
            while channel.id in self.state['plla_loops']:
                try:
                    full_msg = self.get_plla_message(channel.id, mentions, custom_messages)

                    typing_duration = len(full_msg) * 0.02
                    typing_duration = max(0.1, min(typing_duration, 3.0))

                    await self.bot.http.send_typing(channel.id)
                    elapsed = 0
                    while elapsed < typing_duration:
                        await asyncio.sleep(min(5, typing_duration - elapsed))
                        elapsed += 5
                        if elapsed < typing_duration:
                            await self.bot.http.send_typing(channel.id)

                    await channel.send(full_msg)
                    self.rate_limit_penalty.on_success()
                    delay = self.get_channel_delay(channel.id)
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                except discord.errors.HTTPException as e:
                    if e.status == 401:
                        self.is_valid = False
                        remove_token_from_file(self.token)
                        break
                    elif e.status == 429:
                        retry_after = getattr(e, 'retry_after', 5)
                        self.rate_limit_penalty.on_rate_limit()
                        self.logger.warning(f"rate limited in plla loop, waiting {retry_after}s + penalty")
                        await asyncio.sleep(retry_after)
                    else:
                        await asyncio.sleep(5)
                except Exception as e:
                    self.logger.error(f"error in plla loop: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(loop_task())
        self.state['plla_loops'][channel.id] = task
        await self.save_state()

    async def stop_plla_loop(self, channel_id):
        self.state['stopped_by_command'][f"plla_{channel_id}"] = True
        if channel_id in self.state['plla_loops']:
            self.state['plla_loops'][channel_id].cancel()
            del self.state['plla_loops'][channel_id]
        if channel_id in self.state.get('plla_loops_files', {}):
            del self.state['plla_loops_files'][channel_id]
        if channel_id in self.plla_counters:
            del self.plla_counters[channel_id]
        await self.save_state()

    async def start_baii_loop(self, channel, text: str):
        if channel.id in self.state['baii_loops']:
            return

        if self.state['stopped_by_command'].get(f"baii_{channel.id}"):
            del self.state['stopped_by_command'][f"baii_{channel.id}"]

        if 'baii_texts' not in self.state: self.state['baii_texts'] = {}
        self.state['baii_texts'][channel.id] = text

        async def loop_task():
            while channel.id in self.state['baii_loops']:
                try:
                    typing_duration = len(text) * 0.02
                    typing_duration = max(0.1, min(typing_duration, 3.0))

                    await self.bot.http.send_typing(channel.id)
                    elapsed = 0
                    while elapsed < typing_duration:
                        await asyncio.sleep(min(5, typing_duration - elapsed))
                        elapsed += 5
                        if elapsed < typing_duration:
                            await self.bot.http.send_typing(channel.id)

                    await channel.send(text)
                    self.rate_limit_penalty.on_success()
                    delay = self.get_channel_delay(channel.id)
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                except discord.errors.HTTPException as e:
                    if e.status == 401:
                        self.is_valid = False
                        remove_token_from_file(self.token)
                        break
                    elif e.status == 429:
                        retry_after = getattr(e, 'retry_after', 5)
                        self.rate_limit_penalty.on_rate_limit()
                        self.logger.warning(f"rate limited in baii loop, waiting {retry_after}s + penalty")
                        await asyncio.sleep(retry_after)
                    else:
                        await asyncio.sleep(5)
                except Exception as e:
                    self.logger.error(f"error in baii loop: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(loop_task())
        self.state['baii_loops'][channel.id] = {'task': task, 'text': text}
        await self.save_state()

    async def stop_baii_loop(self, channel_id):
        self.state['stopped_by_command'][f"baii_{channel_id}"] = True
        if channel_id in self.state['baii_loops']:
            self.state['baii_loops'][channel_id]['task'].cancel()
            del self.state['baii_loops'][channel_id]
            await self.save_state()

    async def start_fatamea_loop(self, channel, mentions="", custom_file=None):
        if channel.id in self.state['fatamea_loops']:
            return

        if self.state['stopped_by_command'].get(f"fatamea_{channel.id}"):
            del self.state['stopped_by_command'][f"fatamea_{channel.id}"]

        self.saved_loop_mentions[f"fatamea_{channel.id}"] = mentions

        custom_messages = []
        if custom_file:
            custom_messages = self.load_custom_messages(custom_file)
            if custom_messages:
                self.state['fatamea_loops_files'][channel.id] = custom_file

        if not custom_file and channel.id in self.state.get('fatamea_loops_files', {}):
             custom_file = self.state['fatamea_loops_files'][channel.id]
             custom_messages = self.load_custom_messages(custom_file)

        async def loop_task():
            while channel.id in self.state['fatamea_loops']:
                try:
                    if custom_messages:
                        idx = self.fatamea_counters.get(channel.id, 0)
                        msg = custom_messages[idx % len(custom_messages)]
                        self.fatamea_counters[channel.id] = idx + 1

                        lines = msg.split('\n')
                        prefixed_lines = [f"# > {line}" for line in lines]
                        msg = "\n".join(prefixed_lines)

                        if mentions:
                            msg = msg + " " + mentions
                        full_msg = msg
                    else:
                        full_msg = self.get_fatamea_message(channel.id, mentions)

                    typing_duration = len(full_msg) * 0.01
                    typing_duration = max(0.1, min(typing_duration, 2.0))

                    await self.bot.http.send_typing(channel.id)
                    elapsed = 0
                    while elapsed < typing_duration:
                        await asyncio.sleep(min(5, typing_duration - elapsed))
                        elapsed += 5
                        if elapsed < typing_duration:
                            await self.bot.http.send_typing(channel.id)

                    await channel.send(full_msg)
                    self.rate_limit_penalty.on_success()
                    delay = self.get_channel_delay(channel.id)
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                except discord.errors.HTTPException as e:
                    if e.status == 401:
                        self.is_valid = False
                        remove_token_from_file(self.token)
                        break
                    elif e.status == 429:
                        retry_after = getattr(e, 'retry_after', 5)
                        self.rate_limit_penalty.on_rate_limit()
                        self.logger.warning(f"rate limited in fatamea loop, waiting {retry_after}s + penalty")
                        await asyncio.sleep(retry_after)
                    else:
                        await asyncio.sleep(5)
                except Exception as e:
                    self.logger.error(f"error in fatamea loop: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(loop_task())
        self.state['fatamea_loops'][channel.id] = task
        await self.save_state()

    async def stop_fatamea_loop(self, channel_id):
        self.state['stopped_by_command'][f"fatamea_{channel_id}"] = True
        if channel_id in self.state['fatamea_loops']:
            self.state['fatamea_loops'][channel_id].cancel()
            del self.state['fatamea_loops'][channel_id]
        if channel_id in self.state.get('fatamea_loops_files', {}):
            del self.state['fatamea_loops_files'][channel_id]
        if channel_id in self.fatamea_counters:
            del self.fatamea_counters[channel_id]
        await self.save_state()

    async def start_typing_loop(self, channel):
        if channel.id in self.typing_tasks:
            return

        async def loop_task():
            while channel.id in self.typing_tasks:
                try:
                    await self.bot.http.send_typing(channel.id)
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"error in typing loop: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(loop_task())
        self.typing_tasks[channel.id] = task
        await self.save_state()

    async def start_auto_reply(self, user_id, channel, user_name):
        self.auto_replies[user_id] = {
            'channel_id': channel.id,
            'user_name': user_name,
            'start_time': time.time()
        }
        await self.save_state()

    async def start_group_name_loop(self, channel, names, delay: float):
        channel_id = channel.id

        if channel_id in self.group_name_changes:
            data = self.group_name_changes[channel_id]
            if 'task' in data:
                data['task'].cancel()
            del self.group_name_changes[channel_id]

        if self.state['stopped_by_command'].get(f"chg_{channel_id}"):
            del self.state['stopped_by_command'][f"chg_{channel_id}"]

        if isinstance(names, str):
            names = [names]

        async def loop_task():
            consecutive_errors = 0
            max_errors = 10
            name_index = 0
            MIN_DELAY = 1.5
            first_run = True

            while channel_id in self.group_name_changes and self.is_valid:
                try:
                    current_name = names[name_index % len(names)]
                    name_index += 1

                    bypass_client = self.get_bypass_client()

                    if first_run:
                        first_run = False
                        try:
                            headers = {
                                "Authorization": self.token,
                                "Content-Type": "application/json",
                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                            }
                            async with aiohttp.ClientSession() as _s:
                                async with _s.patch(
                                    f"https://discord.com/api/v10/channels/{channel_id}",
                                    json={"name": current_name},
                                    headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=10)
                                ) as _r:
                                    if _r.status == 429:
                                        data = await _r.json()
                                        retry_after = data.get("retry_after", 5)
                                        self.rate_limit_penalty.on_rate_limit()
                                        await asyncio.sleep(retry_after)
                                        name_index -= 1
                                        first_run = True
                                        continue
                                    elif 200 <= _r.status < 300:
                                        self.rate_limit_penalty.on_success()
                                        consecutive_errors = 0
                                    elif _r.status == 401:
                                        self.is_valid = False
                                        remove_token_from_file(self.token)
                                        return
                                    else:
                                        consecutive_errors += 1
                        except Exception as _e:
                            self.logger.error(f"chg first run direct error: {_e}")
                            consecutive_errors += 1
                        if consecutive_errors >= max_errors:
                            break
                        total_delay = max(delay, MIN_DELAY) + self.rate_limit_penalty.get_penalty_delay()
                        await asyncio.sleep(total_delay)
                        continue

                    if bypass_client and bypass_client.is_valid:
                        result_event = asyncio.Event()
                        result_holder = {}

                        def _cb(status, text):
                            result_holder['status'] = status
                            try:
                                loop = asyncio.get_event_loop()
                                loop.call_soon_threadsafe(result_event.set)
                            except:
                                result_event.set()

                        bypass_client.add_job("PATCH", f"/channels/{channel_id}", json_data={"name": current_name}, callback=_cb)
                        try:
                            await asyncio.wait_for(result_event.wait(), timeout=15)
                        except asyncio.TimeoutError:
                            pass

                        status = result_holder.get('status', 0)
                        if 200 <= status < 300:
                            self.rate_limit_penalty.on_success()
                            consecutive_errors = 0
                        elif status == 429:
                            self.rate_limit_penalty.on_rate_limit()
                            await asyncio.sleep(5)
                            continue
                        elif status == 401:
                            self.is_valid = False
                            remove_token_from_file(self.token)
                            break
                        else:
                            consecutive_errors += 1
                    else:
                        try:
                            await channel.edit(name=current_name)
                            self.rate_limit_penalty.on_success()
                            consecutive_errors = 0
                        except discord.errors.HTTPException as e:
                            if e.status == 401:
                                self.is_valid = False
                                remove_token_from_file(self.token)
                                break
                            elif e.status == 429:
                                retry_after = getattr(e, 'retry_after', 30)
                                self.rate_limit_penalty.on_rate_limit()
                                await asyncio.sleep(retry_after)
                                continue
                            else:
                                consecutive_errors += 1

                    if consecutive_errors >= max_errors:
                        break

                    if delay <= 0 and name_index >= len(names):
                        if channel_id in self.group_name_changes:
                            del self.group_name_changes[channel_id]
                        await self.save_state()
                        break

                    total_delay = max(delay, MIN_DELAY) + self.rate_limit_penalty.get_penalty_delay()
                    await asyncio.sleep(total_delay)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"error in group name loop: {e}")
                    consecutive_errors += 1
                    if consecutive_errors >= max_errors:
                        break
                    await asyncio.sleep(max(delay, MIN_DELAY))

        task = asyncio.create_task(loop_task())
        self.group_name_changes[channel_id] = {
            'task': task,
            'names': names,
            'delay': delay
        }
        await self.save_state()
        self.logger.info(f"started group name change loop: {names} (delay: {delay}s)")

    async def stop_group_name_loop(self, channel_id: int):
        self.state['stopped_by_command'][f"chg_{channel_id}"] = True
        if channel_id in self.group_name_changes:
            data = self.group_name_changes[channel_id]
            if 'task' in data:
                data['task'].cancel()
            del self.group_name_changes[channel_id]
            await self.save_state()
            self.logger.info(f"stopped group name change loop for {channel_id}")

    async def run(self):
        self.is_running = True
        while self.is_running and self.is_valid:
            try:
                await self.bot.start(self.token, bot=False, reconnect=True)
            except KeyboardInterrupt:
                self.logger.info("received keyboard interrupt")
                break
            except discord.errors.LoginFailure:
                self.logger.error("login failed - invalid token")
                self.is_valid = False
                remove_token_from_file(self.token)
                break
            except Exception as e:
                if not self.is_valid:
                    break

                error_str = str(e).lower()
                if "401" in error_str or "unauthorized" in error_str:
                    self.logger.error("token invalid (401)")
                    self.is_valid = False
                    remove_token_from_file(self.token)
                    break

                self.logger.error(f"bot crashed: {e}")
                self.reconnect_attempts += 1
                delay = min(self.reconnect_attempts * 5, self.max_reconnect_delay)
                self.logger.info(f"reconnecting in {delay}s (attempt {self.reconnect_attempts})...")
                await asyncio.sleep(delay)

    async def stop(self):
        self.is_running = False
        if self.health_check_task:
            self.health_check_task.cancel()
        if self.status_text_task:
            self.status_text_task.cancel()
        await self.bot.close()

async def main():
    global running_instances, bypass_manager

    create_default_files()

    tokens = await validate_and_clean_tokens()

    if not tokens:
        logging.error("no valid tokens found!")
        return

    logging.info(f"starting with {len(tokens)} valid tokens")

    if _pm_instance is not None:
        pass # _pm_instance already started by start_proxy_manager()

    bypass_manager = BypassManager(tokens)
    bypass_manager.start_all()
    logging.info("bypass manager started")

    for idx, token in enumerate(tokens):
        instance = BotInstance(token, idx)
        running_instances.append(instance)

    try:
        await asyncio.gather(*[inst.run() for inst in running_instances])
    except KeyboardInterrupt:
        logging.info("shutting down all bots...")
        for inst in running_instances:
            await inst.stop()

if __name__ == "__main__":
    try:
        print(fade.water(loading))
        print("\n")
        loading_animation()
        clear_terminal_event()
        print(fade.fire(text))
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("program terminated")
