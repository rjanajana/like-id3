from flask import Flask, request, jsonify
import os
import asyncio
import aiohttp
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import binascii
import requests
import json
import like_pb2
import like_count_pb2
import uid_generator_pb2
from google.protobuf.message import DecodeError
from threading import Lock

app = Flask(__name__)

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
MAX_CONCURRENT = 50   # ek waqt mein kitne async requests
RETRY_COUNT    = 2    # fail hone par retry
REQUEST_TIMEOUT = 15  # seconds

# ─────────────────────────────────────────
# TOKEN SYSTEM
# ─────────────────────────────────────────
_token_cache = {}
_token_index = {}
_token_mtime = {}
_token_lock  = Lock()

def _server_key(server_name):
    if server_name == "IND":
        return "IND"
    elif server_name in {"BR", "US", "SAC", "NA"}:
        return "BR"
    else:
        return "BD"

def _token_file(key):
    return {"IND": "token_ind.json", "BR": "token_br.json", "BD": "token_bd.json"}.get(key)

def load_tokens(server_name):
    key = _server_key(server_name)
    fname = _token_file(key)
    
    if not os.path.exists(fname):
        app.logger.error(f"Token file not found: [{key}]")
        return None
        
    current_mtime = os.path.getmtime(fname)

    with _token_lock:
        # Check if cache is missing or file was modified
        if key not in _token_cache or _token_mtime.get(key) != current_mtime:
            try:
                with open(fname, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not data:
                    app.logger.error(f"Token file empty: {fname}")
                    return None
                
                _token_cache[key] = data
                _token_mtime[key] = current_mtime
                _token_index[key] = 0
                app.logger.info(f"Auto-Reloaded {len(data)} tokens for [{key}]")
            except Exception as e:
                app.logger.error(f"Token load error [{key}]: {e}")
                return None
    return _token_cache.get(key)

def get_next_token(server_name):
    key    = _server_key(server_name)
    tokens = load_tokens(server_name)
    if not tokens:
        return None
    with _token_lock:
        idx            = _token_index.get(key, 0)
        token          = tokens[idx % len(tokens)]["token"]
        _token_index[key] = (idx + 1) % len(tokens)
    return token

def reload_tokens(server_name=None):
    with _token_lock:
        if server_name:
            key = _server_key(server_name)
            _token_cache.pop(key, None)
            _token_index.pop(key, None)
            _token_mtime.pop(key, None)
        else:
            _token_cache.clear()
            _token_index.clear()
            _token_mtime.clear()

def get_token_count(server_name):
    tokens = load_tokens(server_name)
    return len(tokens) if tokens else 0

# ─────────────────────────────────────────
# CRYPTO
# ─────────────────────────────────────────
def encrypt_message(plaintext):
    try:
        key    = b'Yg&tc%DEuh6%Zc^8'
        iv     = b'6oyZDr22E3ychjM%'
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded = pad(plaintext, AES.block_size)
        return binascii.hexlify(cipher.encrypt(padded)).decode('utf-8')
    except Exception as e:
        app.logger.error(f"Encrypt error: {e}")
        return None

# ─────────────────────────────────────────
# PROTOBUF HELPERS
# ─────────────────────────────────────────
def create_protobuf_message(user_id, region):
    try:
        msg        = like_pb2.like()
        msg.uid    = int(user_id)
        msg.region = region
        return msg.SerializeToString()
    except Exception as e:
        app.logger.error(f"Protobuf create error: {e}")
        return None

def create_protobuf(uid):
    try:
        msg         = uid_generator_pb2.uid_generator()
        msg.saturn_ = int(uid)
        msg.garena  = 1
        return msg.SerializeToString()
    except Exception as e:
        app.logger.error(f"UID protobuf error: {e}")
        return None

def enc(uid):
    data = create_protobuf(uid)
    return encrypt_message(data) if data else None

def decode_protobuf(binary):
    try:
        items = like_count_pb2.Info()
        items.ParseFromString(binary)
        return items
    except DecodeError as e:
        app.logger.error(f"Protobuf decode error: {e}")
        return None
    except Exception as e:
        app.logger.error(f"Protobuf unexpected error: {e}")
        return None

# ─────────────────────────────────────────
# URL HELPER
# ─────────────────────────────────────────
def get_url(server_name, endpoint):
    base = {
        "IND": "https://client.ind.freefiremobile.com",
        "BR":  "https://client.us.freefiremobile.com",
        "US":  "https://client.us.freefiremobile.com",
        "SAC": "https://client.us.freefiremobile.com",
        "NA":  "https://client.us.freefiremobile.com",
    }
    return f"{base.get(server_name, 'https://clientbp.ggblueshark.com')}/{endpoint}"

# ─────────────────────────────────────────
# HTTP HEADERS
# ─────────────────────────────────────────
_HEADERS_BASE = {
    'User-Agent':      "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
    'Connection':      "Keep-Alive",
    'Accept-Encoding': "gzip",
    'Content-Type':    "application/x-www-form-urlencoded",
    'Expect':          "100-continue",
    'X-Unity-Version': "2018.4.11f1",
    'X-GA':            "v1 1",
    'ReleaseVersion':  "OB54"
}

def _make_headers(token):
    return {**_HEADERS_BASE, 'Authorization': f"Bearer {token}"}

# ─────────────────────────────────────────
# ASYNC CORE — HAR TOKEN PROCESS HOGA
# ─────────────────────────────────────────
async def _send_one(session, semaphore, edata, token, url, token_idx):
    """
    Single token ke liye request bhejta hai.
    Retry bhi karta hai RETRY_COUNT baar.
    Returns: (token_idx, "ok" | "timeout" | "http_XXX" | "err:...")
    """
    for attempt in range(1, RETRY_COUNT + 2):   # 1 original + RETRY_COUNT retries
        try:
            async with semaphore:
                async with session.post(
                    url, data=edata,
                    headers=_make_headers(token),
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    if resp.status == 200:
                        return (token_idx, "ok")
                    else:
                        result = f"http_{resp.status}"
        except asyncio.TimeoutError:
            result = "timeout"
        except Exception as e:
            result = f"err:{e}"

        if attempt <= RETRY_COUNT:
            app.logger.debug(f"Token[{token_idx}] attempt {attempt} failed ({result}), retrying...")
            await asyncio.sleep(0.3 * attempt)

    return (token_idx, result)


async def send_all_tokens(uid, server_name, url):
    """
    ── MAIN ENHANCEMENT ──
    Server ke SAARE tokens use karta hai.
    Har token exactly ek baar use hoga.
    Semaphore se MAX_CONCURRENT requests ek waqt mein.
    Returns: dict with stats
    """
    tokens = load_tokens(server_name)
    if not tokens:
        return None

    protobuf = create_protobuf_message(uid, server_name)
    if not protobuf:
        return None
    encrypted = encrypt_message(protobuf)
    if not encrypted:
        return None

    edata     = bytes.fromhex(encrypted)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    total     = len(tokens)

    app.logger.info(f"Processing ALL {total} tokens for server [{server_name}]")

    async with aiohttp.ClientSession() as session:
        tasks = [
            _send_one(session, semaphore, edata, t["token"], url, i)
            for i, t in enumerate(tokens)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # ── Stats compile karo ──
    ok       = 0
    timeouts = 0
    http_err = 0
    other    = 0

    for r in results:
        if isinstance(r, Exception):
            other += 1
            continue
        _, status = r
        if status == "ok":
            ok += 1
        elif status == "timeout":
            timeouts += 1
        elif status.startswith("http_"):
            http_err += 1
        else:
            other += 1

    stats = {
        "total_tokens":  total,
        "success":       ok,
        "timeout":       timeouts,
        "http_errors":   http_err,
        "other_errors":  other,
        "success_rate":  f"{round(ok / total * 100, 1)}%" if total else "0%"
    }
    app.logger.info(f"Done | {stats}")
    return stats

# ─────────────────────────────────────────
# SYNC PLAYER FETCH
# ─────────────────────────────────────────
def make_request(encrypt, server_name, token):
    try:
        url   = get_url(server_name, "GetPlayerPersonalShow")
        edata = bytes.fromhex(encrypt)
        resp  = requests.post(url, data=edata, headers=_make_headers(token), verify=False, timeout=15)
        return decode_protobuf(bytes.fromhex(resp.content.hex()))
    except Exception as e:
        app.logger.error(f"make_request error: {e}")
        return None

def fetch_player_info(uid):
    try:
        r = requests.get(f"https://nr-codex-info.vercel.app/get?uid={uid}", timeout=8)
        if r.status_code == 200:
            info = r.json().get("AccountInfo", {})
            return {
                "Level":          info.get("AccountLevel",  "NA"),
                "Region":         info.get("AccountRegion", "NA"),
                "ReleaseVersion": info.get("ReleaseVersion","NA")
            }
    except Exception as e:
        app.logger.error(f"fetch_player_info error: {e}")
    return {"Level": "NA", "Region": "NA", "ReleaseVersion": "NA"}

# ─────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────
@app.route('/like', methods=['GET'])
def handle_requests():
    uid         = request.args.get("uid", "").strip()
    server_name = request.args.get("server_name", "").upper().strip()

    if not uid or not server_name:
        return jsonify({"error": "uid and server_name required"}), 400

    try:
        # 1. Player info
        player_info = fetch_player_info(uid)
        region      = player_info["Region"]
        level       = player_info["Level"]
        rel_ver     = player_info["ReleaseVersion"]

        server_used = region if (region != "NA" and server_name != region) else server_name

        # 2. Before likes
        token_before = get_next_token(server_used)
        if not token_before:
            return jsonify({"error": "No tokens available"}), 503

        encrypted_uid = enc(uid)
        if not encrypted_uid:
            return jsonify({"error": "UID encryption failed"}), 500

        before_proto = make_request(encrypted_uid, server_used, token_before)
        if not before_proto:
            return jsonify({"error": "Failed to fetch before-likes"}), 500

        data_before = json.loads(MessageToJson(before_proto))
        before_like = int(data_before.get('AccountInfo', {}).get('Likes', 0) or 0)

        # 3. Send ALL tokens
        like_url = get_url(server_used, "LikeProfile")
        stats    = asyncio.run(send_all_tokens(uid, server_used, like_url))
        if stats is None:
            return jsonify({"error": "Like sending failed"}), 500

        # 4. After likes
        token_after = get_next_token(server_used)
        after_proto = make_request(encrypted_uid, server_used, token_after or token_before)
        if not after_proto:
            return jsonify({"error": "Failed to fetch after-likes"}), 500

        data_after  = json.loads(MessageToJson(after_proto))
        after_like  = int(data_after.get('AccountInfo', {}).get('Likes', 0) or 0)
        player_uid  = int(data_after.get('AccountInfo', {}).get('UID', 0) or 0)
        player_name = str(data_after.get('AccountInfo', {}).get('PlayerNickname', '') or '')
        like_given  = after_like - before_like

        return jsonify({
            "LikesGivenByAPI":    like_given,
            "LikesafterCommand":  after_like,
            "LikesbeforeCommand": before_like,
            "PlayerNickname":     player_name,
            "Region":             region,
            "Level":              level,
            "UID":                player_uid,
            "ReleaseVersion":     rel_ver,
            "status":             1 if like_given != 0 else 2,
            # ── Extra stats ──
            "TokenStats": {
                "TotalProcessed": stats["total_tokens"],
                "Success":        stats["success"],
                "Timeouts":       stats["timeout"],
                "HttpErrors":     stats["http_errors"],
                "OtherErrors":    stats["other_errors"],
                "SuccessRate":    stats["success_rate"]
            }
        })

    except Exception as e:
        app.logger.error(f"/like error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/status', methods=['GET'])
def status_route():
    """Har server ke loaded tokens ki count dikhata hai"""
    servers = ["IND", "BR", "BD"]
    counts  = {}
    for s in servers:
        counts[s] = get_token_count(s)
    return jsonify({
        "token_counts": counts,
        "max_concurrent": MAX_CONCURRENT,
        "retry_count":    RETRY_COUNT
    })


@app.route('/reload_tokens', methods=['GET'])
def reload_tokens_route():
    server = request.args.get("server", "all").upper()
    reload_tokens(None if server == "ALL" else server)
    return jsonify({"message": f"Tokens reloaded: {server}"}), 200


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
