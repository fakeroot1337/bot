from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackContext
import os
from datetime import datetime
import time
import glob
import re
import asyncio
import signal
import sys
import json
from collections import defaultdict
from typing import Dict, Set, List, Tuple
import threading
import queue
import aiohttp
import urllib.parse

# ==================== GLOBAL VARIABLES ====================
coin_balance = {}
ALLOWED_GROUPS = set()
search_tasks = {}
user_requests = defaultdict(list)
processed_files_cache = {}

# বট সেটিংস
BOT_TOKEN = "7453670842:AAGnJdTltGB8UhB2cN3g4HgX51b4lx_zG9k"
BOT_OWNER_ID = 5472497832
ADMIN_IDS = [5472497832, 1294008126, 5614361085]

# API সেটিংস
API_URL = "https://sixeye.fwh.is/zeroleakapi.php"
API_KEY = "7290888"
USE_API = True  # API ব্যবহার করতে চাইলে True, লগ ফাইল ব্যবহার করতে চাইলে False

# ফাইল পাথ
LOGS_FOLDER = "logs"
COIN_FILE = "coin.txt"
GROUP_FILE = "group.txt"
BACKUP_FILE = "bot_backup.json"

# রেট লিমিট সেটিংস
RATE_LIMIT = {
    'free': {'window': 60, 'limit': 3},
    'paid': {'window': 60, 'limit': 10},
    'command': {'window': 30, 'limit': 10}
}

# ==================== API FUNCTIONS ====================
async def search_from_api(keyword: str, max_results: int = 100) -> List[str]:
    """API থেকে ডেটা সার্চ করে"""
    results = []
    
    if not USE_API:
        return results
    
    try:
        # URL এনকোড করা
        encoded_keyword = urllib.parse.quote(keyword)
        url = f"{API_URL}?api={API_KEY}&url={encoded_keyword}"
        
        print(f"🔗 API Request: {url}")
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.text()
                    
                    # API থেকে ডেটা পার্স করা
                    if data.strip():
                        # বিভিন্ন ফরম্যাটের ডেটা হ্যান্ডেল করা
                        lines = data.split('\n')
                        
                        for line in lines:
                            line = line.strip()
                            if line:
                                # লাইনে সার্চ কীওয়ার্ড আছে কিনা চেক করা
                                if keyword.lower() in line.lower():
                                    results.append(line)
                                
                                # অথবা যদি API নির্দিষ্ট ফরম্যাটে ডেটা দেয়
                                elif ':' in line or '@' in line or '.' in line:
                                    results.append(line)
                            
                            if len(results) >= max_results:
                                break
                    
                    print(f"✅ API থেকে {len(results)} ফলাফল পাওয়া গেছে")
                else:
                    print(f"❌ API Error: Status {response.status}")
    
    except aiohttp.ClientError as e:
        print(f"❌ Network Error: {e}")
    except asyncio.TimeoutError:
        print(f"❌ API Timeout")
    except Exception as e:
        print(f"❌ API Search Error: {e}")
    
    return results

async def hybrid_search(keyword: str, max_files: int = 50, max_results: int = 1000) -> List[str]:
    """হাইব্রিড সার্চ - API এবং লোকাল ফাইল দুটো থেকেই"""
    all_results = []
    
    try:
        print(f"🔍 Hybrid search for: {keyword}")
        
        # API থেকে সার্চ (একসাথে রান করবে)
        api_task = asyncio.create_task(search_from_api(keyword, max_results))
        
        # লোকাল ফাইল থেকে সার্চ (যদি API না থাকে)
        local_results = []
        if not USE_API:
            local_results = fast_accurate_search(keyword, max_files, max_results)
        
        # API রেস্পন্সের জন্য অপেক্ষা
        api_results = await api_task
        
        # সব রেজাল্ট মার্জ করা
        all_results.extend(api_results)
        all_results.extend(local_results)
        
        # ডুপ্লিকেট রিমুভ
        unique_results = []
        seen_lines = set()
        
        for result in all_results:
            result_hash = hash(result.strip())
            if result_hash not in seen_lines:
                unique_results.append(result)
                seen_lines.add(result_hash)
        
        print(f"✅ Total unique results: {len(unique_results)} (API: {len(api_results)}, Local: {len(local_results)})")
        
        return unique_results[:max_results]
        
    except Exception as e:
        print(f"❌ Hybrid search error: {e}")
        import traceback
        traceback.print_exc()
        
        # ফ্যালব্যাক হিসেবে লোকাল সার্চ
        if not USE_API:
            return fast_accurate_search(keyword, max_files, max_results)
        return []

# ==================== GUI CLASS ====================
class GUI:
    @staticmethod
    def create_box(text: str, title: str = None) -> str:
        lines = text.split('\n')
        max_len = max(len(line) for line in lines) if lines else 0
        box_width = max(max_len + 4, 40)
        
        if title:
            top = f"┌─{'─' * (box_width-4)}─┐\n"
            title_line = f"│ {title.center(box_width-4)} │\n"
            separator = f"├{'─' * (box_width-2)}┤\n"
        else:
            top = f"┌{'─' * (box_width-2)}┐\n"
            title_line = ""
            separator = ""
        
        middle = ""
        for line in lines:
            middle += f"│ {line.ljust(box_width-4)} │\n"
        
        bottom = f"└{'─' * (box_width-2)}┘"
        
        return top + title_line + separator + middle + bottom
    
    @staticmethod
    def create_header(title: str = "SYSTEMADMINBD LOG BOT") -> str:
        now = datetime.now()
        date_str = now.strftime("%d %B, %Y")
        time_str = now.strftime("%I:%M:%S %p")
        
        header = "╔════════════════════════════════════════════════╗\n"
        header += f"║{' '*48}║\n"
        header += f"║{' '*10}🚀 {title}{' ' * (48 - len(title) - 10)}║\n"
        header += f"║{' '*48}║\n"
        header += f"║ 📅 {date_str}{' '* (48 - len(date_str) - 4)}║\n"
        header += f"║ 🕐 {time_str}{' '* (48 - len(time_str) - 4)}║\n"
        header += "╚════════════════════════════════════════════════╝"
        return header
    
    @staticmethod
    def create_status_box(status: str, icon: str = "📊") -> str:
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        
        box = f"""
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃          {icon} SYSTEM STATUS          ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃ 🔄 Status: {status:<26} ┃
┃ 🕐 Time: {timestamp:<27} ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
"""
        return box.strip()
    
    @staticmethod
    def create_api_status_box() -> str:
        """API স্ট্যাটাস দেখাবে"""
        api_status = "✅ ACTIVE" if USE_API else "❌ INACTIVE"
        source = "🌐 API" if USE_API else "📁 LOCAL FILES"
        
        box = f"""
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃            🔧 DATA SOURCE            ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃ 📡 Source: {source:<27} ┃
┃ 🚀 API Status: {api_status:<24} ┃
┃ 🔑 API Key: {'Connected' if USE_API else 'Not Used':<27} ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
"""
        return box.strip()

# ==================== FILE OPERATIONS ====================
def load_groups_from_file():
    global ALLOWED_GROUPS
    try:
        if os.path.exists(GROUP_FILE):
            with open(GROUP_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and ':' in line:
                        parts = line.split(':')
                        if len(parts) >= 2:
                            try:
                                group_id = int(parts[0].strip())
                                ALLOWED_GROUPS.add(group_id)
                            except ValueError:
                                continue
        print(f"📥 Loaded {len(ALLOWED_GROUPS)} groups")
    except Exception as e:
        print(f"❌ Error loading groups: {e}")

def save_groups_to_file():
    try:
        with open(GROUP_FILE, 'w', encoding='utf-8') as f:
            for group_id in ALLOWED_GROUPS:
                f.write(f"{group_id}:Verified_Group\n")
    except Exception as e:
        print(f"❌ Error saving groups: {e}")

def load_coins_from_file():
    global coin_balance
    try:
        if os.path.exists(COIN_FILE):
            with open(COIN_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and ':' in line:
                        parts = line.split(':')
                        if len(parts) >= 2:
                            username = parts[0].strip()
                            try:
                                coins = int(parts[1].strip())
                                coin_balance[username] = coins
                            except ValueError:
                                continue
        print(f"📥 Loaded {len(coin_balance)} user coins")
    except Exception as e:
        print(f"❌ Error loading coins: {e}")

def save_coins_to_file():
    try:
        with open(COIN_FILE, 'w', encoding='utf-8') as f:
            for username, coins in coin_balance.items():
                f.write(f"{username}:{coins}\n")
    except Exception as e:
        print(f"❌ Error saving coins: {e}")

def save_backup():
    try:
        backup_data = {
            'allowed_groups': list(ALLOWED_GROUPS),
            'coin_balance': coin_balance,
            'timestamp': datetime.now().isoformat()
        }
        with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, indent=2)
    except Exception as e:
        print(f"❌ Error saving backup: {e}")

def load_backup():
    try:
        if os.path.exists(BACKUP_FILE):
            with open(BACKUP_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                ALLOWED_GROUPS.update(data.get('allowed_groups', []))
                coin_balance.update(data.get('coin_balance', {}))
    except Exception as e:
        print(f"❌ Error loading backup: {e}")

# ==================== RATE LIMIT SYSTEM ====================
def check_rate_limit(user_id: int, action: str = 'command') -> bool:
    now = time.time()
    user_key = f"{user_id}_{action}"
    
    user_requests[user_key] = [req for req in user_requests[user_key] 
                              if now - req < RATE_LIMIT[action]['window']]
    
    if len(user_requests[user_key]) >= RATE_LIMIT[action]['limit']:
        return False
    
    user_requests[user_key].append(now)
    return True

# ==================== SIGNAL HANDLERS ====================
def setup_signal_handlers():
    def signal_handler(signum, frame):
        print(f"\n⚠️ Received signal {signum}, saving data...")
        save_coins_to_file()
        save_groups_to_file()
        save_backup()
        print("✅ Data saved. Exiting...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

# ==================== INITIALIZE DATA ====================
def initialize_data():
    load_backup()
    load_coins_from_file()
    load_groups_from_file()
    
    if not USE_API:
        # শুধুমাত্র API না চালু থাকলে লগ ফাইল স্ক্যান করবে
        scan_logs_folder()
    
    print("📊 Initialization complete")
    print(GUI.create_api_status_box())

# ==================== LOGS FOLDER SCANNER ====================
def scan_logs_folder():
    """logs ফোল্ডার স্ক্যান এবং ফাইল লিস্ট তৈরি"""
    try:
        if not os.path.exists(LOGS_FOLDER):
            os.makedirs(LOGS_FOLDER)
            print(f"📁 Created logs folder: {LOGS_FOLDER}")
            return
        
        # সব ধরনের টেক্সট ফাইল চেক
        file_patterns = ['*.txt', '*.log', '*.csv', '*.json', '*.xml']
        all_files = []
        
        for pattern in file_patterns:
            files = glob.glob(os.path.join(LOGS_FOLDER, pattern))
            all_files.extend(files)
        
        # Subdirectories থেকেও ফাইল চেক
        for root, dirs, files in os.walk(LOGS_FOLDER):
            for file in files:
                if file.lower().endswith(('.txt', '.log', '.csv', '.json', '.xml')):
                    all_files.append(os.path.join(root, file))
        
        print(f"📁 Found {len(all_files)} files in logs folder:")
        for i, file_path in enumerate(all_files[:10]):  # প্রথম ১০টি ফাইল দেখাবে
            file_size = os.path.getsize(file_path) / 1024  # KB তে
            print(f"  {i+1}. {os.path.basename(file_path)} ({file_size:.1f} KB)")
        
        if len(all_files) > 10:
            print(f"  ... and {len(all_files) - 10} more files")
            
    except Exception as e:
        print(f"❌ Error scanning logs folder: {e}")

# ==================== ADVANCED SEARCH FUNCTIONS ====================
def normalize_keyword(keyword: str) -> List[str]:
    """কীওয়ার্ড নরমালাইজ করবে সঠিক সার্চের জন্য"""
    # স্পেশাল ক্যারেক্টার রিমুভ
    keyword = re.sub(r'[^\w\s\-\.@]', ' ', keyword.lower())
    
    # সাধারণ ডোমেইন এক্সটেনশন রিমুভ
    keyword = re.sub(r'\.(com|net|org|edu|gov|in|bd|uk|us|info|biz|co|io|me)$', '', keyword)
    
    # http/https/www রিমুভ
    keyword = re.sub(r'^(https?://|www\.)', '', keyword)
    
    # এক্সট্রা স্পেস রিমুভ
    keyword = ' '.join(keyword.split())
    
    # কীওয়ার্ড স্প্লিট
    words = keyword.split()
    
    # ছোট শব্দ ফিল্টার (২ অক্ষরের কম)
    words = [word for word in words if len(word) >= 3]
    
    return words

def search_in_file_comprehensive(log_file: str, target_words: List[str]) -> List[str]:
    """একটি ফাইলে গভীর সার্চ"""
    file_results = []
    try:
        # ফাইল সাইজ চেক (বড় ফাইল এর জন্য আলাদা ট্রিটমেন্ট)
        file_size = os.path.getsize(log_file)
        
        if file_size > 50 * 1024 * 1024:  # 50MB এর বেশি হলে
            print(f"⚠️ Large file detected: {os.path.basename(log_file)} ({file_size/1024/1024:.1f} MB)")
            # বড় ফাইলের জন্য অপটিমাইজড রিডিং
            return search_in_large_file(log_file, target_words)
        
        # ছোট ফাইলের জন্য নরমাল রিডিং
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # লাইন বাই লাইন প্রসেস
        lines = content.split('\n')
        
        for line in lines:
            line_lower = line.lower()
            
            # সঠিক ম্যাচিং লজিক
            match_score = 0
            
            # সব কীওয়ার্ড ম্যাচ করলে হাই স্কোর
            if all(word in line_lower for word in target_words):
                match_score = 100
            # ৭০% কীওয়ার্ড ম্যাচ করলে মিডিয়াম স্কোর
            elif sum(1 for word in target_words if word in line_lower) >= len(target_words) * 0.7:
                match_score = 70
            # মেইন কীওয়ার্ড ম্যাচ করলে (প্রথম ৩টি)
            elif len(target_words) >= 3 and all(word in line_lower for word in target_words[:3]):
                match_score = 80
            
            if match_score >= 70:
                # ডুপ্লিকেট চেক
                line_hash = hash(line.strip())
                if line_hash not in processed_files_cache.get(log_file, set()):
                    file_results.append(line.strip())
                    if log_file not in processed_files_cache:
                        processed_files_cache[log_file] = set()
                    processed_files_cache[log_file].add(line_hash)
                    
                    if len(file_results) >= 150:
                        break
        
        # টেম্পোরারি ক্যাশে ক্লিন
        if log_file in processed_files_cache and len(processed_files_cache[log_file]) > 1000:
            processed_files_cache[log_file] = set()
            
    except UnicodeDecodeError:
        # UTF-8 ফেল করলে অন্যান্য এনকোডিং ট্রাই
        try:
            with open(log_file, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
            
            lines = content.split('\n')
            for line in lines[:100]:  # শুধু প্রথম ১০০ লাইন
                line_lower = line.lower()
                if all(word in line_lower for word in target_words):
                    file_results.append(line.strip())
                    if len(file_results) >= 50:
                        break
                        
        except Exception:
            pass
            
    except Exception as e:
        print(f"⚠️ Error reading {log_file}: {e}")
    
    return file_results

def search_in_large_file(log_file: str, target_words: List[str]) -> List[str]:
    """বড় ফাইলের জন্য মেমোরি-এফিসিয়েন্ট সার্চ"""
    file_results = []
    try:
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            chunk_size = 8192
            buffer = ''
            lines_processed = 0
            
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                
                buffer += chunk
                lines = buffer.split('\n')
                buffer = lines[-1]
                
                for line in lines[:-1]:
                    lines_processed += 1
                    if lines_processed > 10000:  # সর্বোচ্চ ১০,০০০ লাইন প্রসেস
                        return file_results
                    
                    line_lower = line.lower()
                    if all(word in line_lower for word in target_words[:2]):  # প্রথম ২টি কীওয়ার্ড
                        file_results.append(line.strip())
                        if len(file_results) >= 50:
                            return file_results
                
    except Exception:
        pass
    
    return file_results

def find_all_log_files():
    """logs ফোল্ডারে সব ধরনের টেক্সট ফাইল খুঁজে বের করে"""
    all_files = []
    
    try:
        # প্রধান প্যাটার্ন
        patterns = ['*.txt', '*.log', '*.csv', '*.json', '*.xml', '*.dat']
        
        for pattern in patterns:
            files = glob.glob(os.path.join(LOGS_FOLDER, pattern))
            all_files.extend(files)
        
        # Subdirectories থেকেও
        for root, dirs, files in os.walk(LOGS_FOLDER):
            for file in files:
                file_lower = file.lower()
                if (file_lower.endswith(('.txt', '.log', '.csv', '.json', '.xml', '.dat')) or
                    'log' in file_lower or 'data' in file_lower or 'dump' in file_lower):
                    all_files.append(os.path.join(root, file))
        
        # ডুপ্লিকেট রিমুভ
        all_files = list(set(all_files))
        
        # ফাইল সাইজ অনুযায়ী সর্ট (ছোট ফাইল আগে)
        all_files.sort(key=lambda x: os.path.getsize(x) if os.path.exists(x) else 0)
        
    except Exception as e:
        print(f"❌ Error finding log files: {e}")
    
    return all_files

def fast_accurate_search(target_text: str, max_files: int = 50, max_results: int = 1000) -> List[str]:
    """ফাস্ট এবং অ্যাকুরেট সার্চ (শুধুমাত্র লোকাল ফাইলের জন্য)"""
    all_results = []
    
    try:
        # কীওয়ার্ড প্রিপারেশন
        target_words = normalize_keyword(target_text)
        
        if not target_words:
            return []
        
        print(f"🔍 Local search for: {target_text}")
        print(f"📋 Keywords: {target_words}")
        
        # সব ফাইল খুঁজে বের করো
        log_files = find_all_log_files()
        
        if not log_files:
            print("❌ No log files found!")
            return []
        
        print(f"📁 Found {len(log_files)} total files")
        
        # ফাইল লিমিট
        log_files = log_files[:max_files]
        print(f"🔍 Processing {len(log_files)} files...")
        
        # সিঙ্গেল থ্রেডেড সার্চ (সরলীকৃত)
        for log_file in log_files:
            file_results = search_in_file_comprehensive(log_file, target_words)
            all_results.extend(file_results)
            
            if len(all_results) >= max_results:
                break
        
        # ডুপ্লিকেট রিমুভ
        unique_results = []
        seen_lines = set()
        
        for result in all_results:
            result_hash = hash(result.strip())
            if result_hash not in seen_lines:
                unique_results.append(result)
                seen_lines.add(result_hash)
        
        print(f"✅ Found {len(unique_results)} unique results")
        
        return unique_results[:max_results]
        
    except Exception as e:
        print(f"❌ Search error: {e}")
        import traceback
        traceback.print_exc()
        return []

# ==================== QUICK REPLY FUNCTION ====================
async def quick_reply(update: Update, text: str):
    try:
        await update.message.reply_text(text)
    except Exception as e:
        print(f"❌ Quick reply error: {e}")

# ==================== COMMAND HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        chat = update.message.chat
        
        if not check_rate_limit(user.id, 'command'):
            await quick_reply(update, "⚠️ Too many requests! Please wait...")
            return
        
        # স্ট্যাটাস মেসেজ
        data_source = "🌐 API" if USE_API else "📁 LOCAL FILES"
        
        if chat.type == 'private':
            user_name = user.first_name or "Friend"
            
            welcome_msg = f"""
{GUI.create_header("Welcome")}

{GUI.create_api_status_box()}

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃        🎉 WELCOME {user_name.upper():<10}       ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃ 📋 Available Commands:               ┃
┃ • /help - Show all commands          ┃
┃ • /myplan - Check subscription       ┃
┃ • /free - Search free logs          ┃
┃ • /paid - Premium logs (1 coin)     ┃
┃ • /coin - Check coin balance        ┃
┃ • /source - Show data source        ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

📊 Data Source: {data_source}
📢 Support: @systemadminbd_bot
🌟 Developed by: SYSTEMADMINBD TEAM
"""
            
            await quick_reply(update, welcome_msg)
            return
        
        if chat.type in ['group', 'supergroup'] and chat.id not in ALLOWED_GROUPS and user.id != BOT_OWNER_ID and user.id not in ADMIN_IDS:
            error_msg = GUI.create_box(
                f"❌ This group is not verified!\nContact admin to verify this group",
                "ACCESS DENIED"
            )
            await quick_reply(update, error_msg)
            return
        
        user_name = user.first_name or "Friend"
        welcome_msg = f"""
{GUI.create_header("Welcome")}

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃        🎉 WELCOME {user_name.upper():<10}       ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃ 📋 Available Commands:               ┃
┃ • /help - Show all commands          ┃
┃ • /myplan - Check subscription       ┃
┃ • /free - Search free logs          ┃
┃ • /paid - Premium logs (1 coin)     ┃
┃ • /coin - Check coin balance        ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

📊 Data Source: {data_source}
📢 Support: @systemadminbd_bot
"""
        
        await quick_reply(update, welcome_msg)
        
    except Exception as e:
        print(f"❌ Error in start command: {e}")

async def source_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ডেটা সোর্স দেখাবে"""
    try:
        user = update.message.from_user
        
        data_source = "🌐 API" if USE_API else "📁 LOCAL FILES"
        api_status = "✅ ACTIVE" if USE_API else "❌ INACTIVE"
        
        source_msg = GUI.create_box(
            f"🔧 DATA SOURCE INFORMATION\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 Primary Source: {data_source}\n"
            f"🚀 API Status: {api_status}\n"
            f"🔗 API URL: {API_URL if USE_API else 'Not Used'}\n"
            f"🔑 API Key: {'Connected' if USE_API else 'Not Used'}\n"
            f"📂 Local Folder: {'Not Used' if USE_API else LOGS_FOLDER}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Bot is currently using {data_source}\n"
            f"for all search operations.",
            "DATA SOURCE"
        )
        
        await quick_reply(update, source_msg)
        
    except Exception as e:
        print(f"❌ Error in source command: {e}")

async def myplan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User plan information"""
    try:
        user = update.message.from_user
        
        if not check_rate_limit(user.id, 'command'):
            await quick_reply(update, "⚠️ Too many requests! Please wait...")
            return
        
        username = user.username or user.first_name or str(user.id)
        user_coins = coin_balance.get(username, 0)
        
        plan_msg = f"""
{GUI.create_header("User Profile")}

{GUI.create_api_status_box()}

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃           👤 USER PROFILE           ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃ 🆔 ID: {user.id:<30} ┃
┃ 👤 Username: {username[:23]:<23} ┃
┃ 📅 Date: {datetime.now().strftime('%d %B'):<26} ┃
┃ 🕐 Time: {datetime.now().strftime('%I:%M %p'):<28} ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃          💰 COIN BALANCE            ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃ ⭐ Coins: {user_coins:<30} ┃
┃ 🎯 Status: {'✅ Active' if user_coins > 0 else '❌ No Coins':<28} ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

📊 SUBSCRIPTION PLAN:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔹 FREE: 10 logs per search
🔹 PREMIUM: 1000 logs per coin
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💎 Buy Premium: Contact @systemadminbd_bot

🌟 Thank you for using our service! 🚀
"""
        
        await quick_reply(update, plan_msg)
        
    except Exception as e:
        print(f"❌ Error in myplan command: {e}")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        
        if not check_rate_limit(user.id, 'command'):
            await quick_reply(update, "⚠️ Too many requests! Please wait...")
            return
        
        help_msg = GUI.create_box(
            "📋 AVAILABLE COMMANDS:\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🎯 /start - Start/restart bot\n"
            "📊 /myplan - Subscription & points\n"
            "🔍 /free <keyword> - Free logs (10)\n"
            "💎 /paid <keyword> - Premium logs (1000)\n"
            "💰 /coin - Check coin balance\n"
            "🌐 /source - Show data source info\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🛠️ ADMIN COMMANDS:\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🎁 /give_coin <user> <amount>\n"
            "📦 /bulk_coin <file> <amount>\n"
            "✅ /verify_group\n"
            "📋 /list_groups\n"
            "❌ /remove_group <group_id>\n"
            "📊 /stats\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "📢 Support: @systemadminbd_bot",
            "HELP MENU"
        )
        
        await quick_reply(update, help_msg)
        
    except Exception as e:
        print(f"❌ Error in help command: {e}")

async def free_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        chat = update.message.chat
        
        if not check_rate_limit(user.id, 'free'):
            await quick_reply(update, "⚠️ Too many searches! Wait 1 minute.")
            return
        
        if not context.args:
            error_box = GUI.create_box(
                "❌ Provide search keyword\n"
                "Example: /free google.com\n"
                "Example: /free admin login\n"
                "Example: /free username:password",
                "ERROR"
            )
            await quick_reply(update, error_box)
            return
        
        if chat.type in ['group', 'supergroup'] and chat.id not in ALLOWED_GROUPS and user.id != BOT_OWNER_ID and user.id not in ADMIN_IDS:
            error_msg = GUI.create_box(
                f"❌ This group is not verified!",
                "ACCESS DENIED"
            )
            await quick_reply(update, error_msg)
            return
        
        target_text = ' '.join(context.args)
        user_name = user.username or user.first_name or str(user.id)
        
        # দ্রুত রিপ্লাই
        status_msg = GUI.create_status_box("SEARCHING FROM API...", "🔍")
        await quick_reply(update, status_msg)
        
        # হাইব্রিড সার্চ (API থেকে ডেটা নিবে)
        logs = await hybrid_search(target_text, max_results=10)
        
        if not logs:
            # সাজেশন সহ এরর মেসেজ
            suggestions = []
            if '.' in target_text:
                suggestions.append(f"• Try without domain: {target_text.split('.')[0]}")
            if ':' in target_text:
                suggestions.append(f"• Try specific part: {target_text.split(':')[0]}")
            
            suggestions_text = "\n".join(suggestions) if suggestions else "• Try different keywords"
            
            no_result_msg = GUI.create_box(
                f"❌ No logs found for: {target_text}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💡 Suggestions:\n"
                f"{suggestions_text}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📡 Data Source: {'🌐 API' if USE_API else '📁 LOCAL FILES'}",
                "NO RESULTS"
            )
            await quick_reply(update, no_result_msg)
            return
        
        # ফ্রি ইউজারের জন্য ১০টি রেজাল্ট
        free_logs = logs[:10]
        
        current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', target_text[:30])
        file_name = f"{clean_name}_free_{current_time}.txt"
        
        try:
            with open(file_name, "w", encoding="utf-8") as f:
                f.write("\n".join(free_logs))
            
            success_msg = GUI.create_box(
                f"✅ SEARCH COMPLETE!\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔍 Keyword: {target_text}\n"
                f"👤 User: {user_name}\n"
                f"📊 Results: {len(free_logs)}/10\n"
                f"📡 Source: {'🌐 API' if USE_API else '📁 LOCAL FILES'}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💎 For 1000+ results use /paid",
                "SEARCH RESULTS"
            )
            
            await quick_reply(update, success_msg)
            
            with open(file_name, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    caption=f"LOGS FINDER BY SYSTEMADMINBD\nSUPPORT @systemadminbd_bot\n\n📁 File: {clean_name}_free_logs.txt\n🔍 Results: {len(free_logs)}/10\n👤 User: {user_name}\n🎯 Keywords: {target_text}\n📡 Source: {'API' if USE_API else 'Local Files'}"
                )
            
            os.remove(file_name)
            
        except Exception as e:
            print(f"❌ File error: {e}")
            error_box = GUI.create_box(
                f"❌ File processing error\n"
                f"But found {len(free_logs)} results:\n"
                f"\n".join(free_logs[:3]),
                "RESULTS"
            )
            await quick_reply(update, error_box)
        
    except Exception as e:
        print(f"❌ Error in free_command: {e}")
        error_box = GUI.create_box(
            f"❌ Search failed\nError: {str(e)[:100]}",
            "ERROR"
        )
        await quick_reply(update, error_box)

async def paid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        chat = update.message.chat
        
        if not check_rate_limit(user.id, 'paid'):
            await quick_reply(update, "⚠️ Too many searches! Wait 1 minute.")
            return
        
        if not context.args:
            error_box = GUI.create_box(
                "❌ Provide search keyword\nExample: /paid google.com",
                "ERROR"
            )
            await quick_reply(update, error_box)
            return
        
        if chat.type in ['group', 'supergroup'] and chat.id not in ALLOWED_GROUPS and user.id != BOT_OWNER_ID and user.id not in ADMIN_IDS:
            error_msg = GUI.create_box(
                f"❌ This group is not verified!",
                "ACCESS DENIED"
            )
            await quick_reply(update, error_msg)
            return
        
        user_name = user.username or user.first_name or str(user.id)
        target_text = ' '.join(context.args)
        
        if user_name not in coin_balance or coin_balance[user_name] <= 0:
            no_coins_msg = GUI.create_box(
                f"❌ INSUFFICIENT COINS!\n"
                f"Your Coins: {coin_balance.get(user_name, 0)}\n"
                f"Required: 1 coin\n"
                f"Contact admin for coins",
                "NO COINS"
            )
            await quick_reply(update, no_coins_msg)
            return
        
        # Deduct coin first
        coin_balance[user_name] -= 1
        save_coins_to_file()
        
        status_msg = GUI.create_status_box("PREMIUM SEARCH IN PROGRESS...", "💎")
        await quick_reply(update, status_msg)
        
        # Comprehensive search from API
        logs = await hybrid_search(target_text, max_results=1000)
        
        if not logs:
            # Refund coin
            coin_balance[user_name] += 1
            save_coins_to_file()
            
            no_result_msg = GUI.create_box(
                f"❌ No logs found for: {target_text}\n"
                f"💰 Refunded: 1 coin\n"
                f"📡 Source: {'🌐 API' if USE_API else '📁 LOCAL FILES'}",
                "NO RESULTS"
            )
            await quick_reply(update, no_result_msg)
            return
        
        premium_logs = logs[:1000]
        current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', target_text[:30])
        file_name = f"{clean_name}_premium_{current_time}.txt"
        
        try:
            with open(file_name, "w", encoding="utf-8") as f:
                f.write("\n".join(premium_logs))
            
            success_msg = GUI.create_box(
                f"✅ PREMIUM SEARCH COMPLETE!\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔍 Keyword: {target_text}\n"
                f"👤 User: {user_name}\n"
                f"📊 Results: {len(premium_logs)}/1000\n"
                f"💰 Remaining: {coin_balance[user_name]} coins\n"
                f"📡 Source: {'🌐 API' if USE_API else '📁 LOCAL FILES'}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🌟 Thank you for using premium service!",
                "PREMIUM RESULTS"
            )
            
            await quick_reply(update, success_msg)
            
            with open(file_name, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    caption=f"LOGS FINDER BY SYSTEMADMINBD\nSUPPORT @systemadminbd_bot\n\n📁 File: {clean_name}_premium_logs.txt\n🔍 Results: {len(premium_logs)}/1000\n👤 User: {user_name}\n💰 Coins Left: {coin_balance[user_name]}\n🎯 Keywords: {target_text}\n📡 Source: {'API' if USE_API else 'Local Files'}"
                )
            
            os.remove(file_name)
            
        except Exception as e:
            print(f"❌ File error: {e}")
            # Refund coin
            coin_balance[user_name] += 1
            save_coins_to_file()
            
            error_box = GUI.create_box(
                f"❌ File processing error\n"
                f"💰 Refunded: 1 coin\n"
                f"But found {len(premium_logs)} results",
                "ERROR"
            )
            await quick_reply(update, error_box)
        
    except Exception as e:
        print(f"❌ Error in paid_command: {e}")
        error_box = GUI.create_box(
            f"❌ Search failed",
            "ERROR"
        )
        await quick_reply(update, error_box)

# ==================== ADMIN COMMANDS ====================
async def give_coin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        
        if user.id != BOT_OWNER_ID and user.id not in ADMIN_IDS:
            await quick_reply(update, "❌ Admin only!")
            return
        
        if len(context.args) != 2:
            help_msg = "Usage: /give_coin {username} {amount}"
            await quick_reply(update, help_msg)
            return
        
        target_user = context.args[0].replace("@", "").strip()
        try:
            coin_no = int(context.args[1])
            if coin_no <= 0:
                await quick_reply(update, "❌ Positive amount only!")
                return
            
            if target_user:
                coin_balance[target_user] = coin_balance.get(target_user, 0) + coin_no
                save_coins_to_file()
                
                success_msg = GUI.create_box(
                    f"✅ Given {coin_no} coins to {target_user}\nTotal: {coin_balance[target_user]}",
                    "SUCCESS"
                )
                await quick_reply(update, success_msg)
        except ValueError:
            await quick_reply(update, "❌ Invalid amount!")
    
    except Exception as e:
        print(f"❌ Error in give_coin: {e}")

async def bulk_coin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        
        if user.id != BOT_OWNER_ID and user.id not in ADMIN_IDS:
            await quick_reply(update, "❌ Admin only!")
            return
        
        if len(context.args) != 2:
            help_msg = "Usage: /bulk_coin {filename} {amount}"
            await quick_reply(update, help_msg)
            return
        
        filename = context.args[0]
        try:
            coin_amount = int(context.args[1])
            if coin_amount <= 0:
                await quick_reply(update, "❌ Positive amount only!")
                return
        except ValueError:
            await quick_reply(update, "❌ Invalid amount!")
            return
        
        if not os.path.exists(filename):
            await quick_reply(update, f"❌ File not found: {filename}")
            return
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                users = [line.strip() for line in f if line.strip()]
            
            if len(users) > 100:
                users = users[:100]
                await quick_reply(update, f"⚠️ Limited to first 100 users")
            
            updated_users = []
            for username in users:
                username = username.replace("@", "").strip()
                if username:
                    current_balance = coin_balance.get(username, 0)
                    coin_balance[username] = current_balance + coin_amount
                    updated_users.append(username)
            
            save_coins_to_file()
            
            success_msg = GUI.create_box(
                f"✅ Bulk coins distributed!\n"
                f"Users: {len(updated_users)}\n"
                f"Amount: {coin_amount} each\n"
                f"Total: {len(updated_users) * coin_amount} coins",
                "SUCCESS"
            )
            await quick_reply(update, success_msg)
            
        except Exception as e:
            await quick_reply(update, f"❌ Error: {str(e)[:50]}")
    
    except Exception as e:
        print(f"❌ Error in bulk_coin: {e}")

async def verify_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        
        if user.id != BOT_OWNER_ID and user.id not in ADMIN_IDS:
            await quick_reply(update, "❌ Admin only!")
            return
        
        chat = update.message.chat
        if chat.type in ['group', 'supergroup']:
            group_id = chat.id
            group_name = chat.title or "Unknown"
            
            if group_id in ALLOWED_GROUPS:
                msg = f"✅ Group already verified: {group_name}"
            else:
                ALLOWED_GROUPS.add(group_id)
                save_groups_to_file()
                msg = f"✅ Group verified: {group_name}"
            
            await quick_reply(update, msg)
        else:
            await quick_reply(update, "❌ Use in group only!")
    
    except Exception as e:
        print(f"❌ Error in verify_group: {e}")

async def list_groups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        
        if user.id != BOT_OWNER_ID and user.id not in ADMIN_IDS:
            await quick_reply(update, "❌ Admin only!")
            return
        
        if not ALLOWED_GROUPS:
            await quick_reply(update, "📭 No groups verified")
            return
        
        groups_list = "\n".join([f"• {gid}" for gid in sorted(ALLOWED_GROUPS)[:20]])
        msg = f"📋 Verified Groups ({len(ALLOWED_GROUPS)}):\n{groups_list}"
        
        if len(ALLOWED_GROUPS) > 20:
            msg += f"\n... and {len(ALLOWED_GROUPS) - 20} more"
        
        await quick_reply(update, msg)
    
    except Exception as e:
        print(f"❌ Error in list_groups: {e}")

async def remove_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        
        if user.id != BOT_OWNER_ID and user.id not in ADMIN_IDS:
            await quick_reply(update, "❌ Admin only!")
            return
        
        if not context.args:
            await quick_reply(update, "Usage: /remove_group {group_id}")
            return
        
        try:
            group_id = int(context.args[0])
            if group_id in ALLOWED_GROUPS:
                ALLOWED_GROUPS.remove(group_id)
                save_groups_to_file()
                await quick_reply(update, f"✅ Group {group_id} removed")
            else:
                await quick_reply(update, f"❌ Group {group_id} not found")
        except ValueError:
            await quick_reply(update, "❌ Invalid group ID")
    
    except Exception as e:
        print(f"❌ Error in remove_group: {e}")

async def coin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        username = user.username or user.first_name or str(user.id)
        
        user_coins = coin_balance.get(username, 0)
        
        coin_msg = GUI.create_box(
            f"💰 COIN BALANCE\n"
            f"👤 User: {username}\n"
            f"🎯 Coins: {user_coins}\n"
            f"📊 Status: {'✅ Active' if user_coins > 0 else '❌ No Coins'}",
            "COIN BALANCE"
        )
        
        await quick_reply(update, coin_msg)
    
    except Exception as e:
        print(f"❌ Error in coin command: {e}")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        
        if user.id != BOT_OWNER_ID and user.id not in ADMIN_IDS:
            await quick_reply(update, "❌ Admin only!")
            return
        
        total_coins = sum(coin_balance.values())
        active_users = sum(1 for coins in coin_balance.values() if coins > 0)
        data_source = "API" if USE_API else "Local Files"
        
        stats_msg = GUI.create_box(
            f"📊 BOT STATISTICS\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Total Users: {len(coin_balance)}\n"
            f"✅ Active Users: {active_users}\n"
            f"💰 Total Coins: {total_coins}\n"
            f"👥 Verified Groups: {len(ALLOWED_GROUPS)}\n"
            f"📡 Data Source: {data_source}\n"
            f"📅 Date: {datetime.now().strftime('%d %B, %Y')}",
            "STATISTICS"
        )
        
        await quick_reply(update, stats_msg)
    
    except Exception as e:
        print(f"❌ Error in stats: {e}")

# ==================== BACKGROUND TASKS ====================
async def auto_save_task(context: CallbackContext):
    try:
        save_coins_to_file()
        save_groups_to_file()
        save_backup()
        print(f"🔄 Auto-save: {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"❌ Auto-save error: {e}")

async def cleanup_task(context: CallbackContext):
    try:
        now = time.time()
        for key in list(user_requests.keys()):
            user_requests[key] = [req for req in user_requests[key] 
                                 if now - req < 300]
            if not user_requests[key]:
                del user_requests[key]
        
        # ক্যাশে ক্লিনআপ
        for log_file in list(processed_files_cache.keys()):
            if len(processed_files_cache[log_file]) > 10000:
                processed_files_cache[log_file] = set()
        
    except Exception as e:
        print(f"❌ Cleanup error: {e}")

# ==================== MAIN FUNCTION ====================
def main():
    try:
        setup_signal_handlers()
        initialize_data()
        
        print(GUI.create_header("Bot Starting"))
        print(GUI.create_status_box("INITIALIZING", "🚀"))
        
        # APP CREATION
        app = Application.builder().token(BOT_TOKEN).build()
        
        # Command handlers
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("myplan", myplan_command))
        app.add_handler(CommandHandler("free", free_command))
        app.add_handler(CommandHandler("paid", paid_command))
        app.add_handler(CommandHandler("give_coin", give_coin_command))
        app.add_handler(CommandHandler("bulk_coin", bulk_coin_command))
        app.add_handler(CommandHandler("verify_group", verify_group_command))
        app.add_handler(CommandHandler("list_groups", list_groups_command))
        app.add_handler(CommandHandler("remove_group", remove_group_command))
        app.add_handler(CommandHandler("coin", coin_command))
        app.add_handler(CommandHandler("stats", stats_command))
        app.add_handler(CommandHandler("source", source_command))
        
        # JOB QUEUE
        job_queue = app.job_queue
        if job_queue:
            job_queue.run_repeating(auto_save_task, interval=300, first=10)
            job_queue.run_repeating(cleanup_task, interval=60, first=5)
        
        print(GUI.create_status_box("RUNNING", "✅"))
        print(f"📊 Bot initialized successfully!")
        print(f"📡 Data Source: {'🌐 API' if USE_API else '📁 LOCAL FILES'}")
        
        # বট স্টার্ট
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            poll_interval=0.5,
            timeout=15
        )
        
    except Exception as e:
        print(GUI.create_status_box("CRASHED", "💀"))
        print(f"❌ Bot crashed: {e}")
        import traceback
        traceback.print_exc()
        
        # Emergency save
        try:
            save_coins_to_file()
            save_groups_to_file()
            save_backup()
        except Exception as save_error:
            print(f"❌ Emergency save failed: {save_error}")
        
        print("🔄 Restarting in 10 seconds...")
        time.sleep(10)
        os.execv(sys.executable, ['python'] + sys.argv)

if __name__ == '__main__':
    main()