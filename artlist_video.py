#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/3/17 9:54
# @Author  : CUI liuliu
# @File    : artlist_video01.py
import os
import time
import requests
import mysql.connector
from mysql.connector import Error
import shutil
import subprocess
import logging
from tqdm import tqdm
from datetime import datetime
import random
import uuid
from concurrent.futures import ThreadPoolExecutor
import argparse

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("artlist_download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'user': 'root',
    'password': 'zq828079',
    'host': '192.168.10.70',
    'database': 'yunjing',
    'raise_on_warnings': True
}

# 文件存储路径配置
SAVE_DIR = r"E:\4.2\artlist\{KEYWORDS}"
os.makedirs(SAVE_DIR, exist_ok=True)

# 自定义ID前缀
DATAL_ID_PREFIX = "90"
page_start = 1

# 添加关键词组配置
KEYWORDS = [
    # "Tilt",
    # "Zoom",
    # "Dolly",
    # "Tracking",
    # "Crane",
    # "Jib",
    # "Boom",
    # "Steadicam",
    # "Drone",
    # "Whip Pan",
    # "Dutch Angle",
    # "Time-Lapse",
    # "Slow Motion",
    # "Bullet Time",
    # "Overhead",
    # "POV",
    # "Reverse Tracking",
    # "360 Spin",
    # "Push In/Pull Out",
    # "Handheld",
    # "Lock-off",
    # "Match Cut",
    # "Crash Zoom",
    # "Pedestal Move",
    # "Rotation Shot",
    # "Whip Pan",
    # "Follow Focus",
    # "Focus Pull",
    # "Dolly Zoom",
    # "Arc Shot",
    # "Helicopter Spin",
    # "Swoop Zoom",
    # "Undulating Motion",
    # "Serpentine Tracking",
    # "Pendulum Swing",
    # "Combination Move"
    # "Low-Angle Tilt",
    # "High-Angle Tilt",
    # "Dutch Angle",
    # "POV",
    # "Insect's Eye View",
    # "Satellite Perspective",
    # "Steadicam",
    # "Gimbal",
    # "Rail Dolly",
    # "Jib Arm",
    # "Boom Pole",
    # "Drone",
    # "Underwater Scooter",
    # "Hyperlapse",
    # "Freeze Frame",
    # "Variable Speed",
    # "Jump Cut",
    # "Match Cut",
    # "Seamless Transition",
    # "Forward Tracking",
    # "Backward Tracking",
    # "Circular Tracking",
    # "360° Pan",
    # "Vertical Tilt",
    # "Horizontal Pan",
    # "Tilting Rotation",
    # "Bullet Time",
    # "Fast Motion",
    # "Multiple Exposure",
    # "Split Screen",
    # "PIP",
    # "Follow Spot",
    # "Chasing Light",
    # "Light Shifting",
    # "Dynamic Shadow",
    # "Color Temperature Shift",
    # "Multi-Camera Sync",
    # "Master-Slave Rig",
    # "Robotic Arm",
    # "Drone Swarm",
    # "Underwater Housing",
    # "Subjective Shot",
    # "Objective Shot",
    # "Metaphorical Shot",
    # "Symbolic Shot",
    # "Contrast Shot",
    # "Parallel Editing",
    # "Car Mount",
    # "Cycle Mount",
    # "Boat Rig",
    # "Rail Mount",
    # "Ski Cam",
    # "Climbing Rig",
    # "8K Panorama",
    # "3D Audio",
    # "VR Immersion",
    # "AR Enhancement",
    # "Mocap",
    # "Virtual Production",
    # "Rhythmic Editing",
    # "Emotional Editing",
    # "Informative Editing",
    # "Suspense Editing",
    # "Comedic Timing",
    # "Explosion Capture",
    # "Smoke Tracking",
    # "Water Effect",
    # "Fire Capture",
    # "Particle Tracking",
    # "Micro Camera",
    # "Covert Camera",
    # "Wearable Camera",
    # "Extendable Lens",
    # "Fisheye Lens",
    # "establishing shot",
    # "medium shot",
    # "close-up",
    # "extreme close-up",
    # "two-shot",
    # "group shot",
    # "Flashback",
    # "Flashforward",
    # "Reverse Chronology",
    # "Prolepsis",
    # "Interpolation",
    # "Surround Panning",
    # "Dynamic Reverb",
    # "Ambience Matching",
    # "Foley Art",
    # "Sound Montage",
    # "Color Gradient",
    # "Warm-Cool Contrast",
    # "Monochromatic Accent",
    # "Temperature Jump",
    # "Highlight Bloom",
    # "Handheld Shake",
    # "Random Motion",
    # "Anti-Gravity Shot",
    # "Mirror Reflection",
    # "Refraction Shot",
    # "Anamorphic Lens",
    # "Multi-Focus Capture"
    # FPV/无人机视角
    # "FPV shot", "Drone shot", "Aerial shot",
    
    # #  缩放
    # "Zoom in", "Zoom out", "Dolly zoom", "Push in zoom", "Pull out zoom",

    # # 鸟瞰
    # "Bird's eye view", "Aerial view", "Top down shot", "Overhead shot",
    
    # # 延时摄影
    # "Time lapse", "Timelapse photography", "Motion timelapse",
    
    # #  移轴
    # "Tilt shift", "Miniature effect", "Selective focus",
    
    # # 地面镜头
    # "Ground level shot", "Low angle shot", "Worm's eye view",
    
    # # 倾斜（上下左右）
    # "Tilt up", "Tilt down", "Dutch angle", "Canted angle",
    # "Tilt left", "Tilt right", "Angular shot",
    
    # # 360度
    # "360 degree shot", "360 pan", "Full rotation shot",
    # "Orbital shot", "Spherical panorama",
    
    # # 环绕（上下左右）
    # "Circular tracking", "Orbit shot", "Wraparound shot",
    # "Circle around", "360 tracking shot",
    
    # # 拉远/推进
    # "Pull back shot", "Push in shot", "Dolly in",
    # "Dolly out", "Track in", "Track out",
    # "Forward tracking", "Backward tracking"
    #  "Static shot",  # 固定镜头
    # "Moving shot",  # 运动镜头
    # "Horizontal movement",  # 水平移动
    "Vertical movement",  # 垂直移动
    "Circular movement",  # 环形移动
    "Slow movement",  # 慢速移动
    "Fast movement",  # 快速移动
    "Smooth movement",  # 平滑移动
    "Jittery movement",  # 抖动移动
    "Pan shot",  # 摇摄
    "Tilt shot",  # 俯仰拍摄
    "Zoom shot",  # 变焦拍摄
    "Dolly shot",  # 推轨拍摄
    "Tracking shot",  # 跟拍
    "Crane shot",  # 升降拍摄
    "Aerial shot",  # 航拍
    "Handheld shot",  # 手持拍摄
    "Steadicam shot",  # 稳定器拍摄
    "Time - lapse shot",  # 延时拍摄
    "Slow - motion shot",  # 慢动作拍摄
    "360 - degree shot",  # 360度拍摄
    "POV shot",  # 第一人称视角拍摄
    "Reverse shot",  # 反拍
    "Over - the - shoulder shot",  # 过肩拍摄
    "Establishing shot",  # 全景镜头
    "Close - up shot",  # 特写镜头
    "Medium shot",  # 中景镜头
    "Long shot",
]

# 添加随机User-Agent池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 OPR/120.0.0.0"
]

# 添加请求重试装饰器
def retry_on_failure(max_retries=3, delay=5):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"最终尝试失败: {str(e)}")
                        raise
                    logger.warning(f"第{attempt + 1}次尝试失败，等待{delay}秒后重试: {str(e)}")
                    time.sleep(delay + random.uniform(0, 2))  # 添加随机延迟
            return None
        return wrapper
    return decorator

def get_random_headers():
    """生成随机请求头"""
    return {
        "Content-Type": "application/json",
        "Origin": "https://artlist.io",
        "Referer": "https://artlist.io/",
        "User-Agent": random.choice(USER_AGENTS),
        "X-Anonymous-Id": str(uuid.uuid4()),  # 随机生成
        "X-User-Status": "guest",
        "X-Visitor-Id": str(uuid.uuid4()),  # 随机生成
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Sec-Ch-Ua": '"Chromium";v="134", "Not:A-Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "cookie":"_hjSessionUser_3149206=eyJpZCI6ImIxZWIyM2Q2LTNlZjItNTc0ZC1iOGUyLTdmMTA0OTk5MjM0MiIsImNyZWF0ZWQiOjE3NDIyMDY0Nzk3NzQsImV4aXN0aW5nIjpmYWxzZX0=; bot-score=41; verified-bot=false; user_ip=2406%3Ada14%3A18d8%3A3600%3A23db%3A114f%3Ae3ca%3A80e; countryCode-from-headers=JP; AL__anonymous_id=385fb12d-d69d-4fda-bacc-6fda53b71b0b; _fs_sample_user=false; _gcl_au=1.1.1637840059.1742206485; _ga=GA1.1.380376371.1742206479; PAPVisitorId=xZhItZIeXVONZo5ZQAqQWrf1Eho1VaQZ; FPAU=1.1.1637840059.1742206485; _gtmeec=e30%3D; _fbp=fb.1.1742206484917.1358813907; ajs_anonymous_id=6a33054c-5898-4217-aadb-ef8fc2c13db3; _hjSessionUser_458951=eyJpZCI6IjQ0OTcyZDc1LTFjZmMtNWQ1OS1hZTJjLWVhZDAyYWE5NDAyMyIsImNyZWF0ZWQiOjE3NDIyMDY0ODQ1OTUsImV4aXN0aW5nIjp0cnVlfQ==; _clck=9uprlj%7C2%7Cfub%7C0%7C1902; __cf_bm=_Wpsvhu.nNULo52p2t5W.jzJRve.LoMu0zdb3QFNqoQ-1742379445-1.0.1.1-N9orRZqCZ_XZgG1iIEz3bEij1wiR4MkmV5iAua7zNag84Nf_4sh2KK7tdlfki5irqPBr4agEwSh7aozzF4x6SpPfaBVQBWKy7sp8qUmGuzs; cf_clearance=REIyNpwup_AzCMZQb_jqwWOEsBoVmLwFR.pkNo0BFvk-1742379446-1.2.1.1-AEG3iHzNbB8whONyzFdhweBGN0ugAmLk1zHVsi3qihrAYlsVtaRc9_XJfwSf3RlFJ_.4OZxCnBtxHvop10XxEwAUkdOUVT7qKVX57vJn6D5R76empNgE9EUVDaSG4NPNeJ7vZw1I2LJDLC1t4Zrn0EIsYf1Nyo5QviUA3nk5yCvLzd7eW2tZVgaxzvkRbDMe8t_oSGK4uWlMahtVK8eeCoE7YVp41GXo9fGL_ZFVBMbHzVIG1xaQFSDuOK5Zv3bMhVy.UsXxUFUTQ9J21r7tL7cP0mM0oUqANU9RLlCiAzitHXPN8LdVv4.szOJ7Yt1_UHOgwt.QBsZ3kACAs8bdyeCc1twU7htGvRFG83lFi9U; userSession=d2e54e21-c583-44a8-b59f-272f0b04772f; _uetsid=5bb05a8004ab11f096df8550b1c1076e; _uetvid=a5dad3c0031811f09af6538646ae40d0; _hjSession_458951=eyJpZCI6ImFmMzQxMzYyLTZlZWUtNDE0My04NjQyLWRiN2U4YjcxZmNiYyIsImMiOjE3NDIzNzk0NDc3NzcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; _ga_FWF31VG0W2=GS1.1.1742379447.5.1.1742379447.0.0.1451781860; _ga_65CXCH03KJ=GS1.1.1742379447.5.0.1742379447.60.0.0; ab.storage.deviceId.a8859136-009c-49a9-9457-9554c055e10d=%7B%22g%22%3A%22574d9ee8-48f1-5056-2be0-4268d1d9ed65%22%2C%22c%22%3A1742206485426%2C%22l%22%3A1742379448122%7D; ab.storage.sessionId.a8859136-009c-49a9-9457-9554c055e10d=%7B%22g%22%3A%22b0d8270a-e020-de65-681f-fe71a3db8657%22%2C%22e%22%3A1742381248481%2C%22c%22%3A1742379448122%2C%22l%22%3A1742379448481%7D"
    }

def create_table():
    """创建数据表"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS artlist_video (
            id VARCHAR(255) PRIMARY KEY,
            clip_name VARCHAR(255),
            original_url VARCHAR(512) UNIQUE,
            insert_time DATETIME DEFAULT CURRENT_TIMESTAMP,
            download_state BOOLEAN DEFAULT FALSE,
            save_path VARCHAR(512),
            datal_id VARCHAR(255)
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("数据表创建/验证成功")
    except Error as e:
        # 捕获特定的数据库错误
        if e.errno == 1050:  # 表已存在错误码
            logger.warning("数据表已存在，无需创建")
        else:
            logger.error(f"数据库错误: {e}")
    except Exception as e:
        logger.error(f"其他错误: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def check_download_state(video_id):
    """检查是否已下载"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        check_query = """
        SELECT download_state FROM artlist_video 
        WHERE id = %s
        """
        cursor.execute(check_query, (video_id,))
        result = cursor.fetchone()

        return result[0] if result else False

    except Error as e:
        logger.error(f"状态检查失败: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def insert_video_record(video_data):
    """插入视频记录"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
               INSERT INTO artlist_video 
                   (id, clip_name, original_url)
               VALUES
                   (%s, %s, %s)
               AS alias
               ON DUPLICATE KEY UPDATE
                   clip_name = alias.clip_name,
                   original_url = alias.original_url
               """
        cursor.execute(insert_query, (
            video_data['id'],
            video_data['clipName'],
            video_data['clipPath']
        ))
        conn.commit()
        logger.info(f"已插入记录: {video_data['id']}")
        return cursor.lastrowid

    except Error as e:
        logger.error(f"数据库插入错误: {e}")
        return None
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def update_video_record(video_id, save_path):
    """更新下载状态"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 生成自定义ID：前缀 + 时间戳 + 随机后缀
        custom_id = f"{DATAL_ID_PREFIX}"

        update_query = """
        UPDATE artlist_video 
        SET download_state = TRUE,
            save_path = %s,
            datal_id = %s
        WHERE id = %s
        """
        cursor.execute(update_query, (save_path, custom_id, video_id))
        conn.commit()
        logger.info(f"已更新记录: {video_id}")

    except Error as e:
        logger.error(f"数据库更新错误: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def process_m3u8(video_item):
    """处理m3u8下载和转换"""
    try:
        video_id = video_item['id']
        logger.info(f"开始处理视频: {video_id}")

        # 下载原始m3u8文件
        original_url = video_item['clipPath']
        logger.debug(f"下载原始m3u8: {original_url}")

        response = requests.get(original_url)
        response.raise_for_status()

        # 解析最高画质片段
        lines = response.text.split('\n')
        last_stream = None
        for i, line in enumerate(lines):
            if "#EXT-X-STREAM-INF" in line:
                last_stream = lines[i + 1].strip()

        if not last_stream:
            raise ValueError("未找到有效视频流")

        # 构建新URL
        base_url = '/'.join(original_url.split('/')[:-1])
        new_url = f"{base_url}/{last_stream}"
        logger.debug(f"解析到高清流地址: {new_url}")

        # 下载实际m3u8
        m3u8_path = os.path.join(SAVE_DIR, f"{video_id}.m3u8")
        response = requests.get(new_url)
        response.raise_for_status()
        with open(m3u8_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"已下载m3u8文件: {m3u8_path}")

        # 解析m3u8文件，获取所有ts片段
        ts_files = []
        with open(m3u8_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if line.strip().startswith('#EXTINF'):
                    # 下一行是ts文件
                    if i + 1 < len(lines):
                        ts_file = lines[i + 1].strip()
                        if ts_file:
                            ts_files.append(ts_file)

        if not ts_files:
            raise ValueError("未找到有效的ts片段")

        # 创建视频文件夹
        video_save_dir = os.path.join(SAVE_DIR, f"{video_id}")
        os.makedirs(video_save_dir, exist_ok=True)

        # 添加并发下载ts片段
        def download_ts_segment(ts_info):
            ts_url, ts_path = ts_info
            for _ in range(3):  # 重试机制
                try:
                    response = requests.get(ts_url, headers=get_random_headers(), stream=True)
                    response.raise_for_status()
                    with open(ts_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    return True
                except Exception as e:
                    logger.warning(f"下载ts片段失败，重试: {ts_url}, 错误: {e}")
                    time.sleep(random.uniform(1, 3))
            return False

        # 并发下载ts片段
        ts_download_tasks = [(f"{base_url}/{ts_file}", os.path.join(video_save_dir, ts_file)) 
                           for ts_file in ts_files]
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(tqdm(
                executor.map(download_ts_segment, ts_download_tasks),
                total=len(ts_download_tasks),
                desc="下载视频片段"
            ))

        if not all(results):
            raise Exception("部分视频片段下载失败")

        # 生成输入列表文件
        input_list_path = os.path.join(video_save_dir, 'input_list.txt')
        with open(input_list_path, 'w', encoding='utf-8') as f:
            for ts_file in ts_files:
                f.write(f"file '{os.path.join(video_save_dir, ts_file)}'\n")

        logger.info(f"生成输入列表文件: {input_list_path}")

        # 转换MP4
        output_path = os.path.join(SAVE_DIR, f"{video_id}.mp4")
        logger.info(f"开始视频转换: {output_path}")
        ffmpeg_path = r"D:\ffmpeg-7.0.2-essentials_build\bin\ffmpeg.exe"
        ffmpeg_cmd = [
            ffmpeg_path,
            '-y',
            '-loglevel', 'debug',
            '-protocol_whitelist', 'file,pipe,concat',
            '-f', 'concat',
            '-safe', '0',
            '-i', input_list_path,
            '-c', 'copy',
            output_path
        ]

        process = subprocess.run(
            ffmpeg_cmd,
            check=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True
        )

        logger.info(f"视频转换完成: {output_path}")

        # 删除处理过程中生成的中间文件和文件夹
        logger.info("开始清理临时文件...")
        # 删除m3u8文件
        if os.path.exists(m3u8_path):
            os.remove(m3u8_path)
            logger.debug(f"已删除m3u8文件: {m3u8_path}")
        # 删除输入列表文件
        if os.path.exists(input_list_path):
            os.remove(input_list_path)
            logger.debug(f"已删除输入列表文件: {input_list_path}")
        # 删除视频文件夹及其内容
        if os.path.exists(video_save_dir):
            shutil.rmtree(video_save_dir)
            logger.debug(f"已删除视频文件夹: {video_save_dir}")
        logger.info("临时文件清理完成.")

        return output_path

    except Exception as e:
        logger.error(f"视频处理失败: {str(e)}")
        return None


def process_video(video_item):
    """处理单个视频"""
    video_id = video_item['id']

    # 状态检查
    if check_download_state(video_id):
        logger.info(f"视频已下载，跳过处理: {video_id}")
        return
    # 插入数据库记录
    insert_video_record(video_item)

    # 处理视频下载
    start_time = time.time()
    output_path = process_m3u8(video_item)
    if not output_path:
        return

    # 更新数据库
    update_video_record(video_id, output_path)

    duration = time.time() - start_time
    logger.info(f"视频处理完成: {video_id} 耗时: {duration:.2f}s")

@retry_on_failure(max_retries=3, delay=5)
def send_artlist_graphql_request(keyword, page):
    """发送GraphQL请求"""
    url = "https://search-api.artlist.io/v1/graphql"
    
    # 更新的请求头
    headers = {
        "authority": "search-api.artlist.io",
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://artlist.io",
        "referer": "https://artlist.io/stock-footage/search/video",
        "sec-ch-ua": '"Google Chrome";v="122", "Chromium";v="122", "Not(A:Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "x-anonymous-id": str(uuid.uuid4()),
        "x-user-status": "guest",
        "x-visitor-id": str(uuid.uuid4())
    }
    
    # 更新的查询参数
    payload = {
        "operationName": "ClipList",
        "variables": {
            "page": page,
            "queryType": 1,
            "filterCategories": [250],
            "searchTerms": [keyword],
            "sortType": 1,
            "durationMin": 10000,
            "orientation": None,
            "durationMax": None
        },
        "query": """
        query ClipList(
            $filterCategories: [Int!]
            $searchTerms: [String]
            $sortType: Int
            $queryType: Int
            $page: Int
            $durationMin: Int
            $durationMax: Int
            $orientation: ClipOrientation
        ) {
            clipList(
                filterCategories: $filterCategories
                searchTerms: $searchTerms
                sortType: $sortType
                queryType: $queryType
                page: $page
                durationMin: $durationMin
                durationMax: $durationMax
                orientation: $orientation
            ) {
                querySearchType
                exactResults {
                    id
                    clipName
                    clipPath
                    duration
                    width
                    height
                }
                totalExact
                totalSimilar
            }
        }
        """
    }
    
    # 增加重试间隔和随机延迟
    for attempt in range(3):
        try:
            # 使用会话保持连接
            with requests.Session() as session:
                # 设置会话级别的headers
                session.headers.update(headers)
                
                # 模拟真实用户行为
                try:
                    # 访问主页
                    session.get(
                        "https://artlist.io/",
                        timeout=10,
                        headers={"purpose": "prefetch"}
                    )
                    time.sleep(random.uniform(2, 4))
                    
                    # 访问搜索页
                    session.get(
                        "https://artlist.io/stock-footage/search/video",
                        timeout=10,
                        headers={"purpose": "prefetch"}
                    )
                    time.sleep(random.uniform(1, 3))
                    
                    # 发送预检请求
                    session.options(url)
                    time.sleep(random.uniform(0.5, 1))
                    
                except requests.exceptions.RequestException as e:
                    logger.warning(f"预热请求失败: {e}")
                
                # 发送GraphQL请求
                response = session.post(
                    url,
                    json=payload,
                    timeout=15,
                    headers={
                        "content-type": "application/json",
                        "x-request-id": str(uuid.uuid4())
                    }
                )
                
                # 检查响应状态码
                if response.status_code == 403:
                    raise ValueError("访问被拒绝，可能需要更新Cookie或等待更长时间")
                response.raise_for_status()
                
                # 记录响应信息用于调试
                logger.debug(f"Response headers: {dict(response.headers)}")
                logger.debug(f"Response status: {response.status_code}")
                
                # 尝试解析响应
                try:
                    data = response.json()
                    if not data or 'data' not in data:
                        logger.error(f"Invalid response structure: {data}")
                        raise ValueError("Invalid JSON response structure")
                    return data
                except ValueError as e:
                    logger.error(f"JSON解析失败: {str(e)}")
                    logger.debug(f"Response content: {response.text[:200]}")
                    raise
                    
        except Exception as e:
            logger.warning(f"请求失败 (尝试 {attempt + 1}/3): {str(e)}")
            if attempt < 2:
                wait_time = (attempt + 1) * 30 + random.uniform(10, 20)
                logger.info(f"等待 {wait_time:.1f} 秒后重试...")
                time.sleep(wait_time)
            continue
            
    raise Exception("所有重试都失败了")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Artlist视频下载工具')
    parser.add_argument('--start-page', type=int, default=1, help='开始下载的页码')
    parser.add_argument('--end-page', type=int, default=30, help='结束的页码')
    parser.add_argument('--save-dir', type=str, default=SAVE_DIR, help='保存目录')
    return parser.parse_args()

def main_processing():
    args = parse_arguments()
    
    logger.info("=" * 30)
    logger.info("启动Artlist视频下载任务")
    logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"开始页码: {args.start_page}, 结束页码: {args.end_page}")

    create_table()
    
    consecutive_errors = 0  # 添加连续错误计数
    empty_pages = 0  # 添加空页面计数
    
    for keyword in KEYWORDS:
        logger.info(f"开始处理关键词: {keyword}")
        page = args.start_page
        
        while page <= args.end_page:
            try:
                # 如果连续错误次数过多，增加等待时间
                if consecutive_errors >= 3:
                    wait_time = min(300, consecutive_errors * 30)
                    logger.warning(f"检测到多次连续失败，等待 {wait_time} 秒后继续...")
                    time.sleep(wait_time)
                    consecutive_errors = 0
                
                logger.info(f"正在获取第 {page} 页的视频列表...")
                result = send_artlist_graphql_request(keyword, page)
                
                exact_results = result.get('data', {}).get('clipList', {}).get('exactResults', [])
                
                if not exact_results:
                    logger.info(f"第{page}页没有视频: {keyword}")
                    empty_pages += 1
                    # 如果连续5页都是空的，才考虑结束
                    if empty_pages >= 5:
                        logger.info(f"连续{empty_pages}页都没有视频，切换到下一个关键词")
                        break
                    # 否则继续尝试下一页
                    page += 1
                    time.sleep(random.uniform(5, 10))
                    continue
                
                # 找到视频，重置空页面计数
                empty_pages = 0
                logger.info(f"发现 {len(exact_results)} 个待处理视频")
                consecutive_errors = 0  # 重置连续错误计数
                
                # 处理视频...
                with ThreadPoolExecutor(max_workers=2) as executor:  # 降低并发数
                    list(tqdm(
                        executor.map(process_video, exact_results),
                        total=len(exact_results),
                        desc=f"处理视频 - {keyword} - 第{page}页"
                    ))
                
                page += 1
                time.sleep(random.uniform(5, 10))
                
            except Exception as e:
                logger.error(f"处理页面失败: {keyword} - 第{page}页, 错误: {e}")
                consecutive_errors += 1
                time.sleep(random.uniform(10, 20))
                continue
        
        logger.info(f"关键词 {keyword} 处理完成")
        time.sleep(random.uniform(30, 60))

    logger.info(f"所有任务完成")
    logger.info(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 30)


if __name__ == "__main__":
    main_processing()