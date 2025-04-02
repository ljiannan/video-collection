import requests
import logging
import re
import time
from datetime import datetime
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.sql.cons import disk_id, folder_name, retry_times, pexels_table_name
from src.sql.sql_operate import select_data_table, insert_data_table

from loguru import logger


# 比较视频文件是否存在于指定目录
@logger.catch
def compare_video_dir(file_path, input_path):
    file_name = os.path.basename(file_path)  # 提取文件名
    target_file = os.path.join(input_path, file_name)  # 拼接路径
    return os.path.isfile(target_file)  # 验证文件存在性


# 设置日志
log_dir = "./logs"
log_file = os.path.join(log_dir, "pexels_log.log")
max_log_size = 10 * 1024 * 1024  # 10MB
backup_count = 5  # 最多保留5个日志文件

# 创建logs目录，如果不存在
os.makedirs(log_dir, exist_ok=True)
# 创建 RotatingFileHandler 实例
log_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count, encoding='utf-8')
# 设置日志格式
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_format)

# # 获取根日志记录器
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
#
# # 将处理程序添加到根日志记录器
# if not logger.handlers:
#     logger.addHandler(log_handler)

# 添加控制台输出
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_format)


# logger.addHandler(stream_handler)

@logger.catch
def data_sql_check(table_name, video_filename, mp4_link, keywords, is_local, datal_id):
    logger.info(f"table: {table_name}, start operate..")
    if video_filename and mp4_link:
        logger.info("start execute sql operate!!!")
        ret = select_data_table(table_name, video_filename)
        if ret:
            logger.warning(f"data:{video_filename}, link: {mp4_link}, already exists: {ret}")
            return True
        else:
            # logger.debug("start insert data!!!")
            # 插入数据时传入额外的参数
            insert_data_table(table_name, video_filename, mp4_link, keywords=keywords, save_path=is_local,
                              datal_id=datal_id)


@logger.catch
def get_data_and_down(info, keywords):
    try:
        attributes = info.get('attributes')
        title = attributes.get('id')
        video = attributes.get('video')
        mp4_link = video.get('src')
        logger.info(f"成功获取MP4链接:{mp4_link}")

        os.makedirs(folder_name, exist_ok=True)

        # 视频保存的文件名
        video_filename = f"{title}.mp4"
        # 构造下载文件的完整路径和文件名
        save_path_new = os.path.join(folder_name, keywords)

        os.makedirs(save_path_new, exist_ok=True)
        file_path = os.path.join(save_path_new, video_filename)

        flag = data_sql_check(pexels_table_name, video_filename, mp4_link, keywords, is_local=file_path,
                              datal_id=disk_id)
        # 检查文件是否已经存在
        if os.path.exists(file_path) or compare_video_dir(file_path, folder_name) or flag:
            logger.warning(f"文件已存在: {file_path}, 跳过下载: {mp4_link}")
        else:
            try:
                with requests.get(mp4_link, stream=True, timeout=60) as response:
                    response.raise_for_status()
                    # 以二进制方式打开文件用于写入
                    with open(file_path, 'wb') as file:
                        # 分块读取内容并写入文件
                        for chunk in response.iter_content(chunk_size=8192):
                            file.write(chunk)
                    logger.info(f'视频已成功保存到 {file_path}')
                    time.sleep(2)
            except Exception as e:
                logger.error(f"检查视频存在性时发生错误: {e}")
    except Exception as e:
        logger.error(f"函数发生错误: {e}")
        time.sleep(6)


@logger.catch
def spider_video(search_term):
    bic_url = f'https://www.pexels.com/zh-cn/api/v3/search/videos?query={search_term}&page=page_&per_page=24&orientation=all&size=all&color=all&sort=popular&seo_tags=true'

    headers = {
        "authority": "www.pexels.com",
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "content-type": "application/json",
        "referer": "https://www.pexels.com/zh-cn/search/videos/^%^E5^%^8A^%^A8^%^E7^%^89^%^A9/",
        "sec-ch-ua": "^\\^",
        "sec-ch-ua-arch": "^\\^",
        "sec-ch-ua-bitness": "^\\^",
        "sec-ch-ua-full-version": "^\\^",
        "sec-ch-ua-full-version-list": "^\\^",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "^\\^",
        "sec-ch-ua-platform-version": "^\\^",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "secret-key": "H2jk9uKnhRmL6WPwh89zBezWvr",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 SLBrowser/9.0.3.5211 SLBChan/25",
        "x-client-type": "react"
    }

    cookies = {
        "_ga": "GA1.1.847454793.1716803525",
        "OptanonAlertBoxClosed": "2024-05-27T09:52:07.144Z",
        "__cf_bm": "phupLYfVtz9w47IbOiISCjaIwMvPL.5a6PuDvD7kiX4-1728630903-1.0.1.1-RZnKDaPuK4Ic7YKNg0kyYPsq7.O2O79Q4JOIv9L93EvKD_eJC8vp7OxRksEg4QOG6.8p.XDdr3A.ey1KiHuibg",
        "_cfuvid": "XtG_mUTotRAQ8SyWkxDXEgOyaX7Wx7wQkuqJvZAtsL8-1728630904661-0.0.1.1-604800000",
        "_sp_ses.9ec1": "*",
        "country-code-v2": "CN",
        "OptanonConsent": "isGpcEnabled",
        "_sp_id.9ec1": "77bf343b-9555-472d-be05-da97c7fad4eb.1723108835.2.1728630955.1723108862.1fa1eecb-a012-4926-9cbd-cc411cb449a2.265c566f-5a99-496a-84da-9d1afd38cb44.fca9ca70-f8bb-458e-95e8-b5e5137f89f9.1728630945729.5",
        "_ga_8JE65Q40S6": "GS1.1.1728630956.3.0.1728630956.0.0.0",
        "cf_clearance": "v1f.3cGZpinHIkg8PrayKItRgVv16Y12xq9sYnVIkTQ-1728630958-1.2.1.1-rivqwBbiorDF2uUztIVca3UMt7aC.UbKYVGVp94rYXPpaoRSYuerLtskvrXPOBK2cuLSg0K2P_ffzpPAh15vdaXYVGmnZaDOa10XuL2wspEIKFqzP0r9Em1XsRC8VAxXB_sI5UYdjzLumUCE5cWYjHn9YGu5sP9yFSEOpbUx52V4CcmontDoPwXp4E4m9aQZgbMrdVrV_P6BUQP9eK_ddC6kl7nzoipGGpj0Gx_TYVuoGj0SCZ2ubQfIidBpmz53MDez0a7DZMyfypNVnK6TTZvMHK.1bYS93psTrefzVYvU7Xvf_5xN2CzUoDBZ4Vr79noISbE4G018g3Y4J4y0RXjAihjSG.ibrh56.mpQsgo6L1ETZ2uEvrwvOVqNPcj4dTx2067i2nVY8E3waGzKImCZgjKh0zE3Swtyxh_Sfew"
    }
    init_times = 0

    page = 1
    while True:
        try:
            time.sleep(15)
            page += 1
            url = bic_url.replace('page_', f'{page}')
            # 发送GET请求
            response = requests.get(url, headers=headers, cookies=cookies, timeout=60)
            response.encoding = response.apparent_encoding

            if response.status_code == 200:
                logger.info(f"page{page}页访问成功")

                data = response.json().get('data')
                if data is None:
                    logger.info(f"DOWN!!!")
                    break

                with ThreadPoolExecutor(max_workers=5) as executor:  # 假设使用5个线程
                    # 传递关键词给 get_data_and_down 函数
                    dfs = executor.map(lambda info: get_data_and_down(info, search_term), data)

            else:
                init_times += 1
                logger.info(f"page{page}页访问失败")

            if init_times > retry_times:
                logger.error(f"break loop, expect max retry times!!!: {url}")
                break
        except Exception as e:
            logger.error(f"发生错误0：{e}")


if __name__ == '__main__':
    search_terms = [
    #    "Pan",
    # "zoom out",
    # "zoom in",
    # "push in",
    # "pull out",
    # "hand-held",
    # "low angle",
    # "tacking",
    # "around",
    # "Birds-Eye-View Shot Overhead shot",
    # "over the shoulder",
    # "Ground camera",
    # "macro",
    # "translation",
    # "360 degree lens",
    # "time-lapse photography",
    # "slow motion",
    # "Tilt",
    #     "Drone perspective",
    #     "Sideways Tracking",
    #     "Longitudinal Dolly",
    #     "Pedestal Move",
    #     "Rotation Shot",
    #     "Whip Pan",
    #     "Follow Focus",
    #     "Focus Pull",
    #     "Dolly Zoom",
    #     "Arc Shot",
    #     "Helicopter Spin",
    #     "Swoop Zoom",
    #     "Undulating Motion",
    #     "Serpentine Tracking",
    #     "Pendulum Swing",
    #     "Combination Move",
    #     "Low-Angle Tilt",
    #     "High-Angle Tilt",
    #     "Dutch Angle",
    #     "POV",
    #     "Insect's Eye View",
    #     "Satellite Perspective",
    #     "Steadicam",
    #     "Gimbal",
    #     "Rail Dolly",
    #     "Jib Arm",
    #     "Boom Pole",
    #     "Drone",
    #     "Underwater Scooter",
    #     "Hyperlapse",
    #     "Freeze Frame",
    #     "Variable Speed",
    #     "Jump Cut",
    #     "Match Cut",
    #     "Seamless Transition",
    #     "Forward Tracking",
    #     "Backward Tracking",
    #     "Circular Tracking",
    #     "360° Pan",
    #     "Vertical Tilt",
    #     "Horizontal Pan",
    #     "Tilting Rotation",
    #     "Bullet Time",
    #     "Fast Motion",
    #     "Multiple Exposure",
    #     "Split Screen",
    #     "PIP",
    #     "Follow Spot",
    #     "Chasing Light",
    #     "Light Shifting",
    #     "Dynamic Shadow",
    #     "Color Temperature Shift",
    #     "Multi-Camera Sync",
    #     "Master-Slave Rig",
    #     "Robotic Arm",
    #     "Drone Swarm",
    #     "Underwater Housing",
    #     "Subjective Shot",
    #     "Objective Shot",
    #     "Metaphorical Shot",
    #     "Symbolic Shot",
    #     "Contrast Shot",
    #     "Parallel Editing",
    #     "Car Mount",
    #     "Cycle Mount",
    #     "Boat Rig",
    #     "Rail Mount",
    #     "Ski Cam",
    #     "Climbing Rig",
    #     "8K Panorama",
    #     "3D Audio",
    #     "VR Immersion",
    #     "AR Enhancement",
    #     "Mocap",
    #     "Virtual Production",
    #     "Rhythmic Editing",
    #     "Emotional Editing",
    #     "Informative Editing",
    #     "Suspense Editing",
    #     "Comedic Timing",
    #     "Explosion Capture",
    #     "Smoke Tracking",
    #     "Water Effect",
    #     "Fire Capture",
    #     "Particle Tracking",
    #     "Micro Camera",
    #     "Covert Camera",
    #     "Wearable Camera",
    #     "Extendable Lens",
    #     "Fisheye Lens",
    #     "establishing shot",
    #     "medium shot",
    #     "close-up",
    #     "extreme close-up",
    #     "two-shot",
    #     "group shot",
    #     "Flashback",
    #     "Flashforward",
    #     "Reverse Chronology",
    #     "Prolepsis",
    #     "Interpolation",
    #     "Surround Panning",
    #     "Dynamic Reverb",
    #     "Ambience Matching",
    #     "Foley Art",
    #     "Sound Montage",
    #     "Color Gradient",
    #     "Warm-Cool Contrast",
    #     "Monochromatic Accent",
    #     "Temperature Jump",
    #     "Highlight Bloom",
    #     "Handheld Shake",
    #     "Random Motion",
    #     "Anti-Gravity Shot",
    #     "Mirror Reflection",
    #     "Refraction Shot",
    #     "Anamorphic Lens",
    #     "Multi-Focus Capture"
    #     "FPV shot", "Drone shot", "Aerial shot",
    #     "Static shot",  # 固定镜头
    #     "Moving shot",  # 运动镜头
    #       "Horizontal movement",  # 水平移动
    #         "Vertical movement",  # 垂直移动
        "Circular movement",  # 环形移动
    #     "Slow movement",  # 慢速移动
    #     "Fast movement",  # 快速移动
    #     "Smooth movement",  # 平滑移动
    #     "Jittery movement",  # 抖动移动
    #     "Pan shot",  # 摇摄
    #     "Tilt shot",  # 俯仰拍摄
    #     "Zoom shot",  # 变焦拍摄
    #     "Dolly shot",  # 推轨拍摄
    #     "Tracking shot",  # 跟拍
    #     "Crane shot",  # 升降拍摄
    #     "Aerial shot",  # 航拍
    #     "Handheld shot",  # 手持拍摄
    #     "Steadicam shot",  # 稳定器拍摄
    #     "Time - lapse shot",  # 延时拍摄
    #     "Slow - motion shot",  # 慢动作拍摄
    #     "360 - degree shot",  # 360度拍摄
    #     "POV shot",  # 第一人称视角拍摄
    #     "Reverse shot",  # 反拍
    #     "Over - the - shoulder shot",  # 过肩拍摄
    #     "Establishing shot",  # 全景镜头
    #     "Close - up shot",  # 特写镜头
    #     "Medium shot",  # 中景镜头
    #     "Long shot",

    ]
    for search in search_terms:
        spider_video(search)
