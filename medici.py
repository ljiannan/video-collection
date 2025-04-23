import requests
import re
import os
import mysql.connector
import subprocess
import m3u8
import logging
import datetime
from urllib.parse import urlparse, urljoin, unquote # 导入 unquote 用于URL解码
from Cryptodome.Cipher import AES # 确保你安装了 pycryptodomex 库 (pip install pycryptodomex)
import time
import traceback # 导入 traceback 用于错误调试日志
import json # 导入 json 库用于解析 JSON 数据
from bs4 import BeautifulSoup # <--- 导入 BeautifulSoup 用于解析 HTML
import concurrent.futures  # 添加线程池支持
import threading  # 添加线程锁支持
# 确保已安装: pip install beautifulsoup4 lxml

# 配置日志记录
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 创建一个带时间戳的日志文件名
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"medici_download_{timestamp}.log")

    # 配置日志处理器
    logging.basicConfig(
        # level=logging.INFO, # 默认日志级别为 INFO
        level=logging.DEBUG, # <--- 保持 DEBUG，用于调试 CSRF 令牌获取
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler() # 同时输出到控制台
        ]
    )

    # 设置 requests 库的日志级别，避免输出过多调试信息
    logging.getLogger("requests").setLevel(logging.WARNING) # <--- 抑制 requests 自身的 DEBUG 输出
    logging.getLogger("urllib3").setLevel(logging.WARNING) # <--- 抑制 urllib3 自身的 DEBUG 输出

    logger = logging.getLogger()
    logger.info(f"日志已初始化，日志文件: {log_file}")
    return logger

# 初始化日志记录器 (在全局范围，main 函数会使用)
# 确保在 logger 被使用之前调用 setup_logging()
# 但是为了避免全局变量的延迟初始化问题，通常在 if __name__ == "__main__": 块中初始化 logger
logger = logging.getLogger() # 声明一个空的 logger，防止在 setup_logging 前调用出错

# 设置下载路径和ffmpeg路径
INPUT_PATH = "E:\meidci"
FFMPEG_PATH = r"D:\ffmpeg-7.0.2-essentials_build\bin\ffmpeg.exe"

# 添加数据库线程锁
db_lock = threading.Lock()

# 数据库连接配置
DB_CONFIG = {
    "host": "192.168.10.70",
    "user": "root",
    "password": "zq828079",  # 根据实际情况修改，注意安全！
    "database": "data_sql"
}

def create_db():
    """
    连接到MySQL数据库，并创建表（如果不存在）
    """
    logger.info("尝试连接到MySQL数据库...")
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 创建表（如果不存在）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS medici_video (
                slug VARCHAR(255) PRIMARY KEY,
                title VARCHAR(255),
                video_url TEXT,
                download_state BOOLEAN DEFAULT FALSE
            )
        ''')
        conn.commit()
        logger.info("MySQL数据库已连接，表 'medici_video' 已创建或已存在")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"数据库连接或操作失败: {err}")
        # 可以在这里检查 err.errno == errorcode.CR_AUTHENTICATION_PLUGIN_NOT_LOADED
        # 来确认是否是认证插件问题
        if conn:
            conn.close()
        return None


def fetch_concerts(offset, limit=16):
    """
    从API获取音乐会列表数据
    """
    url = f"https://api.medici.tv/search/concert?offset={offset}&limit={limit}"
    logger.info(f"从API获取音乐会数据: {url}")

    # 获取最新的授权令牌，用于 API 请求
    # 注意：这里每次获取音乐会数据都调用一次 get_auth_token()，效率可能不高
    # 更好的做法是在主循环中获取一次，然后传递给 fetch_concerts
    # 但是考虑到 get_auth_token() 内部包含了刷新逻辑，暂时这样处理
    auth_token = get_auth_token() # 这里会触发模拟登录流程
    if not auth_token:
        logger.error("无法获取有效的授权令牌，跳过获取音乐会数据。")
        return None

    # 设置请求头，包含授权令牌
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en",
        "Authorization": auth_token, # 使用获取到的令牌
        "Origin": "https://www.medici.tv",
        "Referer": "https://www.medici.tv/", # API 的 Referer 通常是网站主页或调用页面
        "sec-ch-ua": "\"Google Chrome\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    try:
        response = requests.get(url, headers=headers, timeout=(10, 30)) # 增加超时设置
        if response.status_code == 200:
            logger.info(f"成功获取到音乐会数据，状态码: {response.status_code}")
            return response.json()
        else:
            logger.error(f"获取音乐会数据失败，状态码: {response.status_code}")
            logger.debug(f"获取音乐会数据响应: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"请求音乐会数据时发生错误: {str(e)}")
        return None

def get_auth_token():
    logger.info("开始获取新的授权令牌 (硬编码 CSRF 进行测试)")

    # 创建一个会话
    session = requests.Session()

    # --- 临时硬编码 CSRF 令牌进行测试 ---
    # *** 将这里的值替换为你从浏览器复制的 csrftoken_satie 值 ***
    csrf_token_for_test = "8OPasx9sLHGqjPqDqAwAXcvkycUF7amX7wgu4RPeE67gDGVAN7gQVXC6TndAh0zm" # <<<<<<<<<<<<<<<<<<< 修改这里！
    logger.warning(f"使用硬编码的 CSRF 令牌进行测试: {csrf_token_for_test}")
    # --- 结束临时硬编码 ---


    # 通用请求头 (不含硬编码的Cookie)
    common_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en",
        "Origin": "https://www.medici.tv",
        "Referer": "https://www.medici.tv/en/login", # 登录API的Referer通常是登录页面
        "sec-ch-ua": "\"Google Chrome\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }
    # 访问登录页面的 headers 不再需要，因为我们跳过了那一步


    try:
        # --- 第1步和第2步合并：直接执行登录操作 (使用 POST 方法，新的登录接口) ---
        # *** 修改这里的 URL 为新的登录接口 ***
        login_url = "https://api.medici.tv/satie/login/" # <--- 使用新的登录接口 URL
        logger.info(f"尝试登录新接口 (使用硬编码CSRF): {login_url}")

        login_headers = common_headers.copy() # 复制通用头
        # *** 将硬编码的 CSRF 令牌添加到请求头中 ***
        login_headers["x-csrftoken"] = csrf_token_for_test # <<<<<<<<<< 使用硬编码的值
        logger.debug("在登录请求头中设置硬编码的 x-csrftoken。")

        login_headers["Content-Type"] = "application/json" # 告诉服务器发送的是 JSON 数据
        # *** 检查 Referer ***
        login_headers["Referer"] = "https://www.medici.tv/" # <--- 尝试修改 Referer (或者保持 /en/login)

        # 登录凭据，请确保正确
        login_data = {
            "username": "administrator@zgxmt.top", # <--- 使用你的正确邮箱
            "password": "Caozhaoqi@828079" # <--- 使用你的正确密码！*** 再次检查密码是否正确无误！***
        }

        # 使用 session.post 发送 POST 请求
        # 注意：即使硬编码了 CSRF，但 session 可能需要一些默认 cookie，
        # 如果直接跳过访问 /en/login，可能缺少这些 session 必须的 cookie。
        # 如果直接 POST 失败，可能需要先访问一个基础页面让 session 获取一些初始 cookie。
        # 尝试直接 POST，如果失败再考虑先访问主页等。

        login_response = session.post(
            login_url,
            json=login_data,
            headers=login_headers,
            timeout=(10, 30)
        )

        if login_response.status_code != 200:
            logger.error(f"登录新接口失败 (使用硬编码CSRF)，状态码: {login_response.status_code}")
            logger.debug(f"登录响应内容: {login_response.text}") # 打印响应内容有助于调试
            # 重点看响应内容，服务器可能会在响应中给出失败的具体原因或提示。
            return None # 登录失败

        logger.info("登录新接口成功 (使用硬编码CSRF)")

        # --- 第3步：从登录成功响应的 JSON 体中提取 JWT Access Token ---
        # ... (这部分代码保持不变，因为它依赖于你提供的 JSON 响应结构) ...
        try:
            login_response_data = login_response.json() # 解析 JSON 响应体
            logger.debug(f"登录成功响应 JSON: {login_response_data}") # 打印完整的 JSON 响应体

            # 检查 JSON 结构，提取 access token
            if "jwt" in login_response_data and "access" in login_response_data["jwt"]:
                access_token = login_response_data["jwt"]["access"]
                logger.info("成功从登录响应 JSON 中提取到 Access Token。")

                # Access Token 通常需要加上 "Bearer " 前缀用于 Authorization 头
                auth_token = f"Bearer {access_token}"

                logger.info("成功获取到授权令牌 (Bearer Access Token)。")
                return auth_token # 返回 Bearer Access Token

            else:
                logger.error("登录成功响应 JSON 中未找到 'jwt' 或 'access' 字段。")
                return None

        except json.JSONDecodeError:
            logger.error("登录成功响应不是有效的 JSON。")
            logger.debug(f"原始响应内容: {login_response.text}")
            return None
        except Exception as e:
            logger.error(f"处理登录响应数据时发生错误: {str(e)}")
            logger.debug(traceback.format_exc())
            return None


    except requests.exceptions.RequestException as req_err:
        logger.error(f"请求过程中发生错误: {str(req_err)}")
        logger.debug(traceback.format_exc())
    except Exception as e:
        logger.error(f"获取令牌过程中发生意外错误: {str(e)}")
        logger.debug(traceback.format_exc())

    # 如果上述流程失败，返回 None
    logger.error("无法获取新的授权令牌，所有尝试失败。")
    return None

# ... (其他函数 download_file, get_highest_resolution_stream, process_m3u8, download_and_process_video, main 保持不变) ...
# 注意：需要安装 BeautifulSoup 和 lxml 库 (虽然在这个临时测试版本中没用到，但正式获取时可能需要)
# pip install beautifulsoup4 lxml


def fetch_video_url(slug):
    """
    根据视频 slug 获取视频的 M3U8 URL
    """
    url = f"https://api.medici.tv/satie/edito/movie-file/{slug}/"
    logger.info(f"获取视频URL元数据: {url}")

    # 获取最新的授权令牌
    # 注意：这里每次获取视频URL都调用一次 get_auth_token()，效率可能不高
    # 更好的做法是在主循环中获取一次，并在需要时检查令牌是否有效并刷新
    auth_token = get_auth_token() # 这里会触发模拟登录流程
    if not auth_token:
        logger.error(f"无法获取有效的授权令牌，无法获取视频URL ({slug})。")
        return None

    # 设置请求头，包含授权令牌
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Accept": "application/json", # 期望 JSON 响应
        "Accept-Language": "en",
        "Authorization": auth_token, # 使用获取到的令牌
        "Origin": "https://www.medici.tv",
        # Referer 在这里也可能需要调整，例如指向获取视频元数据的页面
        # 对于这个API，Referer 可以尝试设为空或者主页
        "Referer": "https://www.medici.tv/", # <--- 尝试修改 Referer
        "site": "b2c", # 站点标识，可能需要根据实际情况调整
        "site-catalog": "b2c", # 站点目录，可能需要调整
        "sec-ch-ua": "\"Google Chrome\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    try:
        response = requests.get(url, headers=headers, timeout=(10, 30)) # 增加超时设置
        if response.status_code == 200:
            data = response.json()
            # 检查响应结构，找到视频URL
            if "video" in data and "video_url" in data["video"]:
                video_url = data["video"]["video_url"]
                logger.info(f"成功获取视频URL: {video_url}")
                return video_url
            else:
                logger.warning(f"在响应中未找到视频URL ({slug})。响应内容: {data}")
                return None
        else:
            logger.error(f"获取视频URL失败 ({slug})，状态码: {response.status_code}")
            logger.debug(f"获取视频URL响应内容: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"请求视频URL时发生错误 ({slug}): {str(e)}")
        return None


def download_file(url, filename, max_retries=3, retry_delay=3, verify_ssl=True):
    """
    下载文件，带有重试机制和超时设置
    """
    logger.info(f"下载文件: {url} -> {filename}")

    # 使用 requests.Session() 提高效率，尤其是下载多个分段时
    session = requests.Session()

    # 定义重试策略,应用于 HTTPAdapter
    # 注意：这里的 retry_strategy 是针对 adapter 的，一次 session.get 调用内部会使用这个重试
    # 如果 session.get 内部重试 total 次后仍失败，会抛出 RetryError 或其基类 RequestException
    # download_file 函数层面的重试逻辑会捕获这个异常并进行外部循环重试
    retry_strategy = requests.adapters.Retry(
        total=3, # Adapter 内部重试次数
        backoff_factor=1, # Adapter 内部退避因子
        status_forcelist=[429, 500, 502, 503, 504], # Adapter 内部重试状态码
        allowed_methods=["GET", "HEAD"] # Adapter 内部重试方法
    )

    adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter) # 将重试策略应用于 http 和 https连接
    session.mount("https://", adapter)

    # 手动控制 download_file 函数层面的重试
    for attempt in range(max_retries + 1):
        try:
            # 使用 session.get，Adapter 会自动处理内部重试
            response = session.get(url, verify=verify_ssl, timeout=(10, 60)) # 增加读取超时

            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
                logger.info(f"文件下载成功: {filename}")
                return True # 下载成功，返回 True 并结束 download_file 函数的重试循环
            else:
                logger.warning(f"文件下载尝试 {attempt+1}/{max_retries+1} 失败，状态码: {response.status_code}")
                # 如果状态码不是 200，且不是 Adapter 重试列表中的状态码，Adapter 不会重试
                # 这里我们手动控制 download_file 函数层面的重试

        except requests.exceptions.SSLError as ssl_err:
            logger.warning(f"SSL错误尝试 {attempt+1}/{max_retries+1}: {str(ssl_err)}")
            # SSL错误通常与重试无关，但可以在最后一次尝试时尝试不验证SSL证书
            if verify_ssl and attempt == max_retries: # 在最后一次尝试时，如果启用了验证，则尝试不验证
                 logger.info("尝试不验证SSL证书下载...")
                 # 在这里不再递归调用，避免无限循环或堆栈溢出
                 # 而是直接返回 False，让外层 download_file 函数层面的循环结束或跳过
                 # 注意：这种处理 SSL 错误的方式需要根据实际情况调整，可能需要更复杂的逻辑
                 return False # 标记为下载失败，不再重试SSL错误
            # 如果是 SSL 错误且不尝试不验证，或者不验证也失败，不等待，直接进行 download_file 函数层面的下一次尝试或结束
        except requests.exceptions.RequestException as req_err:
            # Adapter 内部重试后失败会抛出 RequestException (如 ConnectionError, Timeout, RetryError等)
            logger.warning(f"请求错误尝试 {attempt+1}/{max_retries+1}: {str(req_err)}")
            logger.debug(traceback.format_exc())
            # 发生请求错误，手动控制 download_file 函数层面的重试

        except Exception as e:
            logger.warning(f"下载文件尝试 {attempt+1}/{max_retries+1} 失败: {str(e)}")
            logger.debug(traceback.format_exc())
            # 发生其他未知错误，手动控制 download_file 函数层面的重试

        # 如果不是最后一次尝试，且没有成功下载，则等待后重试
        # 只有在发生 RequestException 或非 200 状态码的响应时才等待重试
        if attempt < max_retries:
            # 只有在发生 RequestException 或非 SSL 错误时才等待
            # 如果是 SSL 错误且不尝试不验证，则不等待
            # if not (isinstance(ssl_err, requests.exceptions.SSLError) and not verify_ssl): # 这样判断可能复杂
            wait_time = retry_delay * (2 ** attempt) # 指数退避策略
            logger.info(f"等待 {wait_time} 秒后重试下载文件...")
            time.sleep(wait_time)
        else:
             # 达到最大尝试次数且未能成功下载
             logger.error(f"下载文件失败，已达到最大重试次数: {max_retries}")
             return False # 下载失败


    # 理论上代码不会走到这里，因为上面的循环在成功时会 return True，失败时会 return False
    return False # 默认返回 False (理论上不可达)


def get_highest_resolution_stream(master_m3u8_content):
    """
    分析主 M3U8 内容，找到最高分辨率的视频流 URI
    """
    logger.info("分析主m3u8内容，查找最高分辨率的流")
    try:
        m3u8_obj = m3u8.loads(master_m3u8_content)
        max_resolution_pixels = 0 # 使用像素总数来比较分辨率
        highest_res_stream_uri = None

        # 遍历所有媒体播放列表 (通常包含不同的分辨率流)
        for playlist in m3u8_obj.playlists:
            stream_info = playlist.stream_info
            if stream_info and stream_info.resolution:
                resolution = stream_info.resolution
                # 确保 resolution 是 (width, height) 元组且包含有效数字
                if isinstance(resolution, (list, tuple)) and len(resolution) == 2 and isinstance(resolution[0], int) and isinstance(resolution[1], int):
                     res_pixels = resolution[0] * resolution[1]
                     # logger.debug(f"找到分辨率: {resolution[0]}x{resolution[1]}, 像素: {res_pixels}, URI: {playlist.uri}") # 调试用
                     if res_pixels > max_resolution_pixels:
                         max_resolution_pixels = res_pixels
                         highest_res_stream_uri = playlist.uri # 获取流的 URI

        if highest_res_stream_uri:
            logger.info(f"找到最高分辨率流URI: {highest_res_stream_uri}")
        else:
            logger.warning("在m3u8中没有找到任何有效分辨率流")
        return highest_res_stream_uri
    except Exception as e:
        logger.error(f"解析m3u8内容出错: {str(e)}")
        logger.debug(traceback.format_exc())
        return None


def process_m3u8(video_url, max_retries=5, retry_delay=3):
    """
    处理主 M3U8 文件，获取最高分辨率流的完整 URL
    """
    logger.info(f"处理主m3u8文件: {video_url}")

    for attempt in range(max_retries + 1): # 手动控制尝试次数
        try:
            # 这里可以对主 M3U8 文件下载也添加重试逻辑
            # 使用 requests.get 或 session + adapter 均可
            response = requests.get(video_url, timeout=(10, 30), verify=True) # 可以添加简单的重试逻辑或使用 session

            if response.status_code == 200:
                master_content = response.text
                # 获取最高分辨率流的 URI
                highest_res_stream_uri = get_highest_resolution_stream(master_content)

                if highest_res_stream_uri:
                    # 构建最高分辨率流的完整 URL
                    parsed_url = urlparse(video_url)
                    # 获取 M3U8 文件的基础 URL (目录部分)
                    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{os.path.dirname(parsed_url.path)}/"
                    # 使用 urljoin 拼接基础 URL 和流 URI，处理相对路径
                    stream_url = urljoin(base_url, highest_res_stream_uri)
                    logger.info(f"构建最高分辨率流完整URL: {stream_url}")
                    return stream_url # 成功获取流 URL，结束 process_m3u8 的重试循环
                else:
                    logger.warning(f"尝试 {attempt+1}/{max_retries+1}: 在主m3u8 ({video_url}) 中未找到有效流。")
                    # 没有找到流，可能是 M3U8 内容有问题，不重试 M3u8 下载本身，直接结束
                    return None # 直接返回 None 结束尝试

            else:
                logger.warning(f"尝试 {attempt+1}/{max_retries+1}: 下载主m3u8失败 ({video_url})，状态码: {response.status_code}")

        except requests.exceptions.RequestException as e:
             logger.warning(f"尝试 {attempt+1}/{max_retries+1}: 请求主m3u8出错 ({video_url}): {str(e)}")
             logger.debug(traceback.format_exc())
        except Exception as e:
             logger.warning(f"尝试 {attempt+1}/{max_retries+1}: 处理主m3u8时发生意外错误 ({video_url}): {str(e)}")
             logger.debug(traceback.format_exc())


        # 如果不是最后一次尝试，并且发生了请求错误 (值得重试)，则等待后重试
        if attempt < max_retries:
            wait_time = retry_delay * (2 ** attempt) # 指数退避策略
            logger.info(f"等待 {wait_time} 秒后重试下载主m3u8...")
            time.sleep(wait_time)
        else:
             # 达到最大尝试次数且未能成功下载
             logger.error("处理主m3u8文件失败，已达到最大重试次数")


    logger.error("处理主m3u8文件失败，已达到最大重试次数")
    return None


def download_and_process_video(slug, video_url, output_dir=INPUT_PATH, max_segment_retries=3, max_m3u8_retries=3):
    """
    下载 M3U8 视频流，解密并合并视频文件
    支持加密和非加密流
    """
    logger.info(f"开始下载和处理视频: {slug}")
    # 创建输出目录（使用slug子目录）
    slug_dir = os.path.join(output_dir, slug)
    os.makedirs(slug_dir, exist_ok=True)

    # 1. 处理主 M3U8 获取最高分辨率流的完整 URL
    stream_url = process_m3u8(video_url, max_retries=max_m3u8_retries)
    if not stream_url:
        logger.error(f"无法获取最高分辨率流URL，跳过视频处理 ({slug})。")
        return None

    # 2. 下载流 M3U8 文件
    logger.info(f"下载流m3u8文件: {stream_url}")
    try:
        stream_response = requests.get(stream_url, timeout=(10, 30), verify=True)
        if stream_response.status_code != 200:
            logger.error(f"下载流m3u8文件失败 ({slug}): {stream_url}, 状态码: {stream_response.status_code}")
            logger.debug(f"流m3u8响应: {stream_response.text}")
            return None
        stream_content = stream_response.text
        logger.info("成功下载流m3u8文件。")
    except requests.exceptions.RequestException as e:
        logger.error(f"请求流m3u8文件时发生错误 ({slug}): {str(e)}")
        return None
    except Exception as e:
        logger.error(f"下载流m3u8文件时发生意外错误 ({slug}): {str(e)}")
        logger.debug(traceback.format_exc())
        return None

    # 3. 检查流是否加密
    # 查找 #EXT-X-KEY 标签
    key_info_match = re.search(r'#EXT-X-KEY:METHOD=AES-128,(.*)', stream_content)
    is_encrypted = key_info_match is not None
    
    # 初始化解密相关变量
    key_data = None
    
    # 如果流是加密的，获取密钥和IV
    if is_encrypted:
        logger.info("检测到加密流，准备解密处理")
        key_attributes_str = key_info_match.group(1)
        logger.debug(f"密钥属性字符串: {key_attributes_str}")

        # 从属性字符串中提取 URI
        key_uri_match = re.search(r'URI="([^"]+)"', key_attributes_str)
        if not key_uri_match:
            logger.error("在 EXT-X-KEY 标签中未找到密钥 URI。")
            return None

        key_uri = key_uri_match.group(1)
        logger.info(f"找到密钥URI: {key_uri}")

        # 下载加密密钥
        logger.info(f"下载加密密钥: {key_uri}")
        try:
            key_response = requests.get(key_uri, timeout=(10, 30), verify=True)
            if key_response.status_code != 200:
                logger.error(f"下载加密密钥失败 ({slug}): {key_uri}, 状态码: {key_response.status_code}")
                return None
            key_data = key_response.content
            logger.info("成功获取加密密钥。")
        except Exception as e:
            logger.error(f"下载加密密钥时发生意外错误 ({slug}): {str(e)}")
            logger.debug(traceback.format_exc())
            return None
    else:
        logger.info("检测到非加密流，将直接下载TS分段")

    # 4. 解析 TS 分段 URL
    parsed_stream_url = urlparse(stream_url)
    ts_base_url = f"{parsed_stream_url.scheme}://{parsed_stream_url.netloc}{os.path.dirname(parsed_stream_url.path)}/"
    logger.info(f"TS 分段基础URL: {ts_base_url}")

    # 提取所有 .ts 分段的文件名
    ts_uris = []
    for line in stream_content.splitlines():
        if line.strip().endswith('.ts'):
            ts_uris.append(line.strip())

    if not ts_uris:
        logger.error("在流m3u8中未找到任何TS分段 URI。")
        return None

    logger.info(f"找到 {len(ts_uris)} 个TS分段 URI")

    # 5. 下载并解密 TS 分段（如果需要）
    segment_files = []
    failed_segments = 0
    max_allowed_failures = 5  # 允许最多5个分段失败

    for i, ts_uri in enumerate(ts_uris):
        # 构建 TS 分段的完整 URL
        ts_url = urljoin(ts_base_url, ts_uri)
        
        # 根据是否加密，生成不同的本地文件名
        if is_encrypted:
            local_encrypted_file = os.path.join(slug_dir, f"{slug}_{i:04d}.ts.encrypted")
            local_decrypted_file = os.path.join(slug_dir, f"{slug}_{i:04d}.ts")
        else:
            # 非加密流直接保存为 .ts 文件
            local_decrypted_file = os.path.join(slug_dir, f"{slug}_{i:04d}.ts")
            local_encrypted_file = None  # 非加密流不需要此变量

        # 下载分段
        logger.debug(f"下载分段 {i+1}/{len(ts_uris)}: {ts_url}")
        
        if is_encrypted:
            # 加密流：先下载加密文件，然后解密
            if download_file(ts_url, local_encrypted_file, max_retries=max_segment_retries):
                # 下载成功，解密分段
                try:
                    logger.debug(f"解密分段 {i+1}/{len(ts_uris)}")
                    with open(local_encrypted_file, 'rb') as ef:
                        encrypted_data = ef.read()

                    # 使用 IV 创建新的 cipher 对象
                    # 提取 IV（默认使用分段索引作为IV）
                    iv = i.to_bytes(16, byteorder='big')
                    
                    # 检查 m3u8 中是否指定了 IV
                    iv_match = re.search(r'IV=0x([0-9a-fA-F]+)', key_attributes_str)
                    if iv_match:
                        try:
                            iv = bytes.fromhex(iv_match.group(1))
                        except Exception as e:
                            logger.error(f"解析指定IV失败: {str(e)}")
                            # 回退到默认IV
                    
                    # 每个分段创建新的 cipher 对象，并在创建时传入 IV
                    cipher = AES.new(key_data, AES.MODE_CBC, iv)
                    decrypted_data = cipher.decrypt(encrypted_data)

                    # 写入解密后的数据（处理填充）
                    with open(local_decrypted_file, 'wb') as df:
                        # 处理 PKCS7 填充
                        padding_byte = decrypted_data[-1]
                        if 0 < padding_byte <= AES.block_size and all(b == padding_byte for b in decrypted_data[-padding_byte:]):
                            decrypted_data = decrypted_data[:-padding_byte]
                        df.write(decrypted_data)

                    segment_files.append(local_decrypted_file)
                    # 清理加密文件
                    if os.path.exists(local_encrypted_file):
                        os.remove(local_encrypted_file)

                except Exception as e:
                    logger.error(f"解密或处理分段文件失败 ({slug}_{i:04d}.ts): {str(e)}")
                    logger.debug(traceback.format_exc())
                    failed_segments += 1
                    # 清理失败文件
                    for f in [local_encrypted_file, local_decrypted_file]:
                        if f and os.path.exists(f):
                            try: os.remove(f)
                            except: pass
            else:
                # 下载分段失败
                failed_segments += 1
        else:
            # 非加密流：直接下载到最终文件
            if download_file(ts_url, local_decrypted_file, max_retries=max_segment_retries):
                segment_files.append(local_decrypted_file)
            else:
                failed_segments += 1

        # 打印进度
        if (i+1) % 50 == 0 or i+1 == len(ts_uris):
            logger.info(f"已处理 {i+1}/{len(ts_uris)} 个分段 ({(i+1)/len(ts_uris)*100:.1f}%)")

        # 检查失败分段数量
        if failed_segments > max_allowed_failures:
            logger.error(f"失败分段数量 ({failed_segments}) 已超过阈值 ({max_allowed_failures})，放弃视频处理 ({slug})。")
            # 清理已下载文件
            for sf in segment_files:
                if os.path.exists(sf):
                    try: os.remove(sf)
                    except: pass
            return None

    # 6. 使用 ffmpeg 合并分段文件
    if not segment_files:
         logger.error("没有成功下载和解密任何分段文件，无法合并视频。")
         return None

    logger.info(f"成功处理 {len(segment_files)} 个分段。开始使用ffmpeg合并视频分段...")

    segments_list_file = os.path.join(slug_dir, f"{slug}_segments_list.txt")
    final_output_file = os.path.join(output_dir, f"{slug}.mp4")

    try:
        # 创建分段文件列表
        with open(segments_list_file, 'w') as f:
            for segment in segment_files:
                # 写入文件名（相对于工作目录）
                f.write(f"file '{os.path.basename(segment)}'\n")

        # 构建 ffmpeg 命令
        ffmpeg_cmd = [
            FFMPEG_PATH,
            '-f', 'concat',
            '-safe', '0',
            '-i', os.path.basename(segments_list_file),
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            os.path.basename(final_output_file)
        ]

        logger.info(f"执行ffmpeg命令 (在目录 {slug_dir} 中): {' '.join(ffmpeg_cmd)}")

        # 执行 ffmpeg 命令
        result = subprocess.run(ffmpeg_cmd, cwd=slug_dir, capture_output=True, text=True)

        if result.returncode == 0:
            logger.info("ffmpeg 合并视频分段成功。")
            # 将输出文件移动到输出目录
            final_path = os.path.join(slug_dir, os.path.basename(final_output_file))
            output_path = os.path.join(output_dir, f"{slug}.mp4")
            
            if os.path.exists(final_path):
                # 如果输出目录不是slug目录，则移动文件
                if slug_dir != output_dir:
                    import shutil
                    shutil.move(final_path, output_path)
                    logger.info(f"将合并的视频移动到: {output_path}")
                    return output_path
                else:
                    return final_path
            else:
                logger.error(f"ffmpeg 命令报告成功，但最终输出文件未找到: {final_path}")
                return None
        else:
            logger.error(f"ffmpeg 合并视频分段失败。返回码: {result.returncode}")
            logger.error(f"ffmpeg stderr:\n{result.stderr}")
            return None

    except Exception as e:
        logger.error(f"执行 ffmpeg 或处理文件时发生意外错误: {str(e)}")
        logger.debug(traceback.format_exc())
        return None
    finally:
        # 清理临时文件
        logger.info("清理临时分段文件和列表文件...")
        for f in [segments_list_file, *segment_files]:
            if os.path.exists(f):
                try: os.remove(f)
                except Exception as e: 
                    logger.warning(f"清理文件失败 {f}: {str(e)}")

# 添加处理单个音乐会的函数，用于多线程处理
def process_concert(concert, conn):
    """
    处理单个音乐会数据，用于多线程下载
    """
    slug = concert.get("slug")
    title = concert.get("title")

    if not slug or not title:
        logger.warning(f"跳过无效音乐会数据: {concert}")
        return

    # 清理标题中的非法字符，用于文件名和数据库
    safe_title = re.sub(r'[\\/:*?"<>|]', '_', title)
    logger.info(f"处理音乐会: {safe_title} (slug: {slug})")

    # 使用线程安全的方式检查是否已在数据库中
    try:
        with db_lock:
            cursor = conn.cursor()
            cursor.execute("SELECT download_state FROM medici_video WHERE slug = %s", (slug,))
            record = cursor.fetchone()

        if record:
            # 记录已存在
            download_state = record[0]

            if download_state:
                logger.info(f"视频已成功下载，跳过处理 Slug: {slug}")
                return
            else:
                # 记录存在但未下载成功，尝试重新下载
                logger.info(f"数据库中存在未完成的记录，尝试重新下载 Slug: {slug}")
                video_url = fetch_video_url(slug)
                if video_url:
                    # 处理视频
                    logger.info(f"开始重新下载和处理视频 Slug: {slug} - {safe_title}")
                    output_file = download_and_process_video(slug, video_url, output_dir=INPUT_PATH)
                    if output_file:
                        # 更新下载状态为True
                        with db_lock:
                            cursor = conn.cursor()
                            logger.info(f"更新下载状态为已完成 Slug: {slug}")
                            cursor.execute("UPDATE medici_video SET download_state = %s WHERE slug = %s", (True, slug))
                            conn.commit()
                        logger.info(f"视频重新处理成功: {slug} - {safe_title} -> {output_file}")
                    else:
                        logger.error(f"视频重新处理失败: {slug} - {safe_title}")
                else:
                    logger.error(f"未找到视频URL，无法重新处理 Slug: {slug} - {safe_title}")

        else:
            # 记录不存在，执行插入操作
            with db_lock:
                cursor = conn.cursor()
                logger.info(f"数据库中未找到记录，插入新记录: {slug} - {safe_title}")
                cursor.execute("INSERT INTO medici_video (slug, title, download_state) VALUES (%s, %s, %s)",
                            (slug, title, False))
                conn.commit()

            # 获取视频URL
            video_url = fetch_video_url(slug)
            if video_url:
                # 更新数据库中的视频URL
                with db_lock:
                    cursor = conn.cursor()
                    logger.info(f"更新数据库中的视频URL: {slug}")
                    cursor.execute("UPDATE medici_video SET video_url = %s WHERE slug = %s",
                                (video_url, slug))
                    conn.commit()

                # 处理视频
                logger.info(f"开始下载和处理视频 Slug: {slug} - {safe_title}")
                output_file = download_and_process_video(slug, video_url, output_dir=INPUT_PATH)
                if output_file:
                    # 更新下载状态为True
                    with db_lock:
                        cursor = conn.cursor()
                        logger.info(f"更新下载状态为已完成 Slug: {slug}")
                        cursor.execute("UPDATE medici_video SET download_state = %s WHERE slug = %s",
                                    (True, slug))
                        conn.commit()
                    logger.info(f"视频处理成功: {slug} - {safe_title} -> {output_file}")
                else:
                    logger.error(f"视频处理失败: {slug} - {safe_title}")
            else:
                logger.error(f"未找到视频URL，无法处理 Slug: {slug} - {safe_title}")

    except mysql.connector.errors.IntegrityError as e:
        # 处理可能的并发插入导致的主键冲突
        logger.warning(f"插入记录时发生主键冲突: {str(e)}，Slug: {slug}")
        # 尝试更新记录
        try:
            with db_lock:
                cursor = conn.cursor()
                cursor.execute("UPDATE medici_video SET title = %s WHERE slug = %s",
                            (title, slug))
                conn.commit()
            logger.info(f"已更新冲突记录的标题: {slug}")
        except Exception as update_err:
            logger.error(f"更新冲突记录失败: {str(update_err)}, Slug: {slug}")
    except Exception as db_err:
        logger.error(f"处理数据库记录时发生错误: {str(db_err)}, Slug: {slug}")
        logger.debug(traceback.format_exc())

def main():
    global logger # 声明使用全局的 logger 变量
    logger = setup_logging() # 初始化 logger

    # 确保输出目录存在
    os.makedirs(INPUT_PATH, exist_ok=True)
    logger.info(f"下载路径已设置为: {INPUT_PATH}")

    # 数据库连接
    conn = create_db()
    if not conn:
        logger.error("无法连接数据库，任务终止。")
        return # 数据库连接失败，终止 main 函数

    # 设置多线程下载
    max_workers = 3  # 同时下载3个视频
    logger.info(f"设置并行下载任务数: {max_workers}")

    # 遍历音乐会列表
    for i in range(2, 40):
        offset = 16 * i
        # 获取音乐会数据
        concerts_data = fetch_concerts(offset, limit=16)
        if not concerts_data or "movies" not in concerts_data or "results" not in concerts_data["movies"]:
            logger.error(f"未从 offset {offset} 获取到音乐会数据，或数据结构不正确。")
            continue

        result_count = len(concerts_data["movies"]["results"])
        logger.info(f"从 offset {offset} 获取到 {result_count} 个音乐会数据")

        # 使用线程池并行处理音乐会下载
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务到线程池
            futures = []
            for concert in concerts_data["movies"]["results"]:
                futures.append(executor.submit(process_concert, concert, conn))
            
            # 等待所有任务完成
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"任务执行过程中发生错误: {str(e)}")
                    logger.debug(traceback.format_exc())

    # 关闭数据库连接
    if conn:
        conn.close()
    logger.info("====== Medici视频下载任务完成 ======")

if __name__ == "__main__":
    main()