# https://pixabay.com/zh/videos/search/?pagi=5
import datetime
import logging
from logging.handlers import RotatingFileHandler

from loguru import logger

from fake_useragent import UserAgent
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
import csv
import os
from selenium.webdriver.chrome.options import Options
import requests
from tqdm import tqdm
from urllib.parse import unquote

from src.sql.cons import chrome_path

# from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
#
# driver = webdriver.Chrome(ChromeDriverManager().install())
# 设置日志
log_dir = "./logs"
log_file = os.path.join(log_dir, "pixbay_video.log")
max_log_size = 10 * 1024 * 1024  # 10MB
backup_count = 5  # 最多保留5个日志文件

# 创建logs目录，如果不存在
os.makedirs(log_dir, exist_ok=True)
# 创建 RotatingFileHandler 实例
log_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count, encoding='utf-8')
# 设置日志格式
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_format)



def get_ua():
    ua = UserAgent()
    headers = {
        "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0'}
    return headers


options = Options()
# chrome_options.add_argument("--headless")
options.page_load_strategy = 'eager'

headers = {
    'Content-Type': 'application/json',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0'
}


def write_to_csv(csv_file_path, video_details):
    header_written = False
    with open(csv_file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['lanmu', 'name', 'url'])

        # 如果文件是空的或者我们之前没有写入过标题行，则写入标题行
        if os.path.getsize(csv_file_path) == 0 or not header_written:
            writer.writeheader()
            header_written = True  # 设置标志为True，表示已经写入了标题行

            # 遍历item_details列表并写入每一行
            for item in video_details:
                writer.writerow(item)

    logger.debug(f"数据已保存至：{csv_file_path}")


def download_file(mp4_url, file_path):
    headers = get_ua()  # 假设这个函数返回你的请求头

    # 发送请求，并设置stream=True以流式下载
    with requests.get(mp4_url, headers=headers, stream=True) as response:
        response.raise_for_status()  # 如果请求失败，抛出HTTPError异常

        # 尝试获取文件大小
        total_length = int(response.headers.get('content-length', 0))

        # 使用tqdm来显示进度条
        with open(file_path, 'wb') as file, tqdm(total=total_length, unit='B', unit_scale=True, unit_divisor=1024,
                                                 desc=file_path) as progress_bar:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
                    progress_bar.update(len(chunk))  # 更新进度条


def spider_pixabay(page, keyword):
    while True:
        try:
            page += 1
            # 创建一个新的webdriver实例
            driver = webdriver.Chrome(service=s, options=options)
            # 访问网页

            driver.get(f'https://pixabay.com/zh/videos/search/{key_word}/?pagi={page}')
            # 设置显式等待时间
            time.sleep(20)
            xpath_selector = '//*[@id="app"]/div[1]/div/div[2]/div[3]/div/div/div/div/div/a'
            #               //*[@id="app"]/div[1]/div/div[2]/div[3]/div/div/div/div[3]/div/a
            #               //*[@id="app"]/div[1]/div/div[2]/div[3]/div/div/div[2]/div[1]/div/a
            #               //*[@id="app"]/div[1]/div/div[2]/div[3]/div/div/div[1]/div[1]/script
            # 查找元素
            try:
                element = driver.find_elements(By.XPATH, xpath_selector)
                # 获取src属性
                count = 0
                video_details = []
                if element == []:
                    logger.debug(datetime.datetime.now(), f'{key_word}获取完毕')
                for info in element:
                    try:
                        count += 1
                        logger.debug(f"正在获取{key_word}第{page}页，第{count}个视频")
                        src = info.get_attribute('href')
                        driver2 = webdriver.Chrome(service=s)
                        driver2.get(src)
                        time.sleep(20)
                        xpath_selector_2 = '//*[@id="vjs_video_3_html5_api"]'
                        #                   /html/body/div[1]/div[1]/div/div/div/div[1]/div[2]/div/div[1]/div/video-js/video
                        element_2 = driver2.find_element(By.XPATH, xpath_selector_2)
                        src_2 = element_2.get_attribute('src')
                        # logger.debug(src_2)
                        video_detail = {
                            'lanmu': 'pixabay',
                            'name': f'page{page}_{count}',
                            'url': src_2
                        }
                        # 创建文件夹
                        folder_name = fr"E:\未处理\特效\采集源数据（袁）\pixabay\{key_word}"
                        os.makedirs(folder_name, exist_ok=True)
                        file_path = os.path.join(folder_name, f"{page}_{count}.mp4")
                        if os.path.exists(file_path):
                            logger.debug(f"文件已存在: {file_path}, 跳过下载。")

                            # 检查视频是否存在
                        else:
                            try:
                                # with requests.get(src_2, headers=headers, stream=True) as response:
                                #     response.raise_for_status()  # 如果请求失败，将抛出HTTPError异常
                                #     with open(file_path, 'wb') as file:
                                #         for chunk in response.iter_content(chunk_size=8192):
                                #             if chunk:  # 过滤掉keep-alive new chunks
                                #                 file.write(chunk)
                                download_file(src_2, file_path)
                                logger.debug(f"成功下载 {page}_{count}.mp4 到 {file_path}")
                                time.sleep(3)
                            except requests.RequestException as e:
                                logger.debug(f"下载文件时发生错误: {e}")
                                driver2.close()
                                # 如果下载失败，删除已创建的文件（如果存在）
                                if os.path.exists(file_path):
                                    os.remove(file_path)
                                    logger.debug(f"已删除因错误而创建的文件: {file_path}")

                        driver2.close()
                    except Exception as e:
                        logger.debug(f"发生错误:{e}")
                        try:
                            driver2.close()
                        except Exception as e:
                            logger.debug(datetime.datetime.now(), f'发生错误1')
                write_to_csv(csv_file_path, video_details)
                driver.close()
            except Exception as e:
                logger.debug(f"无法找到元素或获取属性: {e}")
        except Exception as e:
            logger.debug("错误：", str(e))


if __name__ == '__main__':
    csv_file_path = 'pixabay.csv'

    # 指定chromedriver的路径
    s = Service(chrome_path)
    # ,options=chrome_options

    key_word = [
        "low angle nature",
        "low angle portrait",
        "low angle landscape"
    ]
    page = 0
    for key in key_word:
        spider_pixabay(page, key)
