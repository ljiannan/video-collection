#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/3/13 17:36
# @Author  : CUI liuliu
# @File    : mixkit_video.py



import logging
import requests
from lxml import etree
import mysql.connector
from datetime import datetime
import time

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mixkit_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== 数据库配置 ====================
DB_CONFIG = {
    "host": "192.168.10.70",
    "user": "root",
    "password": "zq828079",
    "database": "yunjing"
}

keywords = [
    "animal",
    "Nature",
    "Lifestyle",
    "Food",
    "Transport",
    # "Light",
    # "4K"
    # "first personal view",
    # "fixed",
    # "Pan",
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
    # "Drone perspective"
]
# keyword="animal"
page_start=30
# ==================== 请求头配置 ====================
HEADERS = {
        'Accept': 'text/html, application/xhtml+xml',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Cookie': '__cf_bm=Drw.OSEzMf4_UL2P64Du_QQMs1ioczHzrLMgHSu4hkg-1741859330-1.0.1.1-OO5mcPnnNt7H8U_ICfSzXwVyDc8knqYGBrg3N0VFi3nVfycqHO4wlMB4ud4KST.DMKFCpYAAYZ6xH60VQDsPx9wrahUg99Rc5chkpxDsMLA; CookieConsent={stamp:%27hJEdgU/jEFEJFFhX0xA2+tJffzrTG9PBRXlVW8uhbrskpnk5ALWObw==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:1%2Cutc:1741859336134%2Cregion:%27ca%27}; algolia-user-token=7fc65b27483e47bae47139ba46db98dc; _ga=GA1.1.2020846797.1741859339; _fbp=fb.1.1741859339150.553994311304994010; _ga_HD6V8WBY2G=GS1.1.1741859339.1.1.1741859406.0.0.0',
        'Priority': 'u=1, i',
        'Referer': 'https://mixkit.co/free-stock-video/discover/zoom-in/',
        'Sec-CH-UA': '"Chromium";v="134", "Not:A-Brand";v="24", "Microsoft Edge";v="134"',
        'Sec-CH-UA-Mobile': '?0',
        'Sec-CH-UA-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0'
    }


def init_database():
    """初始化数据库表结构"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS mixkit_videos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_url VARCHAR(512) UNIQUE,
                title VARCHAR(255),
                download_link VARCHAR(512),
                download_state BOOLEAN DEFAULT FALSE,
                keywords VARCHAR(255) DEFAULT NULL,
                save_path VARCHAR(512) DEFAULT NULL,
                datal_id VARCHAR(64) DEFAULT NULL,
                created_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        logger.info("Database initialized successfully")
    except mysql.connector.Error as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def process_download_link(original_src):
    """处理下载链接生成高清版本"""
    try:
        filename = original_src.split("/")[-1]
        base_part = filename.split("-")[0]
        new_filename = f"{base_part}-2160.mp4"
        return original_src.replace(filename, new_filename)
    except Exception as e:
        logger.error(f"Error processing download link: {str(e)}")
        return None


def save_to_database(data):
    """保存数据到数据库"""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO mixkit_videos 
            (video_url, title, download_link, keywords)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            title=VALUES(title), 
            download_link=VALUES(download_link),
            keywords=VALUES(keywords)
        """

        cursor.execute(insert_query, (
            data['video_url'],
            data['title'],
            data['download_link'],
            data['keywords']
        ))

        conn.commit()
        logger.info(f"Inserted/Updated record: {data['video_url']}")
        return True
    except mysql.connector.Error as e:
        logger.error(f"Database operation failed: {str(e)}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


def scrape_page(url,keyword):
    """抓取单个页面"""
    try:
        logger.info(f"Start scraping page: {url}")
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()

        tree = etree.HTML(response.text)
        video_items = tree.xpath('//div[@class="item-grid__item"]')
        logger.info(f"Found {len(video_items)} video items")

        results = []

        for item in video_items:
            try:
                video_tag = item.xpath('.//div[3]/a')[0]
                video_url = "https://mixkit.co" + video_tag.get('href')
                title = video_tag.text.strip()

                video_element = item.xpath('.//div[2]/video')[0]
                original_src = video_element.get('src')

                download_link = process_download_link(original_src)
                if not download_link:
                    continue

                data = {
                    'video_url': video_url,
                    'title': title,
                    'download_link': download_link,
                    'keywords':keyword
                }

                if save_to_database(data):
                    results.append(data)

                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error processing item: {str(e)}")
                continue

        return results

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        return []
    except etree.XPathError as e:
        logger.error(f"XPath parsing error: {str(e)}")
        return []


if __name__ == "__main__":
    init_database()
    for keyword in keywords:
        for i in range(page_start,page_start+300):
            target_url = f"https://mixkit.co/free-stock-video/{keyword}/?page={i}"
            logger.info(f"Start scrapy page{i}")

            start_time = datetime.now()
            scraped_data = scrape_page(target_url,keyword)
            duration = datetime.now() - start_time

            logger.info(f"Scraping completed. Total items: {len(scraped_data)}. Time taken: {duration}")