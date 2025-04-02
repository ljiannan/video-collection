#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/3/14 10:36
# @Author  : CUI liuliu
# @File    : mixlink_update.py
import logging
import re
import mysql.connector
from urllib.parse import urlparse

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('link_updater.log'),
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


def process_download_link(original_url):
    """处理下载链接，将2160替换为1440"""
    try:
        # 使用正则表达式精确替换
        new_url = re.sub(r'(\d+)-2160\.mp4$', r'\1-1440.mp4', original_url)

        # 验证替换是否成功
        if new_url == original_url:
            logger.warning(f"No replacement made for: {original_url}")
            return None

        return new_url
    except Exception as e:
        logger.error(f"Error processing URL {original_url}: {str(e)}")
        return None


def update_database_records():
    """更新数据库记录"""
    conn = None
    updated_count = 0

    try:
        # 连接数据库
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # 获取待处理记录
        select_query = """
            SELECT id, download_link 
            FROM mixkit_videos 
            WHERE download_state = FALSE
        """
        cursor.execute(select_query)
        records = cursor.fetchall()

        if not records:
            logger.info("没有需要更新的记录")
            return 0

        logger.info(f"找到 {len(records)} 条待处理记录")

        # 逐条处理
        for record in records:
            try:
                original_url = record['download_link']
                new_url = process_download_link(original_url)

                if not new_url:
                    continue

                # 执行更新
                update_query = """
                    UPDATE mixkit_videos 
                    SET download_link = %s 
                    WHERE id = %s
                """
                cursor.execute(update_query, (new_url, record['id']))
                conn.commit()

                updated_count += 1
                logger.info(f"更新记录 {record['id']} 成功")

            except Exception as e:
                logger.error(f"更新记录 {record['id']} 失败: {str(e)}")
                conn.rollback()
                continue

        return updated_count

    except mysql.connector.Error as e:
        logger.error(f"数据库操作失败: {str(e)}")
        return 0
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


if __name__ == "__main__":
    logger.info("=== 开始更新任务 ===")

    try:
        total_updated = update_database_records()
        logger.info(f"任务完成，共更新 {total_updated} 条记录")

    except Exception as e:
        logger.exception("程序运行出现异常")
    finally:
        logger.info("=== 任务结束 ===")