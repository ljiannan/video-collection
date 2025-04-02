#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/3/13 18:36
# @Author  : CUI liuliu
# @File    : miskit_download.py

import logging
import os
import requests
import mysql.connector
from pathvalidate import sanitize_filename  # 需要安装 pathvalidate

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mixkit_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== 配置参数 ====================
DB_CONFIG = {
    "host": "192.168.10.70",
    "user": "root",
    "password": "zq828079",
    "database": "yunjing"
}
DATAL_ID = "B95"  # 存储硬盘ID
INPUT_PATH = r"F:\mixkit_video_download"  # 固定存储路径
os.makedirs(INPUT_PATH, exist_ok=True)


def get_download_tasks():
    """获取需要下载的任务"""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # 修改查询条件：只获取未下载且未分配datal_id的记录，并包含keywords
        query = """
            SELECT id, title, download_link, keywords 
            FROM mixkit_videos 
            WHERE download_state = FALSE 
              AND datal_id IS NULL
        """
        cursor.execute(query)
        return cursor.fetchall()

    except mysql.connector.Error as e:
        logger.error(f"数据库查询失败: {str(e)}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


def update_download_status(record_id, save_path):
    """更新下载状态"""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        update_query = """
            UPDATE mixkit_videos 
            SET download_state = TRUE,
                save_path = %s,
                datal_id = %s
            WHERE id = %s
        """
        cursor.execute(update_query, (save_path, DATAL_ID, record_id))
        conn.commit()
        logger.info(f"更新记录 {record_id} 状态成功")
        return True
    except mysql.connector.Error as e:
        logger.error(f"更新失败: {str(e)}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


def download_video():
    """执行下载任务"""
    tasks = get_download_tasks()
    if not tasks:
        logger.info("没有待下载任务")
        return

    logger.info(f"开始处理 {len(tasks)} 个下载任务...")

    for task in tasks:
        try:
            # 处理keywords，生成子目录
            keywords = task['keywords']
            if keywords:
                # 使用sanitize_filename清理keywords，避免非法字符
                safe_keywords = sanitize_filename(keywords.replace(',', '_'))  # 将逗号替换为下划线
                sub_dir = safe_keywords
            else:
                sub_dir = "无关键词"  # 如果没有keywords，下载到默认目录

            # 生成保存路径
            save_dir = os.path.join(INPUT_PATH, sub_dir)
            os.makedirs(save_dir, exist_ok=True)

            # 生成安全文件名
            safe_title = sanitize_filename(task['title'])
            filename = f"{safe_title}.mp4"
            save_path = os.path.join(save_dir, filename)

            # 跳过已存在文件
            if os.path.exists(save_path):
                logger.warning(f"文件已存在，跳过下载: {filename}")
                update_download_status(task['id'], save_path)
                continue

            logger.info(f"正在下载 [{task['id']}] {task['download_link']}")

            # 流式下载（支持大文件）
            response = requests.get(task['download_link'], stream=True, timeout=30)
            response.raise_for_status()

            # 分块写入文件
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                    if chunk:
                        f.write(chunk)

            # 验证文件大小
            if os.path.getsize(save_path) == 0:
                raise ValueError("下载文件为空")

            # 更新数据库
            if update_download_status(task['id'], save_path):
                logger.info(f"成功保存到: {save_path}")
            else:
                logger.warning(f"下载成功但数据库更新失败: {task['id']}")

        except Exception as e:
            logger.error(f"下载失败 [{task['id']}]: {str(e)}")
            # 清理无效文件
            if 'save_path' in locals() and os.path.exists(save_path):
                os.remove(save_path)
            continue


if __name__ == "__main__":
    # 自动创建存储目录
    if not os.path.exists(INPUT_PATH):
        os.makedirs(INPUT_PATH, exist_ok=True)
        logger.info(f"创建存储目录: {INPUT_PATH}")

    download_video()
    logger.info("所有下载任务处理完成")