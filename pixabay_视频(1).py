import datetime
import logging
import logging.handlers
import os
import csv
import time
from urllib.parse import unquote
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent  # Corrected import


# --- Logging Configuration ---
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "pixabay_download_log.log")
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5MB
BACKUP_COUNT = 5

os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT
)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# --- End Logging Configuration ---


def get_ua():
    ua = UserAgent()
    #  Use a specific user agent string, or ua.random for a random one.
    headers = {"User-Agent": ua.chrome}  #  Or use a specific, valid User-Agent.
    return headers


def write_to_csv(csv_file_path, video_details):
    header_written = False
    try:
        with open(csv_file_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['lanmu', 'name', 'url'])
            if os.path.getsize(csv_file_path) == 0 or not header_written:
                writer.writeheader()
                header_written = True

            writer.writerows(video_details)  # More efficient for writing multiple rows
        logger.info(f"Data saved to: {csv_file_path}")
    except Exception as e:
        logger.exception(f"Error writing to CSV: {e}")


def download_file(mp4_url, file_path):
    headers = get_ua()
    try:
        with requests.get(mp4_url, headers=headers, stream=True, timeout=60) as response:
            response.raise_for_status()
            total_length = int(response.headers.get('content-length', 0))

            with open(file_path, 'wb') as file, tqdm(
                total=total_length, unit='B', unit_scale=True, unit_divisor=1024, desc=file_path
            ) as progress_bar:
                for chunk in response.iter_content(chunk_size=8192 * 4): # increased chunk size
                    if chunk:
                        file.write(chunk)
                        progress_bar.update(len(chunk))
    except requests.exceptions.RequestException as e:
        logger.error(f"Download error for {mp4_url}: {e}")
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted incomplete file: {file_path}")
        return False  # Indicate download failure
    except Exception as e:
        logger.exception(f"Unexpected error during download of {mp4_url}: {e}")
        return False
    return True

# --- Main Script ---

csv_file_path = 'pixabay.csv'
service = Service(r"c:\chrom driver\chromedriver.exe")  # Corrected variable name

key_word = '自然风光'
page = 0
MAX_RETRIES = 3
RETRY_DELAY = 5

while True:
    page += 1
    retries = 0
    while retries < MAX_RETRIES:
        try:
            options = Options()
            # options.add_argument("--headless")  # Consider using headless mode
            options.page_load_strategy = 'eager'  #  'eager' or 'none' can speed up loading
            # Add proxy and user-agent here if needed, outside the loop for efficiency.
            # options.add_argument(f"--proxy-server={proxy_address}") # Example, if you have proxies
            # options.add_argument(f"user-agent={get_ua()['User-Agent']}")

            driver = webdriver.Chrome(service=service, options=options)
            driver.get(f'https://pixabay.com/zh/videos/search/{key_word}/?pagi={page}')
            time.sleep(10)  # Reduced initial wait, rely more on explicit waits below

            xpath_selector ='//*[@id="app"]/div[1]/div/div[2]/div[3]/div/div/div/div/div/a'
            elements = driver.find_elements(By.XPATH, xpath_selector)

            if not elements:
                logger.info(f'{key_word} - Page {page}: No video elements found.  Ending scrape.')
                driver.quit()  # Always quit the driver
                break  # Exit the outer loop

            video_details = []
            count = 0
            for element in elements:
                try:
                    count += 1
                    logger.info(f"Fetching video {count} on page {page} for '{key_word}'")
                    src = element.get_attribute('href')

                    # Use a fresh driver instance for each video detail page
                    driver2 = webdriver.Chrome(service=service, options=options) # Reuse options
                    driver2.get(src)
                    time.sleep(5) # shorter sleep

                    xpath_selector_2 = '//*[@id="vjs_video_3_html5_api"]'
                    element_2 = driver2.find_element(By.XPATH, xpath_selector_2)
                    src_2 = element_2.get_attribute('src')

                    if not src_2:
                        logger.warning(f"Video source URL not found for video {count} on page {page}")
                        driver2.quit()
                        continue


                    video_detail = {
                        'lanmu': 'pixabay',
                        'name': f'page{page}_{count}',
                        'url': src_2
                    }
                    video_details.append(video_detail) #append *before* downloading

                    folder_name = fr"E:\未处理\特效\采集源数据（袁）\pixabay\{key_word}"
                    os.makedirs(folder_name, exist_ok=True)
                    file_path = os.path.join(folder_name, f"{page}_{count}.mp4")

                    if os.path.exists(file_path):
                        logger.info(f"File already exists: {file_path}, skipping download.")
                    else:
                        if download_file(src_2, file_path): # download and check result
                            logger.info(f"Successfully downloaded {page}_{count}.mp4 to {file_path}")
                        time.sleep(1) # Short delay

                    driver2.quit() # close driver2

                except Exception as e:
                    logger.exception(f"Error processing video {count} on page {page}: {e}")
                    if 'driver2' in locals():  # Check if driver2 was created
                        driver2.quit()

            write_to_csv(csv_file_path, video_details)
            driver.quit()
            break  #  Exit retry loop on success

        except Exception as e:
            retries += 1
            logger.warning(f"Page {page} access failed (attempt {retries}/{MAX_RETRIES}): {e}")
            if 'driver' in locals(): # check if driver was created
                driver.quit()
            if retries < MAX_RETRIES:
                delay = RETRY_DELAY * (2 ** (retries - 1))
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Max retries reached for page {page}. Skipping.")
                break  # Give up on this page

    if not elements: # Check if elements is empty to break outer loop
         break