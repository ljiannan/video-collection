import re
import requests
import os
import csv
from fake_useragent import UserAgent
from datetime import datetime
from lxml import etree
import time
from concurrent.futures import ThreadPoolExecutor

page = 0
while True:
    try:
        page += 1
        url = f'https://www.airvuz.com/search?q=Aerial&amp;type=video&amp;sort=relevance&amp;id=NMfS0ysDH&amp;page=1&page={page}'
        headers = {
            'authority': 'www.airvuz.com',
            'method': 'GET',
            'path': f'/search?q=Aerial&type=video&sort=relevance&id=NMfS0ysDH&page={page}',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'cache-control': 'max-age=0',
            'cookie': '_ga=GA1.2.1832422736.1729821548; _gid=GA1.2.1903585575.1729821548; _fbp=fb.1.1729821548403.375910764691003156; _gat=1',
            'if-none-match': 'W/"2982b-9JU0/Qk/zI9anfpk8MFX++rWH9c"',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Chromium";v="130", "Microsoft Edge";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
        }
        resp = requests.get(url, headers=headers)
        resp.encoding = resp.apparent_encoding
        if resp.status_code != 200:
            print(datetime.now(), f'page{page}访问失败，请检查网络链接')
            time.sleep(300)
            continue
        html = etree.HTML(resp.text)
        ret = html.xpath(r'/html/body/av-app/div/main/av-search/div/div[3]/div/div/div[1]/div/video-thumbnail-list/div/div/video-thumbnail/div/a')
        if ret != []:
            print(datetime.now(), f'page{page}页访问成功')
            info_list = []
            count = 0
            for info in ret:
                count += 1
                dic = {
                    'count': count,
                    'info': info
                }
                info_list.append(dic)
            # for info in ret:
            def get_and_down(info_dic):
                try:
                    info = info_dic.get('info')
                    count = info_dic.get('count')
                    href = info.get('href')

                    url_2 = f'https://www.airvuz.com{href}'
                    id = href.split('id=')[1]
                    resp_2 = requests.get(url_2, headers=headers)
                    resp_2.encoding = resp_2.apparent_encoding
                    print(datetime.now(), f'正在获取page{page}页，第{count}个视频')

                    obj = re.compile(r'og:video:url.*?content=(?P<url_mp4>.*?)>', re.S)
                    match = obj.search(resp_2.text)
                    mp4_url = match.group('url_mp4')

                    # 创建文件夹
                    folder_name = fr"E:\未处理\航拍\袁采集源数据\airvuz"
                    os.makedirs(folder_name, exist_ok=True)
                    file_path = os.path.join(folder_name, f"{id}.mp4")
                    if os.path.exists(file_path):
                        print(f"文件已存在: {file_path}, 跳过下载。")
                    else:
                        with requests.get(mp4_url, headers=headers, stream=True) as response:
                            response.raise_for_status()  # 如果请求失败，将抛出HTTPError异常
                            with open(file_path, 'wb') as file:
                                for chunk in response.iter_content(chunk_size=8192):
                                    if chunk:  # 过滤掉keep-alive new chunks
                                        file.write(chunk)
                        print(datetime.now(), f'视频已成功保存到{file_path}')
                except Exception as e:
                    print(datetime.now(), f'发生错误1:{e}')


            with ThreadPoolExecutor(max_workers=5) as executor:  # 假设使用5个线程
                dfs = executor.map(get_and_down, info_list)
            print(datetime.now(), f'page{page}页获取完毕')

        else:
            print(datetime.now(), f'航拍获取完毕，跳出循环，共计{page}页')
            break
    except Exception as e:
        print(datetime.now(), f'发生错误0:{e}')