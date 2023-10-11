import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from datetime import datetime
from psycopg2 import sql
import psycopg2
import telegram
import asyncio
import aiohttp

# Парсер
ua = UserAgent()

TOKEN = '6234596308:AAHVM9iuVhOdL2LQbqjK9-lWNKApUjnlo8w'

bot = telegram.Bot(token=TOKEN)

chat_id = '773661341'

url = "https://www.vedomosti.ru/rubrics/economics"

base_url = "https://www.vedomosti.ru"

data_list = []

async def scrape_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()

    soup = BeautifulSoup(html, "lxml")

    # фильтрация бесполезной информации
    for element in soup.find_all('div', class_='card-infoblock__title'):
        element.decompose()

    for element in soup.find_all('div', class_='card-infoblock__header'):
        element.decompose()

    for element in soup.find_all('div', class_='the_projects'):
        element.decompose()  

    for element in soup.find_all('div', class_='card-infoblock__row card-infoblock__read-more'):
        element.decompose()

    for element in soup.find_all('div', class_='card-story__content'):
        element.decompose()          

    info = soup.find_all('a', attrs={'data-vr-title': True, 'href': True})

    # блок с обычной информацией

    for element in info:
        data_vr_title = element['data-vr-title']
        href = element['href']

        data_list.append({'Title': data_vr_title, 'URL': base_url + href})

    # конечный блок с отдельным тегом зачем-то

    cards = soup.find_all("div", class_="card-news__article")

    for element in cards:
        text = element.find("a").text.strip()
        href = element.find("a")["href"]
        
        data_list.append({'Title': text, 'URL': base_url + href})

    return data_list

    # парсинг информации по спаршенным адресам

async def scrap_scrap(data_list):
    async with aiohttp.ClientSession() as session:
        for page in data_list:
            urla = page['URL']
            async with session.get(urla) as response:
                html = await response.text()
            page_soup = BeautifulSoup(html, 'lxml')

            for element in page_soup.find_all('div', class_='box-paywall'):
                element.decompose()
            
            content_element = page_soup.find("div", class_="article-boxes-list article__boxes")
            if content_element:
                contenttext = content_element.get_text().replace('\xa0', '').replace('\n', '').replace('\xad', '').replace('      ', '').strip()
            else:
                contenttext = ''

            times_str = (page_soup.find("time", class_="article-meta__date"))["datetime"]

            timestamp = datetime.fromisoformat(times_str)

            page['Content'] = contenttext

            page['TimeStamp'] = timestamp

    return data_list
 
# РАБОТА С БД 
async def database(data_list):
    conn = psycopg2.connect(
        dbname="postgres", 
        user="postgres", 
        password="12",    
        host="localhost"  
    )

    cur = conn.cursor()

    # Создание таблицы если отсутствует
    table_name = "tz"
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            ID SERIAL PRIMARY KEY,
            URL VARCHAR(200) UNIQUE,
            TITLE VARCHAR(200),
            CONTENT TEXT,
            CREATED_AT TIMESTAMP
        )
    """).format(sql.Identifier(table_name))

    cur.execute(create_table_query)

    conn.commit()

    count_b4_parsing_query = sql.SQL("""SELECT COUNT(*) FROM {}
            """).format(sql.Identifier(table_name))

    cur.execute(count_b4_parsing_query)

    count_b4_parsing = cur.fetchone()[0]

    for page in data_list:
        urla = page['URL']
        title = page['Title']
        content = page['Content']
        timestamp = page['TimeStamp']

        insert_query = sql.SQL("""
            INSERT INTO {} (URL, TITLE, CONTENT, CREATED_AT)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (URL) DO NOTHING;
        """).format(sql.Identifier(table_name))

        cur.execute(insert_query, (urla, title, content, timestamp))
        conn.commit()

    count_after_parsing_query = sql.SQL("""SELECT COUNT(*) FROM {}
            """).format(sql.Identifier(table_name))

    cur.execute(count_after_parsing_query)

    count_after_parsing = cur.fetchone()[0]

    conn.close()
    cur.close()

    return count_b4_parsing, count_after_parsing

async def send_message(count_b4_parsing, count_after_parsing):
    await bot.send_message(chat_id, text=f'Количество записей до парсинга: {count_b4_parsing}\nКоличество записей после парсинга: {count_after_parsing}')

async def main():
    while True:
        data_list = await scrape_data(url)
        data_list = await scrap_scrap(data_list)
        count_b4_parsing, count_after_parsing = await database(data_list)
        await send_message(count_b4_parsing, count_after_parsing) 
        await asyncio.sleep(86400) 

if __name__ == '__main__':
    asyncio.run(main())