import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from datetime import datetime
import psycopg2
from psycopg2 import sql
import telegram
import asyncio
import aiohttp

# Парсер
ua = UserAgent()

TOKEN = '6234596308:AAHVM9iuVhOdL2LQbqjK9-lWNKApUjnlo8w'

bot = telegram.Bot(token=TOKEN)

chat_id = '773661341'

headers = requests.utils.default_headers()
headers.update(
    {
        'User-Agent': ua.random,
    }
)

url = "https://www.vedomosti.ru/rubrics/economics"

base_url = "https://www.vedomosti.ru"

data_list = []

# фильтрация бесполезной для меня информации

responce = requests.get(url)

soup = BeautifulSoup(responce.text, "lxml")

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

# блок с обычной инфой

for element in info:
    data_vr_title = element['data-vr-title']
    href = element['href']

    data_list.append({'Title': data_vr_title, 'URL': base_url + href})

# конченный блок с отедельным тегом зачем-то

cards = soup.find_all("div", class_="card-news__article")

for element in cards:
    text = element.find("a").text.strip()
    href = element.find("a")["href"]
    
    data_list.append({'Title': text, 'URL':base_url + href})

# парсинг информации по спаршенным адресам

for page in data_list:
    urla = page['URL']
    response = requests.get(urla)
    page_soup = BeautifulSoup(response.content, 'lxml')

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
 
# РАБОТА С БД 

# подключение к базе данных
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

async def send_message():
    while True:
        await bot.send_message(chat_id, text=f'Количество записей до парсинга: {count_b4_parsing}\nКоличество записей после парсинга: {count_after_parsing}')
        await asyncio.sleep(10800)
asyncio.run(send_message())
