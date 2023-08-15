import sqlite3 as sq
import requests
from bs4 import BeautifulSoup
from time import sleep

def sql_add(data):
    try:
        with sq.connect('olx_flat.db') as con:
            cur = con.cursor()

            cur.execute('DROP TABLE IF EXISTS flats')

            cur.execute("""
            CREATE TABLE flats (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                title TEXT,
                price INTEGER,
                location TEXT,
                time TEXT,
                link TEXT
            )
            """)

            cur.executemany("""INSERT INTO flats(title,price,location,time,link) VALUES(?,?,?,?,?)""", data)
            cur.execute("""UPDATE flats SET price = REPLACE(price, ' Договірна', '') WHERE price LIKE '% Договірна'""")
            cur.execute("""UPDATE flats SET price = REPLACE(price," ","")""")
            cur.execute("""SELECT * FROM flats  ORDER BY location""")
            result = cur.fetchall()
            for flat in result:
                print(flat[0], flat[1], flat[2], flat[3], flat[4], flat[5])

            con.commit()
    except sq.Error as e:
        print(f'Error occurred: {e}')

def parse_pages():
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
    }

    for page in range(1, 26):
        url = f'https://www.olx.ua/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/kiev/?currency=UAH&page={page}'
        response = requests.get(url, headers=headers)
        sleep(3)
        soup = BeautifulSoup(response.text, 'lxml')
        yield soup

def derive_info(soup):
    info = []
    for proposal in soup.find_all('div', class_='css-1sw7q4x'):
        link_element = proposal.find('a', class_='css-rc5s2u')
        if link_element:
            prop_url = "https://www.olx.ua" + link_element.get('href')
            title = proposal.find('h6', class_='css-16v5mdi er34gjf0').text.strip()
            price_el = proposal.find('p', class_='css-10b0gli er34gjf0').text.strip()
            price = price_el.replace("грн.", "")
            location_element = proposal.find('p', class_='css-veheph er34gjf0')
            if location_element:
                location_text = location_element.text.strip()
                location = location_text.split(' - ')[0]
                time = location_text.split(' - ')[1]
                info.append((title, price, location, time, prop_url))
            else:
                info.append((title, price, "-", "-", prop_url))
    return info


all_info = []
count = 0
for soup in parse_pages():
    count += 1
    info = derive_info(soup)
    if info:
        all_info.extend(info)
    print(f'{count - 1} page(s) processed')

if all_info:
    sql_add(all_info)

print(f'{len(all_info)} flats processed and added to the database')