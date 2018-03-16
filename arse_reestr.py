import csv
import asyncio
import aiohttp
import html5lib
import sqlite3
from bs4 import BeautifulSoup

#http://www.drlz.com.ua/ibp/ddsite.nsf/all/shlz1?opendocument&stype=C249772EDC8AA958C2257E2D00278470

def parse_res(res, url):
    soup = BeautifulSoup(res.decode('cp1251'), 'html5lib')
    tables = soup.findAll('table')[3].findAll('table')
    children = tables[3].findAll('tr')[1:]
    #print(len(children))
    value_dict = {}
    value_dict['dz_url'] = url
    for tr in children:
        if 'Торгівельне найменування' in tr.text:
            value_dict['name'] = tr.text.split(":")[1]
        elif 'Виробник' in tr.text:
            value_dict['manufacturer'] = tr.text.split(":")[1]
        elif 'Форма випуску' in tr.text:
            value_dict['release_type'] = tr.text.split(":")[1]
        elif 'Міжнародне непатентоване' in tr.text:
            value_dict['popular_name'] = tr.text.split(":")[1]
        elif 'Склад діючих' in tr.text:
            value_dict['composition'] = tr.text.split(":")[1]
        elif 'Умови відпуску' in tr.text:
            value_dict['prescription'] = tr.text.split(":")[1]
        elif 'Термін придатності' in tr.text:
            value_dict['expiration_date'] = tr.text.split(":")[1]
    with sqlite3.connect('parsed.db') as conn:
        keys = ','.join(value_dict.keys())
        question_marks = ','.join(list('?'*len(value_dict)))
        values = tuple(value_dict.values())
        sql = 'INSERT INTO drugs('+keys+') VALUES('+question_marks+')'
        conn.execute(sql, values)

async def read_website(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            res = await response.read()
            try:
                parse_res(res, url)
            except Exception as e:
                #print(e.args)
                if 'constraint' not in e.args[0]:
                    print(url)

def parse_part(urls):
    loop = asyncio.get_event_loop()
    for url in urls:
        loop.create_task(read_website(url))
    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))

if __name__ == "__main__":
    with open('reestr.csv', encoding='latin-1') as csvfile:
        reestrreader = csv.DictReader(csvfile, delimiter=';')
        urls = [ 'http://www.drlz.com.ua/ibp/ddsite.nsf/all/shlz1?opendocument&stype='+row['ID'] for row in reestrreader ]
        for i in range(0, len(urls), 30):
            print("index parse: " + str(i))
            parse_part(urls[i-10:i])
        parse_part(urls[len(urls)-30:])
