import aiohttp
import asyncio
import sqlite3
from parsel import Selector
import sys

async def get_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp = await resp.text()
            return resp

def parse(raw_data):
    selector = Selector(text=raw_data)
    data = []
    for it in selector.css("tr.odd, tr.even"):
         row = tuple(map(lambda x: str.strip(x), it.xpath("./td/text()").getall()))
         data.append(row)
    return data

def storage(data):
    with sqlite3.connect("db.sqlite") as connection:
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS entity (nit TEXT, name TEXT, no_users INTEGER)")
        for row in data:
            cursor.execute("INSERT INTO entity (nit, name, no_users) VALUES (?, ?, ?)", row)

async def flow(url):
    data  = parse(await get_data(url))
    storage(data)
    await asyncio.sleep(30)
    return data

async def main():
    URL = "https://operaciones.colombiacompra.gov.co/compradores/beneficios-del-secop-ii-para-compradores/consulte-en-el-secop-ii/entidades-secop-ii"
    # init
    try:
        index = int(sys.argv[1])
    except ValueError, IndexError:
        index = 0

    data  = await flow(URL + f"?page={index}" if index > 0 else "")
    print(f"Last pages: {index}")
    index += 1

    while data != []:
        url = URL + f"?page={index}"
        data = await flow(url)
        print(f"Last pages: {index}")
        index += 1


if __name__ == '__main__':
    asyncio.run(main())
