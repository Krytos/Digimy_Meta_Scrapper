import asyncio
import csv
import json
import os
from time import perf_counter

import aiohttp
import streamlit as st
from bs4 import BeautifulSoup


async def fetch_metadata(session, item_link: str, data):
    try:
        async with session.get(item_link) as response:
            soup = BeautifulSoup(await response.text(), 'html.parser')
            page_title = soup.find("meta", attrs={"property": "og:title"})["content"].replace("&nbsp;", " ").replace(
                '"',
                "'")
            print(f"Fetching metadata for {page_title}...")
            page_description = soup.find("meta", attrs={"name": "description"})["content"].replace("&nbsp;",
                                                                                                   " ").replace(
                '"', "'")
            produkt_beschreibung = [p.text for p in soup.find('div', class_='vsLeft')]
            produkt_beschreibung = [p.strip().replace("&nbsp;", " ").replace('"', "'") for p in produkt_beschreibung if
                                    p != ' ']
            produkt_beschreibung = " ".join(produkt_beschreibung[1:-3]).replace("&nbsp;", " ").replace("&nbsp", " ")
            data.append(
                {
                    "meta title": page_title,
                    "meta description": page_description,
                    "produkt beschreibung": produkt_beschreibung
                }
            )
            try:
                my_bar.progress(int(100 / total_tasks * (total_tasks - len(asyncio.all_tasks()) + 1)))
            except Exception:
                pass
            await asyncio.sleep(0.01)
    except Exception as e:
        print(e)
        pass


async def fetch_pages_for_link(session, url: str, tg: asyncio, data):
    async with session.get(url) as response:
        print(f"Fetching {url}...")
        soup = BeautifulSoup(await response.text(), 'html.parser')
        try:
            items = soup.find('div', class_='vscenter').contents
            if len(items) > 1:
                for item in [item for item in items if item != ' ']:
                    tg.create_task(fetch_metadata(session, item.find('a')['href'], data))
                    print(f"Created task: Fetchingitem {item.find('a')['href']}")
                    await add_tasks()
            else:
                current_task = asyncio.current_task().get_name()
                for task in [task for task in asyncio.all_tasks() if int(task.get_name()[4:]) > int(current_task[4:])]:
                    task.cancel()
        except AttributeError:
            pass


async def add_tasks(n=1):
    global total_tasks
    total_tasks += n

async def main():
    start = perf_counter()
    data = []
    # with Progress() as progress:
    #     task = progress.add_task("[red]Scrapping...", start=False)
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url) as html:
            soup = BeautifulSoup(await html.text(), 'html.parser')

            if categories_to_scrap == []:
                navigation_list = soup.find('ul', class_='navigation--list container')
                links = [a['href'] for a in navigation_list.find_all('a')]
            else:
                links = []
                for i, category in enumerate(url_extensions[1]):
                    if category in categories_to_scrap:
                        if category == "Rebsorten":
                            async with session.get(base_url + url_extensions[0][i]) as reb:
                                soup = BeautifulSoup(await reb.text(), 'html.parser')
                                sub_links = [a.find("a")["href"] for a in
                                             soup.find_all('div', class_='vscategorylistingitem')]
                                links.extend(sub_links)
                        else:
                            links.append(base_url + url_extensions[0][i])

            async with asyncio.TaskGroup() as tg:
                for link in [link + '?p=' + str(i) for i in range(1, 100) for link in links]:
                    tg.create_task((fetch_pages_for_link(session, link, tg, data)), name=f'task{link.split("?p=")[1]}')
                    print(f"Created task {link.split('?p=')[1]}: Generating link for {link}")
                    await add_tasks()
            with open(f"{datei_name}.json", "w+", encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            with open(f"{datei_name}.csv", "w", encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys(), escapechar='\\', doublequote=False)
                writer.writeheader()
                writer.writerows(data)
            while not os.path.exists(f"{datei_name}.json") and not os.path.exists(f"{datei_name}.csv"):
                await asyncio.sleep(0.1)
    print(f"Finished in {perf_counter() - start} seconds")


total_tasks = 0
base_url = "https://www.vinoscout.de/"
url_extensions = [
    ['rotwein/', 'weisswein/', 'rosewein/', 'schaumwein/', 'portwein/', 'sherry/', 'spirituosen/', 'feinkost/',
     'cidre/', 'alkoholfreie-getraenke/', 'laender/', 'rebsorten/'],
    ["Rotwein", "Weißwein", "Roséwein", "Schaumwein", "Portwein", "Sherry", "Spirituosen", "Feinkost", "Cidre",
     "Alkoholfreie Getränke", "Länder", "Rebsorten"]]
categories_to_scrap = st.multiselect("Kategoregien zu scrappen",
                                     ["Rotwein", "Weißwein", "Roséwein", "Schaumwein", "Portwein", "Sherry",
                                      "Spirituosen", "Feinkost", "Cidre", "Alkoholfreie Getränke", "Länder",
                                      "Rebsorten"])

pages_to_scrap = st.slider("Pages to scrap", 1, 100, 50)
datei_name = st.text_input("Dateiname", "results")
my_bar = st.progress(0)
button = st.button("Start scrapping", on_click=asyncio.run, args=(main(),))

try:
    with open(f"{datei_name}.json", "r", encoding='utf-8') as f:
        st.download_button('Download JSON', f, f"{datei_name}.json")
    with open(f"{datei_name}.csv", "r", encoding='utf-8') as f:
        st.download_button('Download CSV', f, f"{datei_name}.csv")
except FileNotFoundError:
    pass
