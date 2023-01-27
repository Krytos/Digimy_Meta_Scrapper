import asyncio
import csv
from time import perf_counter

import aiohttp
import streamlit as st
from bs4 import BeautifulSoup
from rich.progress import Progress


async def fetch_metadata(session, item_link, progress, task):
    async with session.get(item_link) as response:
        soup = BeautifulSoup(await response.text(), 'html.parser')
        page_title = soup.find("meta", attrs={"property": "og:title"})["content"].replace(",", "-").replace("|", "-")
        page_description = soup.find("meta", attrs={"name": "description"})["content"].replace(",", "-").replace("|",
                                                                                                                 "-")
        with open(f"{datei_name}.csv", "a", encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([page_title, page_description])
        progress.start_task(task)
        progress.update(task, advance=1)
        try:
            my_bar.progress(int(100 / total_tasks * (total_tasks - len(asyncio.all_tasks()) + 1)))
        except Exception:
            pass


async def fetch_items(session, url, tg, progress, task):
    global total_tasks
    async with session.get(url) as response:
        soup = BeautifulSoup(await response.text(), 'html.parser')
        listings = soup.find('div', class_='listing')
        try:
            content = [x for x in listings.children][1].contents
        except AttributeError:
            return
        if len(content) == 1:
            return

        for item in content:
            try:
                item_link = item.find('a')['href']
                tg.create_task(fetch_metadata(session, item_link, progress, task))
                await add_tasks()
                progress.update(task, total=total_tasks)
            except TypeError:
                continue


async def fetch_pages_for_link(session, link, tg, progress, task):
    for i in range(1, pages_to_scrap + 1):
        url = link + '?p=' + str(i)
        tg.create_task(fetch_items(session, url, tg, progress, task))
        await add_tasks()
        progress.update(task, total=total_tasks, advance=1)


async def add_tasks():
    global total_tasks
    total_tasks += 1


async def main():
    start = perf_counter()
    with Progress() as progress:
        task = progress.add_task("[red]Scrapping...", start=False)
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
                            links.append(base_url + url_extensions[0][i])

                async with asyncio.TaskGroup() as tg:
                    for link in links:
                        tg.create_task(fetch_pages_for_link(session, link, tg, progress, task))
                        progress.update(task, advance=1)
                    progress.update(task, completed=True)
                    await asyncio.sleep(0.5)
                with open(f"{datei_name}.csv", "r", encoding='utf-8') as f:
                    st.download_button('Download CSV', f, f"{datei_name}.csv")
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
# categories_to_scrap = ["Roséwein"]
pages_to_scrap = st.slider("Pages to scrap", 1, 100, 50)
# pages_to_scrap = 50
datei_name = st.text_input("Dateiname", "results")
my_bar = st.progress(0)
# asyncio.run(main())
button = st.button("Start scrapping", on_click=lambda: asyncio.run(main()))