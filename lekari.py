# coding: utf-8
from id_codes import obor_codes, okres_codes
from bs4 import BeautifulSoup
import itertools
import pandas as pd
import requests
import time
import pdb

def parsePage(html):
    """
        Parse a web page and get links for other listings.
    """

    soup = BeautifulSoup(x.text, "html.parser")
    links = soup.find_all("a")
    links = [link.attrs['href'] for link in links if 'href' in link.attrs]
    more_page_links = filter( lambda l: l.startswith("/seznam-lekaru-426.html?paging.pageNo="), links )
    return more_page_links

def parseRequest(html, obor, okres):
    """
        Parse a web page and find physician names and links for given medical field and location.
    """

    names, municipalities, links, obory, okresy = [], [], [], [], []
    soup = BeautifulSoup(x.text, "html.parser")
    for table in soup.find_all('table', class_='seznam2'):
        for line in table.find_all("tr"):
            cells = line.find_all("td")
            if cells:
                #print(cells)
                name = cells[0].text.strip()
                if name:
                    #TODO prazdne radky jako druhe mesto
                    municipality = cells[1].text.strip()
                    link_ids = cells[2].find_all("a")
                    if link_ids: link_id = link_ids[0].attrs["href"]
                    else: link_id = ""
                    names.append(name)
                    municipalities.append(municipality)
                    links.append(link_id)
                    obory.append(obor)
                    okresy.append(okres)
    return pd.DataFrame( {"name": names, "municipality": municipalities, "link": links, "medical_field": obory, "location": okresy} )

url = "https://www.lkcr.cz/seznam-lekaru-426.html#seznam"
url_base = "https://www.lkcr.cz"
dfs = []
sleep_interval = 3*60

for obor_code, okres_code in itertools.product( obor_codes.keys(), okres_codes.keys() ):
    data = {"filterObor": obor_code, "filterOkresId": okres_code }
    obor, okres = obor_codes[obor_code], okres_codes[okres_code]
    x = requests.post(url, data = data )
    pages = parsePage( x.text )
    if pages:
        for page in pages:
            print(url_base+page, obor, okres)
            x = requests.post(url_base + page, data = data )
            df = parseRequest( x.text, obor, okres )
            dfs.append(df)
            pd.concat( dfs ).to_csv("lekari.csv")
            time.sleep( sleep_interval )
    else:
        print(url, obor, okres)
        df = parseRequest( x.text, obor, okres )
        dfs.append(df)
        pd.concat( dfs ).reset_index(drop = True).to_csv("lekari.csv")
        time.sleep( sleep_interval )
