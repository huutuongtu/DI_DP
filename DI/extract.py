import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import pandas as pd

f = open("link1.txt", "r", encoding="utf8").readlines()
abc = open("wonderbk1.txt", "a", encoding="utf8")


for url in tqdm(f):
    try:
        response = requests.get(url.strip())
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            html_content = response.text
            # print(html_content)
            # try:
            soup = BeautifulSoup(html_content, 'html.parser')
            p_tag = soup.find('p', class_='entry-details-isbn')
            #title
            title_tag = soup.find(class_='h2 entry-title')
            title = title_tag.get_text(strip=True)
            author_tag = soup.find(class_='author vcard')
            author_tag = author_tag.get_text(strip=True)
            #author
            author = author_tag[2:].strip()
            #isbn
            isbn_text = p_tag.get_text(strip=True)
            isbn = isbn_text.split('ISBN: ')[1].split(' /')[0].strip()

            # Extract the publisher
            publisher = p_tag.find('a').get_text(strip=True)

            # Extract the year
            year_text = p_tag.get_text(strip=True)
            year = year_text.split(',')[1].split()[1].strip()
            abc.write(isbn + ";" + publisher + ";" + year + ";" + author + ";" + title + "\n")

    except:
        continue
