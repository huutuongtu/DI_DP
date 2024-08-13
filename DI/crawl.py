import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
# Define the URL of the API endpoint
def page(num):
    return f'https://www.wonderbk.com/shop/books?page={num}'
# url = 'https://www.wonderbk.com/shop/books?page=2'
f = open("link1.txt", "a", encoding="utf8")
for i in tqdm(range(2000)):
    url = page(str(i+2000))
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        entry_links = soup.find_all(class_='entry-link')
        for link in entry_links:
            href = link.get('href')
            if href:
                f.write(href + "\n")
    else:
        print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")
    
    # time.sleep(3)
