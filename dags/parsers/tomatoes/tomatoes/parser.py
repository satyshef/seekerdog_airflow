import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Отправляем GET-запрос к веб-странице
def parse_movies_list():
    url_base = "https://www.rottentomatoes.com"
    #url = "https://www.rottentomatoes.com/browse/movies_in_theaters/ratings:pg_13~sort:newest"
    #url = "https://www.rottentomatoes.com/browse/movies_coming_soon/ratings:pg_13,r~sort:newest"
    url = "https://www.rottentomatoes.com/browse/movies_coming_soon"
    result = []

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
    
        title = soup.title.text
        print("Page title:", title)
        links = soup.find_all("a")
        for link in links:
            link = link.get("href")
            if link != None and link.startswith("/m/"):
                link = url_base + link 
                result.append(link)
        return result
    else:
        print("Load movies list error:", response.status_code)


def parse_movies_info(url):
    response = requests.get(url)
    if response.status_code == 200:
        parsed_url = urlparse(url)
        id = parsed_url.path.rsplit('/', 1)[-1]
        soup = BeautifulSoup(response.content, "html.parser")
        name = extract_movie_name(soup)
        description = extract_movie_description(soup)
        info = extract_movie_info(soup)
        url_image = extract_movie_image(soup)

        result = {
            "id": id,
            "name": name,
            "url_tomat": url,
            "url_image": url_image,
            "description": description,
            "info": info
        }
        return result
    else:
        print("Load movie info error:", response.status_code)

def extract_movie_name(soup):
    h1_tag = soup.find('h1')
    text = h1_tag.text.strip()
    return text


def extract_movie_description(soup):
    p_tag = soup.find("p", attrs={"data-qa": "movie-info-synopsis", "slot": "content"})
    if p_tag:
        synopsis = p_tag.get_text(strip=True)
        return synopsis
    else:
        print("Move description not found")


def extract_movie_info(soup):
    ul_tags = soup.find_all("ul", id="info")
    result = {}
    # Перебираем найденные теги <ul>
    for ul_tag in ul_tags:
        for li_tag in ul_tag.find_all("li", class_="info-item"):
            #info = li_tag.get_text(separator=" ", strip=True)
            info = li_tag.get_text(strip=True)
            info = info.replace("\n","")
            info = info.replace("  ","")
            elements = info.split(":", 1)
            if len(elements) == 2:
                key = elements[0].strip()
                value = elements[1].strip()
                result[key] = value
                
    
    return result


def extract_movie_image(soup):
    hero_image_block = soup.find('tile-dynamic', id="hero-image")

    # Если блок найден, извлекаем из него ссылки на изображения
    if hero_image_block:
        img_tags = hero_image_block.find_all('img')
        image_urls = []
        for img_tag in img_tags:
            srcset = img_tag.get('srcset')
            if srcset:
                # Используем регулярное выражение для извлечения ссылки на изображение
                matches = re.findall(r'https://[^\s,]+', srcset)
                if matches:
                    image_urls.extend(matches)


        if len(image_urls) != 0:
            return image_urls[-1].strip()
    
    return "null"
