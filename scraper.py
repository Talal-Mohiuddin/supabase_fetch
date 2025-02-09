import aiohttp
import asyncio
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import json
from supabase import create_client, Client
from datetime import datetime, timedelta
import ssl
import random
import time

# Configuration
BASE_URL = "https://www.boligportal.dk/lejeboliger/"
OFFSET_STEP = 18  # Number of listings per page


url: str = os.getenv('SUPABASE_URL')
key: str = os.getenv('SUPABASE_KEY')
supabase: Client = create_client(url, key)


DANISH_MONTHS = {
    'januar': 1, 'februar': 2, 'marts': 3, 'april': 4, 'maj': 5, 'juni': 6,
    'juli': 7, 'august': 8, 'september': 9, 'oktober': 10, 'november': 11, 'december': 12
}

# Add list of user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
]

# Fetch and parse a page asynchronously
async def fetch_page(session, url):
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # Add random delay between requests
    await asyncio.sleep(random.uniform(2, 5))
    
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.text()
            elif response.status == 403:
                print(f"Rate limited. Waiting 30 seconds before retry...")
                await asyncio.sleep(30)
                return None
            else:
                print(f"Failed to fetch {url}, Status: {response.status}")
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None

# Parse main listing page to extract individual listing URLs
def parse_listing_urls(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    urls = []
    for a_tag in soup.find_all("a", class_="AdCardSrp__Link"):
        href = a_tag.get("href")
        if href:
            urls.append("https://www.boligportal.dk" + href)
    return urls

# Convert "Ja"/"Nej" to boolean
def convert_to_boolean(value):
    if value == "Ja":
        return True
    elif value == "Nej":
        return False
    elif value == "Ikke angivet":
        return False  # Set to None if the value is "Ikke angivet"
    return None  # Return None for other unknown values

def convert_relative_time(time_str):
    current_time = datetime.now()
    
    # Handle date format "DD. month"
    for month in DANISH_MONTHS.keys():
        if month in time_str.lower():
            day = int(time_str.split('.')[0])
            month_num = DANISH_MONTHS[month]
            year = current_time.year
            # If the date is in the future, use last year
            date = datetime(year, month_num, day)
            if date > current_time:
                date = datetime(year - 1, month_num, day)
            return date.strftime("%Y-%m-%d")
    
    if time_str == "I går":
        return (current_time - timedelta(days=1)).strftime("%Y-%m-%d")
    elif 'min.' in time_str:
        minutes = int(time_str.split()[0])
        return (current_time - timedelta(minutes=minutes)).strftime("%Y-%m-%d")
    elif 'time' in time_str:
        hours = int(time_str.split()[0])
        return (current_time - timedelta(hours=hours)).strftime("%Y-%m-%d")
    elif 'dag' in time_str or 'dage' in time_str:  # 
        days = int(time_str.split()[0])
        return (current_time - timedelta(days=days)).strftime("%Y-%m-%d")
    else:
        return time_str


# Parse individual listing pages to extract details
def parse_listing_details(html_content):
    soup = BeautifulSoup(html_content, "html.parser")

    title_tag = soup.find("span", class_="css-v34a4n")
    title = title_tag.get_text(strip=True) if title_tag else "No title found"

    date_tag = soup.find("div", class_="css-o9y6d5")
    published_date = date_tag.get_text(strip=True) if date_tag else "No date found"
    published_date = convert_relative_time(published_date)

    breadcrumb_tags = soup.find_all("a", class_="css-10zxfph")
    city = breadcrumb_tags[2].get_text(strip=True) if len(breadcrumb_tags) > 2 else "No city found"

    # Extract images  css-1dz0toi
    images = []
    img_tags = soup.find_all("img", class_="css-1dz0toi") 
    for img_tag in img_tags:
        img_src = img_tag.get("src")
        if img_src:
            images.append(img_src)
    
    if len(images) == 0:
        img_tags = soup.find_all("img", class_="css-i2cz4f") 
        for img_tag in img_tags:
            img_src = img_tag.get("src")
            if img_src:
                images.append(img_src)
    
    if len(images) == 0:
        img_tags = soup.find_all("img", class_="css-1aus8y6")
        for img_tag in img_tags:
            img_src = img_tag.get("src")
            if img_src:
                images.append(img_src)
        

    # Extract housing details
    housing_details = {}
    for detail in soup.find_all("div", class_="css-1ksgrzt"):
        key_tag = detail.find("span", class_="css-1td16zm")
        value_tag = detail.find("span", class_="css-1f8murc")
        if key_tag and value_tag:
            key = key_tag.get_text(strip=True)
            value = value_tag.get_text(strip=True)
            housing_details[key] = convert_to_boolean(value) if value in ["Ja", "Nej", "Ikke angivet"] else value

    # Extract rental details
    rental_details = {}
    for detail in soup.find_all("div", class_="css-1ksgrzt"):
        key_tag = detail.find("span", class_="css-1td16zm")
        value_tag = detail.find("span", class_="css-1f8murc")
        if key_tag and value_tag:
            key = key_tag.get_text(strip=True)
            value = value_tag.get_text(strip=True)
            rental_details[key] = convert_to_boolean(value) if value in ["Ja", "Nej","Ikke angivet"] else value

    return {
        "title": title,
        "published_date": published_date,
        "city": city,
        "images": images,
        "housing_details": housing_details,
        "rental_details": rental_details,
    }

# all_data = load_data_file()

def saavedatatosupabase(all_data):
    for listing in all_data:
        # Check if the listing URL already exists in Supabase
        response = supabase.table('listings').select('url').eq('url', listing['url']).execute()

        # If the URL doesn't exist, insert the new listing
        if not response.data:
            insert_response = supabase.table('listings').insert(listing).execute()
            
        else:
            print(f"Skipping listing with URL: {listing['url']} (duplicate).")


# Inside the main function, map the data to match Supabase schema
async def main():
    # Create SSL context that ignores verification
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Increase timeout and add retry logic
    timeout = aiohttp.ClientTimeout(total=60)
    
    current_offset = 0
    max_retries = 3
    all_data = []  # Initialize an empty list to hold data temporarily

    print("Starting scraper. Press CTRL+C to stop and resume later.")

    try:
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            with tqdm() as pbar:
                while True:
                    try:
                        # Ensure clean offset value without any commas
                        clean_offset = str(current_offset).replace(',', '')
                        page_url = f"{BASE_URL}?offset={clean_offset}"
                        html_content = await fetch_page(session, page_url)

                        if html_content:
                            try:
                                urls = parse_listing_urls(html_content)
                                if not urls:
                                    print("No more listings found. Exiting...")
                                    break

                                # Fetch and parse each listing
                                for url in urls:
                                    try:
                                        listing_html = await fetch_page(session, url)
                                        if listing_html:
                                            try:
                                                data = parse_listing_details(listing_html)
                                                
                                                images = data['images']
                                                if not images or len(images) == 0:
                                                    print(f"Skipping listing without images: {url}")
                                                    continue

                                                data["url"] = url

                                                # Map the details to match Supabase schema
                                                try:
                                                    supabase_data = {
                                                        "title": data["title"],
                                                        "city": data["city"],
                                                        "published_date": data["published_date"],
                                                        "images": data["images"],
                                                        "boligtype": data["housing_details"].get("Boligtype", ""),
                                                        "storrelse": data["housing_details"].get("Størrelse", ""),
                                                        "vaerelser": data["housing_details"].get("Værelser", ""),
                                                        "etage": data["housing_details"].get("Etage", ""),
                                                        "moebleret": data["housing_details"].get("Møbleret", False),
                                                        "delevenlig": data["housing_details"].get("Delevenlig", False),
                                                        "husdyr_tilladt": data["housing_details"].get("Husdyr tilladt", False),
                                                        "elevator": data["housing_details"].get("Elevator", False),
                                                        "seniorvenlig": data["housing_details"].get("Seniorvenlig", False),
                                                        "kun_studerende": data["housing_details"].get("Kun for studerende", False),
                                                        "altan_terrasse": data["housing_details"].get("Altan/terrasse", False),
                                                        "parkering": data["housing_details"].get("Parkering", False),
                                                        "opvaskemaskine": data["housing_details"].get("Opvaskemaskine", False),
                                                        "vaskemaskine": data["housing_details"].get("Vaskemaskine", False),
                                                        "ladestander": data["housing_details"].get("Ladestander", ""),
                                                        "toerretumbler": data["housing_details"].get("Tørretumbler", ""),
                                                        "lejeperiode": data["rental_details"].get("Lejeperiode", ""),
                                                        "ledig_fra": data["rental_details"].get("Ledig fra", "Snarest muligt") if data["rental_details"].get("Ledig fra") != "Snarest muligt" else None,
                                                        "maanedlig_leje": data["rental_details"].get("Månedlig leje", ""),
                                                        "aconto": data["rental_details"].get("Aconto", ""),
                                                        "depositum": data["rental_details"].get("Depositum", ""),
                                                        "forudbetalt_husleje": data["rental_details"].get("Forudbetalt husleje", ""),
                                                        "indflytningspris": data["rental_details"].get("Indflytningspris", ""),
                                                        "oprettelsesdato": data["rental_details"].get("Oprettelsesdato", "") if data["rental_details"].get("Oprettelsesdato") else None,
                                                        "sagsnr": data["rental_details"].get("Sagsnr.", ""),
                                                        "url": url,
                                                        # New fields with default values
                                                        "user_id": None,
                                                        "is_our_listing": False,
                                                        "address": None,
                                                        "description": None,
                                                        "availableFrom": None,
                                                        "rentalPeriod": None,
                                                        "status": False,
                                                        "bathrooms": None
                                                    }

                                                    all_data.append(supabase_data)

                                                    if len(all_data) >= 200:
                                                        try:
                                                            saavedatatosupabase(all_data)
                                                            all_data.clear()
                                                        except Exception as e:
                                                            print(f"Error saving to Supabase: {e}")
                                                    pbar.update(1)
                                                except Exception as e:
                                                    print(f"Error processing data for URL {url}: {e}")
                                                    continue
                                            except Exception as e:
                                                print(f"Error parsing listing details for URL {url}: {e}")
                                                continue
                                    except Exception as e:
                                        print(f"Error fetching listing URL {url}: {e}")
                                        continue
                            except Exception as e:
                                print(f"Error parsing listing URLs from page {page_url}: {e}")
                                continue

                        current_offset += OFFSET_STEP
                    except Exception as e:
                        print(f"Error processing page offset {current_offset}: {e}")
                        current_offset += OFFSET_STEP
                        continue

    except KeyboardInterrupt:
        print("Process interrupted. Saving progress.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Push all remaining data to Supabase
        if all_data:
            try:
                saavedatatosupabase(all_data)
                all_data.clear()
            except Exception as e:
                print(f"Error saving final batch to Supabase: {e}")

# Run the scraper
asyncio.run(main())


