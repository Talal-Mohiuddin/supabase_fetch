import aiohttp
import asyncio
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import json
from supabase import create_client, Client

# Configuration
BASE_URL = "https://www.boligportal.dk/lejeboliger/"
OFFSET_STEP = 18  # Number of listings per page


# Supabase client
url: str = os.getenv('SUPABASE_URL')
key: str = os.getenv('SUPABASE_KEY')
supabase: Client = create_client(url, key)



# Fetch and parse a page asynchronously
async def fetch_page(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.text()
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


# Parse individual listing pages to extract details
def parse_listing_details(html_content):
    soup = BeautifulSoup(html_content, "html.parser")

    title_tag = soup.find("span", class_="css-v34a4n")
    title = title_tag.get_text(strip=True) if title_tag else "No title found"

    desc_tag = soup.find("div", class_="css-1j674uz")
    description = desc_tag.get_text(strip=True) if desc_tag else "No description found"

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
        "description": description,
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
    current_offset = 0
    all_data = []  # Initialize an empty list to hold data temporarily

    print("Starting scraper. Press CTRL+C to stop and resume later.")

    try:
        async with aiohttp.ClientSession() as session:
            with tqdm() as pbar:
                while True:
                    try:
                        page_url = f"{BASE_URL}?offset={current_offset}"
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
                                                        "description": data["description"],
                                                        "city": data["city"],
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
                                                        "url": url
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


