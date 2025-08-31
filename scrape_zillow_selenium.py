import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import urllib.parse
import time
import random
import csv
import re
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium_stealth import stealth

# Load environment variables
load_dotenv()
NIMBLE_API_KEY = os.getenv("NIMBLE_API_KEY")
NIMBLE_API_URL = os.getenv("NIMBLE_API_URL")

if not NIMBLE_API_KEY or not NIMBLE_API_URL:
    raise RuntimeError("Missing NIMBLE_API_KEY or NIMBLE_API_URL")

def setup_selenium_driver():
    """Set up a headless Selenium Chrome driver with stealth."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    driver = webdriver.Chrome(options=chrome_options)
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True)
    return driver

def create_scroll_flow():
    """Create a scrolling flow for Nimble API."""
    flow = [
        {"wait_for": {"selectors": ["li[class*='ListItem-c11n']"], "timeout": 30000, "visible": True}},
        {"scroll_to": {"selector": "li[class*='ListItem-c11n']:last-child", "visible": False}},
        {"wait": {"delay": 5000}},
        {"scroll_to": {"selector": "body", "visible": False}},
        {"wait": {"delay": 5000}},
        {"infinite_scroll": {"duration": 60000, "loading_selector": "div[data-test='loading-spinner']", "delay_after_scroll": 5000, "idle_timeout": 10000}},
        {"wait": {"delay": 10000}}
    ]
    return flow

def nimble_request(url, render_flow=None, retries=2, timeout=120):
    """Send a request to Nimble Web API with retries and timeout."""
    headers = {
        "Authorization": f"Basic {NIMBLE_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    payload = {
        "url": url,
        "render": True,
        "format": "json",
        "render_flow": render_flow
    }
    for attempt in range(retries):
        try:
            response = requests.post(NIMBLE_API_URL, headers=headers, json=payload, timeout=timeout)
            response.raise_for_status()
            result = response.json()
            if result["status"] == "success":
                return result["html_content"]
            print(f"Request to {url} failed: {result.get('message', 'Unknown error')}")
            return None
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            if attempt < retries - 1:
                time.sleep(random.uniform(3, 5))
            continue
    print(f"Failed to fetch {url} after {retries} attempts")
    return None

def extract_property_details(card):
    """Extract ZPID, URL, and address from a property card."""
    zpid = card.get('id', '').replace('zpid_', '').replace('property-card-', '')
    link = card.find('a', {'data-test': 'property-card-link'})
    url = f"https://www.zillow.com{link['href']}" if link and 'href' in link.attrs and not link['href'].startswith('http') else link['href'] if link else None
    address_selectors = [
        'address[data-test="property-card-addr"]',
        'div[data-test="property-card-addr"]',
        'span[data-test="property-card-addr"]',
        'address[class*="address"]',
        'div[class*="StyledPropertyCardDataArea"]',
        'span[class*="address"]',
        'div[class*="PropertyCard"]',
        'div[class*="card-info"]'
    ]
    address = 'Unknown'
    for selector in address_selectors:
        address_elem = card.select_one(selector)
        if address_elem:
            address = address_elem.text.strip()
            print(f"Address extracted from card: {address}")
            break
    if address == 'Unknown':
        card_text = card.get_text().strip()
        address_match = re.search(r'\d+\s+[A-Za-z\s-]+,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?', card_text)
        if address_match:
            address = address_match.group(0)
            print(f"Address extracted from card regex: {address}")
    return {'zpid': zpid, 'url': url, 'address': address}

def extract_apartment_details(url):
    """Navigate to an apartment page and extract detailed information."""
    render_flow = [
        {"wait_for": {"selectors": ["span[data-testid='price']", "h1[data-testid='home-details-address']", "div[data-testid='bed-bath-beyond']"], "timeout": 40000, "visible": True}},
        {"scroll_to": {"selector": "body", "visible": False}},
        {"wait": {"delay": 8000}},
        {"scroll_to": {"selector": "div[data-testid='facts-and-features']", "visible": False}},
        {"wait": {"delay": 6000}}
    ]
    html = nimble_request(url, render_flow=render_flow)
    if not html:
        print(f"Skipping detail page {url} due to fetch failure")
        return None
    
    soup = BeautifulSoup(html, 'html.parser')
    details = {}
    
    price_selectors = [
        'span[data-testid="price"]',
        'div[data-testid="price"]',
        'span[class*="price"]',
        'div[class*="price"]',
        'h4[class*="price"]',
        'span[class*="Price"]',
        'div[class*="PriceDetails"]'
    ]
    details['price'] = 'Unknown'
    for selector in price_selectors:
        price_elem = soup.select_one(selector)
        if price_elem:
            details['price'] = price_elem.text.strip()
            break
    
    details['bedrooms'] = 'Unknown'
    details['bathrooms'] = 'Unknown'
    details['sqft'] = 'Unknown'
    summary_elem = soup.select_one('div[data-testid="bed-bath-beyond"]')
    if summary_elem:
        summary_text = summary_elem.text.lower()
        bed_match = re.search(r'(\d+)\s*bed(?:room|s)?', summary_text)
        if bed_match:
            details['bedrooms'] = bed_match.group(1)
        bath_match = re.search(r'(\d+\.?\d*)\s*bath(?:room|s)?', summary_text)
        if bath_match:
            details['bathrooms'] = bath_match.group(1)
        sqft_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*(?:sqft|ft²)', summary_text)
        if sqft_match:
            details['sqft'] = sqft_match.group(1)
    
    if any(details[key] == 'Unknown' for key in ['bedrooms', 'bathrooms', 'sqft']):
        all_text = soup.get_text().lower()
        bed_match = re.search(r'(\d+)\s*bed(?:room|s)?', all_text)
        if bed_match:
            details['bedrooms'] = bed_match.group(1)
        bath_match = re.search(r'(\d+\.?\d*)\s*bath(?:room|s)?', all_text)
        if bath_match:
            details['bathrooms'] = bath_match.group(1)
        sqft_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*(?:sqft|ft²)', all_text)
        if sqft_match:
            details['sqft'] = sqft_match.group(1)
    
    details['address'] = 'Unknown'
    address_elem = soup.select_one('h1[data-testid="home-details-address"]') or soup.select_one('h1[class*="address"]') or soup.select_one('div[class*="address"]')
    if address_elem:
        details['address'] = address_elem.text.strip()
        print(f"Address extracted from detail page: {details['address']}")
    else:
        parsed_url = urllib.parse.urlparse(url)
        path_match = re.search(r'/homedetails/(.+?)/\d+_zpid', parsed_url.path)
        if path_match:
            details['address'] = path_match.group(1).replace('-', ' ')
            print(f"Address extracted from URL: {details['address']}")
    
    facts_selectors = [
        'div[data-testid="facts-and-features"]',
        'ul[class*="facts-features"]',
        'div[class*="home-facts"]',
        'div[class*="facts"]',
        'div[class*="FactGroup"]'
    ]
    details['pets_allowed'] = 'No'
    details['laundry'] = 'None'
    details['parking'] = 'None'
    details['cooling'] = 'None'
    details['heating'] = 'None'
    for selector in facts_selectors:
        facts = soup.select(selector)
        for fact_group in facts:
            items = fact_group.find_all(['span', 'li', 'div'])
            for item in items:
                text = item.text.lower()
                if 'cats' in text or 'dogs' in text or 'no pets' in text:
                    details['pets_allowed'] = text.strip()
                elif 'laundry' in text or 'washer' in text or 'dryer' in text:
                    details['laundry'] = text.strip()
                elif 'parking' in text or 'garage' in text:
                    details['parking'] = text.strip()
                elif 'air conditioning' in text or 'central air' in text:
                    details['cooling'] = text.strip()
                elif 'heating' in text or 'forced air' in text:
                    details['heating'] = text.strip()
        if all(details[key] != 'None' for key in ['pets_allowed', 'laundry', 'parking', 'cooling', 'heating']):
            break
    
    return details

def scrape_zillow_rentals():
    """Scrape all property cards from the first page using Selenium with Nimble API fallback."""
    initial_url = "https://www.zillow.com/bloomington-il-61761/rentals/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A40.636718508366414%2C%22south%22%3A40.43345579864026%2C%22east%22%3A-88.85417987207032%2C%22west%22%3A-89.1054921279297%7D%2C%22filterState%22%3A%7B%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22mp%22%3A%7B%22min%22%3A1000%2C%22max%22%3A2000%7D%2C%22tow%22%3A%7B%22value%22%3Afalse%7D%2C%22mf%22%3A%7B%22value%22%3Afalse%7D%2C%22con%22%3A%7B%22value%22%3Afalse%7D%2C%22land%22%3A%7B%22value%22%3Afalse%7D%2C%22apa%22%3A%7B%22value%22%3Afalse%7D%2C%22manu%22%3A%7B%22value%22%3Afalse%7D%2C%22apco%22%3A%7B%22value%22%3Afalse%7D%2C%22r4r%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%2C%22usersSearchTerm%22%3A%2261761%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A85145%2C%22regionType%22%3A7%7D%5D%7D"
    all_properties = []
    
    # Try Selenium first
    driver = setup_selenium_driver()
    try:
        print(f"Processing page with Selenium: {initial_url}")
        driver.get(initial_url)
        time.sleep(random.uniform(5, 10))  # Initial delay to mimic human behavior
        
        # Simulate human-like mouse movement
        actions = ActionChains(driver)
        actions.move_by_offset(random.randint(100, 500), random.randint(100, 500)).click().perform()
        
        # Check for CAPTCHA
        if "captcha" in driver.page_source.lower():
            print("CAPTCHA detected in Selenium. Falling back to Nimble API.")
            driver.quit()
            html = nimble_request(initial_url, render_flow=create_scroll_flow())
            if not html:
                print("Nimble API also failed to fetch page.")
                return
            soup = BeautifulSoup(html, 'html.parser')
        else:
            # Wait for initial property cards to load
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li[class*='ListItem-c11n']"))
                )
            except TimeoutException:
                print("Timeout waiting for initial property cards")
                driver.save_screenshot("selenium_error_initial.png")
                with open("selenium_error_initial.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                driver.quit()
                return
            
            # Scroll to load all listings
            max_scroll_attempts = 10
            last_height = driver.execute_script("return document.body.scrollHeight")
            for _ in range(max_scroll_attempts):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(3, 5))  # Slower scroll
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Wait for all property cards to be visible
            try:
                cards = WebDriverWait(driver, 30).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li[class*='ListItem-c11n']"))
                )
                print(f"Found {len(cards)} property cards after scrolling")
            except TimeoutException:
                print("Timeout waiting for all property cards")
                driver.save_screenshot("selenium_error_cards.png")
                with open("selenium_error_cards.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                driver.quit()
                return
            
            # Check JSON data for total listings
            try:
                script = driver.find_element(By.CSS_SELECTOR, "script#__NEXT_DATA__")
                json_data = json.loads(script.get_attribute("innerText"))
                listings = json_data["props"]["pageProps"]["componentProps"]["searchResults"]["cat1"]["searchResults"]["mapResults"]
                print(f"Found {len(listings)} listings in JSON data")
            except Exception as e:
                print(f"Could not parse JSON data: {str(e)}")
            
            # Get the fully rendered HTML
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            driver.quit()
        
        cards = soup.select('li[class*="ListItem-c11n"]')
        print(f"Found {len(cards)} cards in parsed HTML")
        
        if len(cards) < 17:
            print("Warning: Fewer cards found than expected (17). Saving raw HTML for debugging.")
            with open('search_page_debug_selenium.html', 'w', encoding='utf-8') as f:
                f.write(str(soup))
        
        for card in cards:
            details = extract_property_details(card)
            if details['url']:
                print(f"Fetching details for {details['url']}")
                try:
                    apartment_details = extract_apartment_details(details['url'])
                    if apartment_details:
                        if details['address'] == 'Unknown' and apartment_details.get('address') != 'Unknown':
                            details['address'] = apartment_details['address']
                        details.update({k: v for k, v in apartment_details.items() if k != 'address'})
                    all_properties.append(details)
                except Exception as e:
                    print(f"Error processing {details['url']}: {str(e)}. Saving partial data.")
                    all_properties.append(details)
                time.sleep(random.uniform(3, 5))  # Delay to avoid rate limiting
    
    finally:
        if 'driver' in locals():
            driver.quit()
    
    if not all_properties:
        print("No properties found.")
        return
    
    print(f"\nFound {len(all_properties)} properties")
    print("First 5 properties:")
    for i, prop in enumerate(all_properties[:5]):
        print(f"  {i+1}. {prop['address']} ({prop['url']})")
    
    # Save to CSV
    csv_columns = ['zpid', 'address', 'url', 'price', 'bedrooms', 'bathrooms', 'sqft', 'pets_allowed', 'laundry', 'parking', 'cooling', 'heating']
    with open('zillow_rentals_selenium.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for prop in all_properties:
            writer.writerow(prop)
    print("Saved apartment details to zillow_rentals_selenium.csv")