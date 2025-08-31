import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
from dotenv import load_dotenv
import urllib.parse
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
NIMBLE_API_KEY = os.getenv("NIMBLE_API_KEY")
NIMBLE_API_URL = os.getenv("NIMBLE_API_URL")

if not NIMBLE_API_KEY or not NIMBLE_API_URL:
    raise RuntimeError("Missing NIMBLE_API_KEY or NIMBLE_API_URL in environment")

def create_render_flow_with_clicks(click_selectors= ["button[data-test='load-more-results']", "button[aria-label='Show more results']"], wait_selectors= ["section[data-test='search-page-list-container'] article[data-test='property-card']"], scroll_duration=10000):
    """Create a render flow with optional click operations."""
    flow = []
    
    # Add wait for initial elements
    if wait_selectors:
        flow.append({"wait_for": {"selectors": wait_selectors, "timeout": 15000, "visible": True}})
    
    # Proactively click optional expanders if present (e.g., load more)
    if click_selectors:
        for selector in click_selectors:
            flow.append({
                "wait_and_click": {
                    "selector": selector,
                    "timeout": 20000,
                    "delay": 500,
                    "scroll": True,
                    "visible": False
                }
            })
    
    # SUPER AGGRESSIVE scrolling strategy to trigger lazy loading
    # First, try to scroll to specific card positions to trigger loading
    scroll_targets = [
        "article[data-test='property-card']:nth-child(5)",   # Scroll to 5th card
        "article[data-test='property-card']:nth-child(10)",  # Scroll to 10th card
        "article[data-test='property-card']:nth-child(15)",  # Scroll to 15th card
        "article[data-test='property-card']:nth-child(20)",  # Scroll to 20th card
        "article[data-test='property-card']:last-child",     # Scroll to last card
        "section[data-test='search-page-list-container']",   # Scroll to container
        "div[data-test='search-page-list']",                 # Scroll to list
        "footer",                                            # Scroll to footer
        "div[data-test='loading-spinner']",                  # Scroll to loading spinner
        "body",                                              # Scroll to very bottom
        "html"                                               # Scroll to very bottom
    ]
    
    for target in scroll_targets:
        flow.append({
            "scroll_to": {
                "selector": target,
                "visible": False
            }
        })
        flow.append({"wait": {"delay": 3000}})  # Longer wait between scrolls
    
    # Add a very aggressive infinite scroll with longer duration
    flow.append({
        "infinite_scroll": {
            "duration": 45000,  # 45 seconds of scrolling
            "loading_selector": "div[data-test='loading-spinner']",
            "delay_after_scroll": 5000,  # Longer delay
            "idle_timeout": 8000  # Longer idle timeout
        }
    })
    
    # Additional scroll attempts after infinite scroll
    flow.append({"wait": {"delay": 8000}})
    flow.append({
        "scroll_to": {
            "selector": "body",
            "visible": False
        }
    })
    flow.append({"wait": {"delay": 5000}})
    
    # Try scrolling to specific positions again
    flow.append({
        "scroll_to": {
            "selector": "article[data-test='property-card']:nth-child(18)",
            "visible": False
        }
    })
    flow.append({"wait": {"delay": 3000}})
    
    # Final settle wait and ensure cards are visible
    flow.append({"wait": {"delay": 8000}})
    if wait_selectors:
        flow.append({"wait_for": {"selectors": wait_selectors, "timeout": 20000, "visible": True}})
    
    return flow

def create_single_card_click_flow(card_selector, wait_for_detail=True):
    """Create a render flow to click on a single card and wait for detail page."""
    flow = [
        {"wait_for": {"selectors": [card_selector], "timeout": 10000, "visible": True}},
        {"wait_and_click": {"selector": card_selector, "timeout": 20000, "delay": 1000, "scroll": True, "visible": False}},
    ]
    
    if wait_for_detail:
        flow.extend([
            {"wait": {"delay": 2000}},  # Wait for navigation
            {"wait_for": {"selectors": ["script#hdpApolloPreloadedData"], "timeout": 15000, "visible": True}},
            {"wait": {"delay": 3000}}  # Wait for data to load
        ])
    
    return flow

def nimble_request(url, render_flow=None, retries=3, backoff=2):
    """Send a request to Nimble Web API with retries and return HTML content."""
    headers = {
        "Authorization": f"Basic {NIMBLE_API_KEY}",
        "Content-Type": "application/json"
    }
    # Default render flow for search pages
    default_render_flow = [
        {"wait_for": {"selectors": ["article[data-test='property-card']"], "timeout": 10000, "visible": True}},
        {"wait_and_click": {"selector": "[class*='StyledCard']", "timeout": 20000, "delay": 500, "scroll": True, "visible": False}},
        {"infinite_scroll": {"duration": 10000, "loading_selector": "div[data-test='loading-spinner']", "delay_after_scroll": 1000}},
        {"wait": {"delay": 3000}}
    ]
    payload = {
        "url": url,
        "render": True,
        "format": "json",
        "render_flow": render_flow or default_render_flow
    }
    for attempt in range(retries):
        try:
            logger.info(f"Fetching URL: {url} (Attempt {attempt + 1})")
            response = requests.post(NIMBLE_API_URL, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            if result["status"] == "success":
                return result["html_content"], result.get("render_flow")
            else:
                logger.error(f"Nimble API error: {result.get('message', 'Unknown error')}")
        except requests.RequestException as e:
            logger.error(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
                continue
        return None, None
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None, None

def parse_search_page(html):
    """Parse the search page HTML to extract detail page URLs and basic card data."""
    soup = BeautifulSoup(html, 'html.parser')
    
    # DEBUG: Save the HTML for inspection
    with open("debug_search_page_full.html", "w", encoding="utf-8") as f:
        f.write(html)
    logger.info("Saved full HTML to debug_search_page_full.html")
    
    # DEBUG: Check for common Zillow patterns
    logger.info("=== DEBUGGING CARD DETECTION ===")
    
    # Check for script tags with data
    scripts = soup.find_all('script')
    logger.info(f"Found {len(scripts)} script tags")
    
    # Check for any elements with property-related classes
    property_elements = soup.find_all(class_=lambda x: x and any(term in x.lower() for term in ['property', 'card', 'listing', 'home', 'house']))
    logger.info(f"Found {len(property_elements)} elements with property-related classes")
    
    # Check for any elements with data-test attributes
    data_test_elements = soup.find_all(attrs={'data-test': True})
    logger.info(f"Found {len(data_test_elements)} elements with data-test attributes")
    for elem in data_test_elements[:10]:  # Show first 10
        logger.info(f"  data-test='{elem.get('data-test')}' -> {elem.name}")
    
    # Try multiple selectors to find property cards
    card_selectors = [
        'article[data-test="property-card"]',
        'div[data-test="property-card"]',
        'li[data-test="property-card"]',
        'div[class*="property-card"]',
        'article[class*="property-card"]',
        'div[class*="StyledCard"]',
        'article[class*="StyledCard"]',
        'div[data-test="search-page-list"] article',
        'div[data-test="search-page-list"] div[class*="card"]',
        'section[data-test="search-page-list-container"] article',
        'section[data-test="search-page-list-container"] div[class*="card"]'
    ]
    
    property_cards = []
    for selector in card_selectors:
        cards = soup.select(selector)
        if cards:
            logger.info(f"Found {len(cards)} cards using selector: {selector}")
            property_cards.extend(cards)
            # Don't break - try all selectors to get maximum coverage
    
    # Remove duplicates while preserving order
    seen_cards = set()
    unique_cards = []
    for card in property_cards:
        # Create a unique identifier for each card
        card_id = f"{card.name}_{card.get('class', [])}_{card.get('data-test', '')}"
        if card_id not in seen_cards:
            seen_cards.add(card_id)
            unique_cards.append(card)
    
    property_cards = unique_cards
    logger.info(f"After deduplication: {len(property_cards)} unique cards")
    
    # If no cards found with specific selectors, try broader approach
    if not property_cards:
        logger.info("No cards found with specific selectors, trying broader search...")
        # Look for any elements that might contain property links
        potential_cards = soup.find_all(['article', 'div', 'li'], class_=lambda x: x and any(term in x.lower() for term in ['card', 'property', 'listing', 'item']))
        property_cards = potential_cards
        logger.info(f"Found {len(property_cards)} potential cards with broader search")
    
    logger.info(f"Total property cards found: {len(property_cards)}")
    
    # DEBUG: Check what we actually found
    if property_cards:
        logger.info("=== FIRST 3 CARDS DEBUG ===")
        for i, card in enumerate(property_cards[:3]):
            logger.info(f"Card {i+1}:")
            logger.info(f"  Tag: {card.name}")
            logger.info(f"  Classes: {card.get('class', [])}")
            logger.info(f"  Data-test: {card.get('data-test', 'None')}")
            logger.info(f"  Text preview: {card.get_text()[:100]}...")
    
    card_data = []
    seen_urls = set()
    
    # Primary pass over found cards
    for card in property_cards:
        card_info = {}
        
        # Try multiple link selectors
        link_selectors = [
            'a[data-test="property-card-link"]',
            'a[href*="/homedetails/"]',
            'a[data-test="property-link"]',
            'a[class*="property-link"]',
            'a[class*="card-link"]'
        ]
        
        a = None
        for selector in link_selectors:
            a = card.select_one(selector)
            if a and 'href' in a.attrs:
                break
        
        if a and 'href' in a.attrs:
            href = a['href']
            if not href.startswith('https://'):
                href = 'https://www.zillow.com' + href
            card_info['detail_url'] = href
            seen_urls.add(href)
        
        # Price - try multiple selectors
        price_selectors = [
            'span[data-test="property-card-price"]',
            'span[data-test="price"]',
            'span[class*="price"]',
            'div[data-test="property-card-price"]',
            'div[class*="price"]',
            '[data-test="price"]',
            '[class*="price"]'
        ]
        for selector in price_selectors:
            price_elem = card.select_one(selector)
            if price_elem:
                card_info['card_price'] = price_elem.get_text(strip=True)
                break
        
        # Address - try multiple selectors
        address_selectors = [
            'address[data-test="property-card-addr"]',
            'address[data-test="address"]',
            'span[data-test="property-card-addr"]',
            'span[data-test="address"]',
            'div[data-test="property-card-addr"]',
            'div[data-test="address"]',
            '[data-test="address"]',
            '[class*="address"]'
        ]
        for selector in address_selectors:
            address_elem = card.select_one(selector)
            if address_elem:
                card_info['card_address'] = address_elem.get_text(strip=True)
                break
        
        # Beds/Baths via selectors or regex
        bed_bath_selectors = [
            'ul[data-test="property-beds"]',
            'ul[class*="bed"]',
            'div[data-test="property-beds"]',
            'div[class*="bed"]',
            '[data-test="bed"]',
            '[class*="bed"]'
        ]
        for selector in bed_bath_selectors:
            beds_baths_elem = card.select_one(selector)
            if beds_baths_elem:
                bed_bath_items = beds_baths_elem.find_all(['li', 'span', 'div'])
                for item in bed_bath_items:
                    text = item.get_text(strip=True).lower()
                    if 'bd' in text or 'bed' in text:
                        card_info['card_bedrooms'] = item.get_text(strip=True)
                    elif 'ba' in text or 'bath' in text:
                        card_info['card_bathrooms'] = item.get_text(strip=True)
                break
        if 'card_bedrooms' not in card_info or 'card_bathrooms' not in card_info:
            all_text = card.get_text().lower()
            import re
            bed_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:bd|bed)', all_text)
            if bed_match:
                card_info['card_bedrooms'] = f"{bed_match.group(1)} bd"
            bath_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:ba|bath)', all_text)
            if bath_match:
                card_info['card_bathrooms'] = f"{bath_match.group(1)} ba"
        
        # Sqft
        sqft_selectors = [
            'span[data-test="property-card-size"]',
            'span[data-test="size"]',
            'span[class*="sqft"]',
            'span[class*="size"]',
            'div[data-test="property-card-size"]',
            'div[data-test="size"]',
            'div[class*="sqft"]',
            'div[class*="size"]',
            '[data-test="size"]',
            '[class*="sqft"]'
        ]
        for selector in sqft_selectors:
            sqft_elem = card.select_one(selector)
            if sqft_elem:
                text = sqft_elem.get_text(strip=True)
                if 'sqft' in text.lower() or any(char.isdigit() for char in text):
                    card_info['card_sqft'] = text
                    break
        
        # Home type
        home_type_selectors = [
            'span[data-test="property-card-type"]',
            'span[data-test="type"]',
            'span[class*="type"]',
            'div[data-test="property-card-type"]',
            'div[data-test="type"]',
            'div[class*="type"]',
            '[data-test="type"]',
            '[class*="type"]'
        ]
        for selector in home_type_selectors:
            home_type_elem = card.select_one(selector)
            if home_type_elem:
                card_info['card_home_type'] = home_type_elem.get_text(strip=True)
                break
        
        if card_info:
            card_data.append(card_info)
    
    # Secondary pass: catch any homedetails links not wrapped in standard cards
    extra_links = soup.select("a[href*='/homedetails/']")
    added = 0
    for a in extra_links:
        href = a.get('href')
        if not href:
            continue
        if not href.startswith('http'):
            href = 'https://www.zillow.com' + href
        if '/homedetails/' not in href:
            continue
        if href in seen_urls:
            continue
        seen_urls.add(href)
        card_data.append({
            'detail_url': href
        })
        added += 1
    if added:
        logger.info(f"Added {added} extra homedetails links from fallback selector")
    
    logger.info(f"Total unique properties parsed: {len(seen_urls)} (with {len(card_data)} card entries)")
    return card_data

def extract_basic_info_from_html(soup):
    """Extract basic property information from HTML elements when JSON data is not available."""
    data = {}
    
    try:
        # Extract price
        price_elem = soup.find('span', attrs={'data-test': 'price'}) or soup.find('span', class_='price')
        if price_elem:
            data['price'] = price_elem.get_text(strip=True)
        
        # Extract address
        address_elem = soup.find('h1', attrs={'data-test': 'home-details-summary-address'}) or soup.find('h1', class_='address')
        if address_elem:
            data['address'] = address_elem.get_text(strip=True)
        
        # Extract bedrooms and bathrooms
        beds_baths = soup.find_all('span', attrs={'data-test': 'bed-bath-brief'})
        for elem in beds_baths:
            text = elem.get_text(strip=True)
            if 'bd' in text:
                data['bedrooms'] = text
            elif 'ba' in text:
                data['bathrooms'] = text
        
        # Extract square footage
        sqft_elem = soup.find('span', attrs={'data-test': 'property-size'}) or soup.find('span', class_='sqft')
        if sqft_elem:
            data['square_feet'] = sqft_elem.get_text(strip=True)
        
        # Extract home type
        home_type_elem = soup.find('span', attrs={'data-test': 'property-type'}) or soup.find('span', class_='home-type')
        if home_type_elem:
            data['home_type'] = home_type_elem.get_text(strip=True)
        
        # Extract year built
        year_elem = soup.find('span', attrs={'data-test': 'year-built'}) or soup.find('span', class_='year-built')
        if year_elem:
            data['year_built'] = year_elem.get_text(strip=True)
        
        logger.info(f"Extracted basic info from HTML: {data}")
        return data
        
    except Exception as e:
        logger.error(f"Error extracting basic info from HTML: {e}")
        return {}

def extract_data_from_search_page_html(html):
    """Extract comprehensive data from the search page HTML using embedded JSON data."""
    soup = BeautifulSoup(html, 'html.parser')
    all_properties = []
    
    try:
        # Look for embedded JSON data in the search page
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and ('apiCache' in script.string or 'property' in script.string):
                try:
                    # Try to extract JSON data
                    script_content = script.string.strip()
                    if 'window.__INITIAL_STATE__=' in script_content:
                        json_start = script_content.find('window.__INITIAL_STATE__=') + len('window.__INITIAL_STATE__=')
                        json_end = script_content.find(';', json_start)
                        if json_end == -1:
                            json_end = len(script_content)
                        json_str = script_content[json_start:json_end]
                        data = json.loads(json_str)
                        
                        # Extract property data from the search results
                        if 'searchResults' in data:
                            for result in data['searchResults'].get('listResults', []):
                                property_data = {
                                    'zpid': result.get('zpid'),
                                    'address': result.get('address', ''),
                                    'price': result.get('price', ''),
                                    'bedrooms': result.get('beds', ''),
                                    'bathrooms': result.get('baths', ''),
                                    'square_feet': result.get('area', ''),
                                    'home_type': result.get('propertyType', ''),
                                    'year_built': result.get('yearBuilt', ''),
                                    'lot_size': result.get('lotSize', ''),
                                    'price_per_sqft': result.get('pricePerSqft', ''),
                                    'days_on_zillow': result.get('daysOnZillow', ''),
                                    'description': result.get('description', ''),
                                    'detail_url': f"https://www.zillow.com/homedetails/{result.get('zpid')}/"
                                }
                                all_properties.append(property_data)
                        
                        logger.info(f"Extracted {len(all_properties)} properties from search page JSON")
                        return all_properties
                        
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Failed to parse JSON from script: {e}")
                    continue
        
        # If no JSON data found, return empty list
        logger.warning("No JSON data found in search page")
        return all_properties
        
    except Exception as e:
        logger.error(f"Error extracting data from search page: {e}")
        return all_properties

def parse_house_page(html):
    """Parse the house detail page HTML to extract property information using embedded JSON."""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Try multiple script selectors
    script = None
    script_selectors = [
        'script#hdpApolloPreloadedData',
        'script[data-zillow-zpid]',
        'script[type="application/json"]',
        'script:contains("apiCache")'
    ]
    
    for selector in script_selectors:
        script = soup.select_one(selector)
        if script:
            logger.info(f"Found script with selector: {selector}")
            break
    
    if not script:
        # Try to find any script containing property data
        scripts = soup.find_all('script')
        for s in scripts:
            if s.string and ('apiCache' in s.string or 'property' in s.string):
                script = s
                logger.info("Found script containing property data")
                break
    
    if not script:
        logger.error("No preloaded data script found. Trying to extract from HTML...")
        # Try to extract basic info from HTML elements
        return extract_basic_info_from_html(soup)
    
    try:
        # Clean the script content
        script_content = script.string.strip()
        if script_content.startswith('window.__INITIAL_STATE__='):
            script_content = script_content.replace('window.__INITIAL_STATE__=', '')
        elif script_content.startswith('window.__APOLLO_STATE__='):
            script_content = script_content.replace('window.__APOLLO_STATE__=', '')
        
        preloaded_data = json.loads(script_content)
        
        # Try different data structures
        api_cache = preloaded_data.get('apiCache', {})
        if not api_cache:
            api_cache = preloaded_data.get('property', {})
        
        for value in api_cache.values():
            if 'property' in value:
                prop = value['property']
                break
        else:
            # If no property found in apiCache, try direct property access
            prop = preloaded_data.get('property', {})
            if not prop:
                logger.error("No property data found in any structure")
                return extract_basic_info_from_html(soup)
                
                # Extract basic property information
                price = prop.get('price', 0)
                living_area = prop.get('livingArea', 0)
                bedrooms = prop.get('bedrooms', 0)
                bathrooms = prop.get('bathrooms', 0)
                year_built = prop.get('yearBuilt', 0)
                home_type = prop.get('homeType', 'Unknown')
                
                # Calculate price per square foot
                price_per_sqft = None
                if living_area and living_area > 0 and price and price > 0:
                    price_per_sqft = round(price / living_area, 2)
                
                # Extract lot size (in square feet)
                lot_size = prop.get('resoFacts', {}).get('lotSize', 0)
                
                # Extract additional property details
                reso_facts = prop.get('resoFacts', {})
                lot_size_sqft = reso_facts.get('lotSize', 0)
                lot_size_acres = reso_facts.get('lotSizeAcres', 0)
                
                # Extract pet policy
                pet_policy = []
                if prop.get('petPolicy', {}).get('dogsAllowed'):
                    pet_policy.append('Dogs OK')
                if prop.get('petPolicy', {}).get('catsAllowed'):
                    pet_policy.append('Cats OK')
                
                # Extract appliances
                appliances = reso_facts.get('appliances', [])
                
                # Extract price history
                price_history = [
                    {
                        'date': event.get('date'),
                        'event': event.get('event'),
                        'price': event.get('price'),
                        'pricePerSquareFoot': event.get('pricePerSquareFoot'),
                        'source': event.get('source')
                    }
                    for event in prop.get('priceHistory', [])
                ]
                
                # Create comprehensive data dictionary
                data = {
                    'zpid': prop.get('zpid'),
                    'address': f"{prop.get('streetAddress', '')}, {prop.get('city', '')}, {prop.get('state', '')} {prop.get('zipcode', '')}",
                    'price': price,
                    'bedrooms': bedrooms,
                    'bathrooms': bathrooms,
                    'square_feet': living_area,
                    'home_type': home_type,
                    'year_built': year_built,
                    'lot_size_sqft': lot_size_sqft,
                    'lot_size_acres': lot_size_acres,
                    'price_per_sqft': price_per_sqft,
                    'description': prop.get('description', ''),
                    'petPolicy': ', '.join(pet_policy) if pet_policy else 'None',
                    'appliances': ', '.join(appliances) if appliances else 'None',
                    'parcelNumber': reso_facts.get('parcelNumber'),
                    'daysOnZillow': prop.get('daysOnZillow', -1),
                    'priceHistory': json.dumps(price_history),
                    'propertyTaxRate': reso_facts.get('propertyTaxRate'),
                    'hoaFee': reso_facts.get('hoaFee'),
                    'hoaFeeFrequency': reso_facts.get('hoaFeeFrequency'),
                    'heating': reso_facts.get('heating'),
                    'cooling': reso_facts.get('cooling'),
                    'parking': reso_facts.get('parking'),
                    'roof': reso_facts.get('roof'),
                    'exteriorMaterial': reso_facts.get('exteriorMaterial'),
                    'foundation': reso_facts.get('foundation'),
                    'basement': reso_facts.get('basement'),
                    'flooring': reso_facts.get('flooring'),
                    'kitchenFeatures': reso_facts.get('kitchenFeatures'),
                    'bathroomFeatures': reso_facts.get('bathroomFeatures'),
                    'bedroomFeatures': reso_facts.get('bedroomFeatures'),
                    'livingRoomFeatures': reso_facts.get('livingRoomFeatures'),
                    'diningRoomFeatures': reso_facts.get('diningRoomFeatures'),
                    'familyRoomFeatures': reso_facts.get('familyRoomFeatures'),
                    'laundryFeatures': reso_facts.get('laundryFeatures'),
                    'otherFeatures': reso_facts.get('otherFeatures'),
                    'schools': json.dumps(prop.get('schools', [])),
                    'neighborhood': prop.get('neighborhood', {}).get('name', ''),
                    'walkScore': prop.get('walkScore', {}).get('score', 0),
                    'transitScore': prop.get('transitScore', {}).get('score', 0),
                    'bikeScore': prop.get('bikeScore', {}).get('score', 0)
                }
                
                # Log the extracted data for verification
                logger.info(f"Extracted data: {bedrooms} bed, {bathrooms} bath, {living_area} sqft, {home_type}, {year_built} built, ${price_per_sqft}/sqft")
                
                return data
        logger.error("No property data found in apiCache.")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error parsing house page: {e}")
        return {}

def get_total_pages(soup):
    """Extract the total number of pages from the pagination nav."""
    pagination_nav = soup.find('nav', attrs={'aria-label': 'Pagination'})
    if not pagination_nav:
        logger.info("No pagination nav found, assuming single page")
        return 1
    pages = []
    for a in pagination_nav.find_all('a'):
        if a.text.isdigit():
            pages.append(int(a.text))
    total = max(pages) if pages else 1
    logger.info(f"Total pages: {total}")
    return total

def process_individual_cards(initial_url, max_cards=None, use_clicks=True):
    """Process each property card individually by clicking on them or direct navigation."""
    all_data = []
    
    # First, get the search page to find all cards
    search_render_flow = create_render_flow_with_clicks(
        click_selectors=[],  # No clicks initially, just get the page
        wait_selectors=["article[data-test='property-card']"],
        scroll_duration=120000  # Increased to 120 seconds for very aggressive scrolling
    )
    
    logger.info("Fetching initial search page to find property cards...")
    html, _ = nimble_request(initial_url, render_flow=search_render_flow)
    if not html:
        logger.error("Failed to fetch initial search page.")
        return all_data
    
    # Save search page HTML for debugging
    with open("debug_search_page.html", "w", encoding="utf-8") as f:
        f.write(html)
    logger.info("Saved search page HTML to debug_search_page.html")
    
    # Check initial card count and try additional scrolling if needed
    soup = BeautifulSoup(html, 'html.parser')
    initial_card_count = len(soup.select('article[data-test="property-card"]'))
    logger.info(f"Initial card count: {initial_card_count}")
    
    # If we have fewer than expected cards, try additional scrolling
    if initial_card_count < 15:  # Expecting around 18, so try to get at least 15
        logger.info(f"Only found {initial_card_count} cards, attempting additional scrolling...")
        
        # Additional aggressive scroll attempts
        additional_scroll_flow = [
            {"wait": {"delay": 5000}},
            {"scroll_to": {"selector": "article[data-test='property-card']:nth-child(10)", "visible": False}},
            {"wait": {"delay": 3000}},
            {"scroll_to": {"selector": "article[data-test='property-card']:nth-child(15)", "visible": False}},
            {"wait": {"delay": 3000}},
            {"scroll_to": {"selector": "article[data-test='property-card']:nth-child(18)", "visible": False}},
            {"wait": {"delay": 3000}},
            {"scroll_to": {"selector": "body", "visible": False}},
            {"wait": {"delay": 3000}},
            {"scroll_to": {"selector": "footer", "visible": False}},
            {"wait": {"delay": 3000}},
            {
                "infinite_scroll": {
                    "duration": 30000,
                    "loading_selector": "div[data-test='loading-spinner']",
                    "delay_after_scroll": 3000,
                    "idle_timeout": 5000
                }
            },
            {"wait": {"delay": 8000}}
        ]
        
        additional_html, _ = nimble_request(initial_url, render_flow=additional_scroll_flow)
        if additional_html:
            html = additional_html  # Use the updated HTML
            soup = BeautifulSoup(html, 'html.parser')
            updated_card_count = len(soup.select('article[data-test="property-card"]'))
            logger.info(f"After additional scrolling: {updated_card_count} cards")
            
            # Save updated HTML
            with open("debug_search_page.html", "w", encoding="utf-8") as f:
                f.write(html)
    
    # Try to extract data from search page JSON first (most comprehensive)
    logger.info("Attempting to extract data from search page JSON...")
    json_data = extract_data_from_search_page_html(html)
    if json_data:
        logger.info(f"Successfully extracted {len(json_data)} properties from search page JSON")
        for i, prop_data in enumerate(json_data, 1):
            prop_data['card_index'] = i
            prop_data['source'] = 'search_page_json'
            all_data.append(prop_data)
        return all_data
    
    # Fallback to card parsing if JSON extraction fails
    logger.info("JSON extraction failed, falling back to card parsing...")
    soup = BeautifulSoup(html, 'html.parser')
    card_data_list = parse_search_page(html)
    logger.info(f"Found {len(card_data_list)} property cards with data")
    
    if max_cards:
        card_data_list = card_data_list[:max_cards]
        logger.info(f"Processing first {len(card_data_list)} cards")
    
    # Custom render flow for detail pages
    detail_render_flow = [
        {"wait": {"delay": 5000}},  # Wait for page to load
        {"wait_for": {"selectors": ["script#hdpApolloPreloadedData", "h1[data-test='home-details-summary-address']", "span[data-test='price']"], "timeout": 20000, "visible": True}},
        {"wait": {"delay": 3000}}  # Wait for data to load
    ]
    
    for i, card_data in enumerate(card_data_list, 1):
        logger.info(f"Processing card {i}/{len(card_data_list)}")
        
        # Get the card's link
        href = card_data.get('detail_url')
        if not href:
            logger.warning(f"Card {i} has no valid link, skipping")
            continue
        
        if use_clicks:
            # Method 1: Click on the card to navigate
            card_selector = f"article[data-test='property-card']:nth-child({i}) a[data-test='property-card-link']"
            card_click_flow = create_single_card_click_flow(card_selector)
            
            logger.info(f"Clicking on card {i}: {href}")
            detail_html, _ = nimble_request(initial_url, render_flow=card_click_flow)
        else:
            # Method 2: Direct navigation to detail URL
            logger.info(f"Navigating directly to card {i}: {href}")
            detail_html, _ = nimble_request(href, render_flow=detail_render_flow)
        
        if detail_html:
            # Save HTML for debugging
            with open(f"debug_card_{i}.html", "w", encoding="utf-8") as f:
                f.write(detail_html)
            logger.info(f"Saved HTML for card {i} to debug_card_{i}.html")
            
            house_data = parse_house_page(detail_html)
            if house_data:
                house_data['card_index'] = i
                house_data['detail_url'] = href
                
                # Add card-level data
                house_data['card_price'] = card_data.get('card_price', '')
                house_data['card_address'] = card_data.get('card_address', '')
                house_data['card_bedrooms'] = card_data.get('card_bedrooms', '')
                house_data['card_bathrooms'] = card_data.get('card_bathrooms', '')
                house_data['card_sqft'] = card_data.get('card_sqft', '')
                house_data['card_home_type'] = card_data.get('card_home_type', '')
                
                all_data.append(house_data)
                logger.info(f"Successfully extracted data from card {i}: {house_data}")
            else:
                # If no detail page data, use card data as fallback
                fallback_data = {
                    'card_index': i,
                    'detail_url': href,
                    'card_price': card_data.get('card_price', ''),
                    'card_address': card_data.get('card_address', ''),
                    'card_bedrooms': card_data.get('card_bedrooms', ''),
                    'card_bathrooms': card_data.get('card_bathrooms', ''),
                    'card_sqft': card_data.get('card_sqft', ''),
                    'card_home_type': card_data.get('card_home_type', ''),
                    'source': 'card_data_only'
                }
                all_data.append(fallback_data)
                logger.info(f"Using card data as fallback for card {i}: {fallback_data}")
        else:
            logger.error(f"Failed to fetch detail page for card {i}")
        
        # Add a small delay between cards
        time.sleep(2)
    
    return all_data

if __name__ == "__main__":
    initial_url = "https://www.zillow.com/bloomington-il-61761/rentals/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A40.636718508366414%2C%22south%22%3A40.43345579864026%2C%22east%22%3A-88.85417987207032%2C%22west%22%3A-89.1054921279297%7D%2C%22filterState%22%3A%7B%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22mp%22%3A%7B%22min%22%3A1000%2C%22max%22%3A2000%7D%2C%22tow%22%3A%7B%22value%22%3Afalse%7D%2C%22mf%22%3A%7B%22value%22%3Afalse%7D%2C%22con%22%3A%7B%22value%22%3Afalse%7D%2C%22land%22%3A%7B%22value%22%3Afalse%7D%2C%22apa%22%3A%7B%22value%22%3Afalse%7D%2C%22manu%22%3A%7B%22value%22%3Afalse%7D%2C%22apco%22%3A%7B%22value%22%3Afalse%7D%2C%22r4r%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%2C%22usersSearchTerm%22%3A%2261761%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A85145%2C%22regionType%22%3A7%7D%5D%7D"
    
    # Process each card individually by clicking on them
    logger.info("Starting individual card processing...")
    
    # Choose your method:
    # use_clicks=True: Click on each card to navigate (more interactive)
    # use_clicks=False: Direct navigation to detail URLs (more reliable)
    use_clicks = False  # Set to False for direct navigation (more reliable)
    
    all_data = process_individual_cards(initial_url, max_cards=20, use_clicks=use_clicks)
    
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv('zillow_rentals_individual_cards.csv', index=False)
        logger.info(f"Saved {len(all_data)} listings from individual card clicks to CSV.")
        
        # Print summary of extracted data
        print("\n=== EXTRACTED DATA SUMMARY ===")
        print(f"Total properties: {len(all_data)}")
        if all_data:
            sample = all_data[0]
            print(f"Sample data fields: {list(sample.keys())}")
            print(f"Sample property: {sample}")
    else:
        logger.warning("No data collected from individual card processing.")