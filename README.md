# Zillow Scraper

This project is designed to scrape rental data from Zillow and store the results in a CSV file.

## Prerequisites

- Python 3.x installed
- A valid Nimble API key

## Setup

1. **Configure the .env file**

   Create a `.env` file in the root directory of the project with the following content:

   ```
   NIMBLE_API_URL='https://api.webit.live/api/v1/realtime/web'
   NIMBLE_API_KEY=<your_nimble_api_key>
   ```

   Replace `<your_nimble_api_key>` with your actual Nimble API key.

2. **Install required dependencies**

   Run the following command in your terminal to install the necessary Python packages:

   ```bash
   pip install -r requirements.txt
   ```

## Running the Scraper

1. **Execute the script**

   In your terminal, run the following command to start the scraping process:

   ```bash
   python scrape_zillow.py
   ```

## Output

The script will generate a CSV file named `zillow_rentals.csv` containing the scraped rental data from Zillow.


## Additional information

The rest of the files in this project are results of efforts to improve the code and address two main issues.

Issue 1: **Incomplete Property Listings**

The scraper was capturing only 9 properties, while the Zillow page displayed 17 or 18 properties (observed over the last two days). Attempts to improve the code by handling pagination, lazy loading, and other techniques did not increase the number of captured properties. Analysis of the debug_search_page_full.html file revealed that information about the properties not captured by the scraper appeared approximately twice less frequently in the debug output compared to listed properties. For example, the property at 1000 Kern St, Normal, IL was among those not captured. Further investigation into this issue is recommended, or consultation with a senior developer may be necessary to resolve it.

Issue 2: **Incomplete Property Data**

The scraper currently does not capture complete data for the properties due to limited parsing of individual property card pages. Efforts were primarily focused on resolving the first issue (incomplete property listings), as it was deemed higher priority based on the customer's requirement that "scraping should be exhaustive." Additional time and effort are needed to enhance the parsing of property details to improve the completeness of the data.