The file nikeCrawler.py is a Python script designed to scrape product data from the Nike website. It uses the Playwright library for browser automation and Rich for logging. The script defines several classes and functions to perform tasks such as logging, timing, data storage, and web scraping.

**Summary of nikeCrawler.py:**

**Imports:** 
The script imports various libraries, including Playwright for browser automation, Rich for enhanced logging, SQLite for database operations, Pandas for data manipulation, and more.

**Logging Setup:** 
The logs function sets up logging using Rich and outputs logs to both the console and a file named scrape.log.

**Timer Decorator:** 
The timer function is a decorator that measures the execution time of asynchronous functions.

**Nike_Men DataClass:** 
The Nike_Men class defines the structure of the data to be scraped, including product name, subtitle, price, available sizes, description, colors, product ID, total reviews, total stars, and product URL.

**SaveData DataClass:** 
The SaveData class manages the storage of scraped data in various formats (JSON, CSV, Excel, SQLite).

**Browser Class:** 
The Browser class handles browser automation tasks such as launching the browser, navigating to specific pages, scrolling, and extracting product data.

**Main Execution:** 
The script's main execution starts the browser and performs the scraping tasks.

