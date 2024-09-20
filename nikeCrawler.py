from playwright.async_api import async_playwright, Page, Playwright
import asyncio
from rich.logging import RichHandler
import logging
import inspect
import os, json
import sqlite3
from fake_useragent import UserAgent
from dataclasses import dataclass, asdict, field
import pandas as pd
import functools
import time


def logs():
    frame = inspect.currentframe().f_back 
    file_name = os.path.basename(frame.f_globals['__file__'])
    logger_name = f"{file_name}"

    logger = logging.getLogger(logger_name)
    logger.setLevel(level=logging.DEBUG)

    terminal = RichHandler()
    logger.addHandler(terminal)
    
    handle = logging.FileHandler("scrape.log", mode='w')
    formats = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    handle.setFormatter(formats)
    logger.addHandler(handle)
   
    return logger

def timer(func):
    @functools.wraps(func)
    async def wrapper(*agrs, **kwargs):
        start = time.perf_counter()
        await func(*agrs, **kwargs)
        end = time.perf_counter()
        total = end - start
        print(f"Execution time: {round(total, 2)}")
    return wrapper


log = logs()

@dataclass
class Nike_Men:
    """ A ```DATACLASS``` obj """
    Name: str = None
    Sbtitle_name: str = None
    Price: float = None
    Available_Sizes: list[str] = None
    Discription: str = None
    Colors: list[str] = None
    Product_Id: str = None
    Total_Review: str = None
    Total_stars: float = None
    Product_url: str = None

@dataclass
class SaveData:
    items: Nike_Men = None
    file: str = ''
    folder:str = ''
    _path:str = ''
    data_list: list[Nike_Men] = field(default_factory=list)
   
    def add_item(self, ):
        return self.data_list.append(self.items)
   
    def create_folder(self):
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)
        self._path = f"{self.folder}/{self.file}"
        return self._path
    
    def dataframe(self):
        return pd.json_normalize((asdict(data) for data in self.data_list), sep='_')

    def save_to_json(self):
        if not os.path.exists(f'{self._path}.json'):
            self.dataframe().to_json(f'{self._path}.json', orient='records', index=False, indent=3)
        else:
            existing_df = pd.read_json(f"{self._path}.json")
            new_df = self.dataframe()
            update_df = pd.concat([existing_df, new_df])
            update_df.to_json(f"{self._path}.json", orient='records', indent=2)

    def save_to_csv(self):
        if os.path.exists(f'{self._path}.csv'):
            self.dataframe().to_csv(f"{self._path}.csv", index=False, mode='a', header=False)
        else:
            self.dataframe().to_csv(f'{self._path}.csv', index=False)
    
    def save_to_excel(self):
        if not os.path.exists(f'{self._path}.xlsx'):
            self.dataframe().to_excel(f'{self._path}.xlsx', index=False)
        else:
            with pd.ExcelWriter(f'{self._path}.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                self.dataframe().to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)

    def save_to_sqlite(self):        
        conn = sqlite3.connect(f"{self._path}.db")
        cur = conn.cursor()
        for dats in self.data_list:
            datas = asdict(dats)
            key = [k for k, v in datas.items()]
            keys = ', '.join(key)
            place_holder = ', '.join('?' for _ in range(len(key)))
            values = [json.dumps(v) if isinstance(v, list) else v for v in datas.values()]
            cur.execute(f"CREATE TABLE IF NOT EXISTS scraped (id INTEGER PRIMARY KEY,{keys})")
            cur.execute(f"INSERT INTO scraped ({keys}) VALUES ({place_holder})", (values))
            conn.commit()
            conn.close()
    
    def save_all(self):
        log.info('Saveing data...')
        self.add_item()
        self.create_folder()
        self.save_to_json()
        self.save_to_csv()
        self.save_to_excel()
        self.save_to_sqlite()
        log.debug('Done saveing...')

class Browser:
    def __init__(self, url: str) -> None:
        self.playwright:Playwright
        self.page: Page 
        self.url: str = url

    async def browser(self)->None:
        log.info("Starting Browser")
        browser = await self.playwright.firefox.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 650, 'height': 540},
            user_agent = UserAgent().random
        )
        self.page = await context.new_page()
        
        log.info(f"page.goto {self.url}")
        await self.page.goto(self.url, timeout=80000)
        self.page.set_default_navigation_timeout(90000)
        await self.page.wait_for_load_state("networkidle")
       
    async def _scroll(self):
        await self.page.wait_for_load_state()
        previous_length = 0
        for _ in range(20):
            selector = '//div[@data-testid="product-card"]/div/figure/a'
            await self.page.wait_for_selector('[data-testid="product-card"]')
            elements = await self.page.query_selector_all(selector=selector)
            new_length = len(elements)-1
            print(new_length)
            await elements[new_length].scroll_into_view_if_needed()
            await self.page.wait_for_timeout(2000)
            if previous_length == new_length:
                log.debug(f"Total Items:{new_length}")
                break
            previous_length = new_length
        log.info('Done scrooling')
  
    async def select_location(self):
        log.debug("Navigateing To Product")
        page = self.page
        await page.wait_for_load_state()
        try:
            await page.get_by_test_id("modal-backdrop").locator("summary").filter(has_text="Americas").click()
            await self.page.wait_for_timeout(1000)
            await page.get_by_role("link", name="United States English").click()
        except Exception as e:
            log.error(f"{e}", exc_info=True)
        await self.page.wait_for_timeout(3000)
        try:    
            await page.get_by_label("menu", exact=True).click()
            await self.page.wait_for_timeout(1000)
            await page.get_by_role("button", name="Men", exact=True).click()
            await self.page.wait_for_timeout(1000)
            await page.get_by_test_id("panel-link__New_Arrivals").click()    
        except Exception as e:
            log.error(f"{e}", exc_info=True)
            
    async def select_tab(self):
        page = self.page
        await self.page.wait_for_load_state()
        try:
            await self.page.wait_for_timeout(1000)
            await page.get_by_role('tablist').locator('a').filter(has_text='Shoes').click()
        except Exception as e:
            log.error(f"{e}", exc_info=True)
        await self.page.wait_for_timeout(3000)
        log.debug("Done Navigating...")

    async def scrape_data(self):
        await self.page.wait_for_timeout(2000)
        name = await self.page.locator('h1#pdp_product_title').inner_text()
        subtitle_name = await self.page.locator('h1#pdp_product_subtitle').inner_text()
        price = await self.page.get_by_test_id("currentPrice-container").first.inner_text()
        discription = await self.page.get_by_test_id("product-description").inner_text()
        try:
            colors_raw = await self.page.get_by_test_id("product-description-color-description").inner_text()
            colors = colors_raw.replace('Shown: ', '').strip()
            if '/' in colors:
                colors = colors.split('/')
        except:
            pass
        try:
            prod_id_raw = await self.page.get_by_test_id("product-description-style-color").inner_text()
            prod_id = prod_id_raw.replace('Style: ', '').strip()
        except:
            pass
        try:
            review_raw = await self.page.locator('span.nds-summary-wrapper > div > h4').inner_text()
            review = review_raw.replace('Reviews (', '').replace(')', '').strip()
        except:
            review = 0
        try:
            star = await self.page.locator('span.nds-summary-wrapper > span > div').get_attribute('title')
        except:
            star = '0 Stars'
        sel = '//div[@data-testid="pdp-grid-selector-item"]/label'
        try:
            sizes = await self.page.locator(sel).all()
            size = [await x.inner_text() for x in sizes]
        except:
            size = 'No available sizes yet or comming soon'
        url_ = self.page.url
        data = Nike_Men(
            Name=name,
            Sbtitle_name=subtitle_name,
            Price=price,
            Available_Sizes=size,
            Discription=discription,
            Colors=colors,
            Product_Id=prod_id,
            Total_Review=review,
            Total_stars=star,
            Product_url=url_
        )
        log.debug(f"{data}")
        store = SaveData(file='nike_shoes', folder='NikeMen_shoes', 
                         items=data)
        store.save_all()

    async def get_item_listing(self):
        page = self.page
        selector = '//div[@data-testid="product-card"]/div/figure/a[@class="product-card__link-overlay"]'
        await page.wait_for_selector('[data-testid="product-card"]')    
        links = await page.locator(selector).all()
        log.info(f'found total of {len(links)} links from product listing')
        urls:list[str] = []
        count = 0
        for _, l in enumerate(links):
            urls_ = await l.get_attribute('href')
            if urls_ not in urls:
                urls.append(urls_)
            else:
                log.debug(f'Duplicate url found and removed: {urls_}')
        for url in urls:
            log.debug(f'{count}. Going to: {url}')
            await page.goto(url, timeout=80000)
            await page.wait_for_load_state()
            await self.scrape_data()
            count += 1
            if count == 40:
                break
    @timer
    async def main(self):
        async with async_playwright() as self.playwright:
            await self.browser()
            
            await self.select_location()
            await self.select_tab()

            await self._scroll()
            await self.get_item_listing()

            await self.page.close()


if __name__ == '__main__':
    try:
        url='https://www.nike.com/'
        b = Browser(url)
        asyncio.run(b.main())    
    except Exception as e:
        log.error(f"{e}", exc_info=True)