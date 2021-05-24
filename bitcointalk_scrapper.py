from bs4 import BeautifulSoup
import cfscrape
from scrapy.selector import Selector
import lxml.html
import shortuuid
import secrets
import time
import csv
import argparse
import requests
import traceback
import sys

class BitcoinTalkScrapper:

    def __init__(self, start, end):
        self.start = start
        self.end = end

    scraper = cfscrape.create_scraper()
    page_base_url = "https://bitcointalk.org/index.php?board=1.0"
    
    def getAllPagePostsResponse(self):
        """dumps post's response in csv for each posts iteratively based on start & end page parameters"""
        filename = f'bitcointalk_{time.time()}.csv'
        with open(filename, "a") as csvfile:
            headers = ['Id', 'Title', 'Datetime', 'MsgId', 'Response']
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
            writer.writeheader()

            html = self.scraper.get(self.page_base_url).content
            soup = BeautifulSoup(html, 'html.parser')

            td = soup.find("td", {"id": "toppages"}).find_all('a')
            total_pages = td[-2].text

            for idx in range(self.start, self.end + 1):
                posts_url = f"{self.page_base_url[:-1]}{(idx - 1) * 40}"
                errorfileName = f'page-{idx}.txt'
                print('page number', idx)
                print('posts_url', posts_url)
                posts_html = self.scraper.get(posts_url)
                
                try:
                    statusCode = posts_html.status_code
                    print(f'Website url calling status code : {statusCode}')
                    if statusCode == 200:
                        post_soup = BeautifulSoup(posts_html.content, 'html.parser')
                        select_obj = Selector(text=posts_html.text)
                        if idx == 1:
                            # post_tr_list = post_soup.select("#bodyarea > div:nth-of-type(3)")[0].find('table').find_all('tr')
                            post_tr_list = post_soup.find("div", {"id": "bodyarea"}).select('div:nth-of-type(3)')[0].find('table').find_all('tr')
                        else:
                            print('else')
                            # post_tr_list = post_soup.select("#bodyarea > div:nth-of-type(2)")[0].find('table').find_all('tr')
                            post_tr_list = post_soup.find("div", {"id": "bodyarea"}).select('div:nth-of-type(2)')[0].find('table').find_all('tr')
                            print(len(post_tr_list))
                        for post_tr in post_tr_list[1:]:
                            db_unique_id = self.generateUniqueId()
                            span = post_tr.find_all('td')[2].find('span')
                            title = span.find('a').text
                            msg_id = span['id']
                            link = span.find('a')['href']
                            total_response_page_list = post_tr.find_all('td')[2].find('small').find_all('a')
                            if total_response_page_list:
                                total_response_page = int(total_response_page_list[-2].text)
                                post_response_counter = 0
                                for i in range(1, total_response_page + 1):
                                    if i != 1:
                                        reply_page_url = f"{link[:-1]}{post_response_counter}"
                                    else:
                                        reply_page_url = link
                                    print('reply_page_url', reply_page_url)    
                                    response_list = self.getAllResponse(reply_page_url)
                                    if response_list:
                                        for dict_obj in response_list:
                                            dict_obj.update({"Title": title, "MsgId": msg_id, "Id": db_unique_id})
                                            writer.writerow(dict_obj)
                                    post_response_counter += 20
                                    time.sleep(1.5)
                            else:
                                reply_page_url = link
                                print('reply_page_url', reply_page_url)            
                                response_list = self.getAllResponse(reply_page_url)
                                if response_list:
                                    for dict_obj in response_list:
                                        dict_obj.update({"Title": title, "MsgId": msg_id, "Id": db_unique_id})
                                        writer.writerow(dict_obj)
                                time.sleep(1.5)
                    else:
                        print(f'Api call error occured with error code : {statusCode}')
                        print(f'Error Response : {posts_html.text}')
                except requests.exceptions.HTTPError as errh:
                    print ("Http Error:",errh)
                    sys.exit()
                except requests.exceptions.ConnectionError as errc:
                    print ("Error Connecting:",errc)
                    sys.exit()
                except requests.exceptions.Timeout as errt:
                    print ("Timeout Error:",errt)
                    sys.exit()
                except requests.exceptions.RequestException as err:
                    print ("Something else requests error", err)
                    sys.exit()    
                except Exception as ex:
                    with open(errorfileName, 'w') as file:
                        file.write(posts_html.text)
                    traceback.print_exception(type(ex), ex, ex.__traceback__)
                    sys.exit()
                time.sleep(1.5)       

    def getAllResponse(self, reply_page_url):
        """Fetches all response text from each tr tag and return list of dicts"""
        response_list = []
        reply_page_html = self.scraper.get(reply_page_url).content
        reply_page_soup = BeautifulSoup(reply_page_html, 'html.parser') 
        table_trs = Selector(text=str(reply_page_soup)).xpath("//form[@id='quickModForm']/table[1]/tr").extract()

        for idx, tr in enumerate(table_trs):
            tr_selector = Selector(text=tr)
            datetime_xp = "//tr[1]/td/table/tr[1]/td[2]/table/tr/td[2]/div[2]"
            datetime_list = tr_selector.xpath(datetime_xp)
            if datetime_list:
                dt_str = datetime_list.xpath('text()').get()
                if not dt_str:
                    dt_str = datetime_list.xpath('span[1]/text()').get()
                tr_all_text = ''.join(list(map(str.strip,tr_selector.xpath('//tr[1]').css('td *::text').getall())))
                response_list.append({"Datetime": dt_str, "Response": tr_all_text})
        return response_list

    @staticmethod
    def generateUniqueId():
        """geneare unique uuid"""
        shortuuid.set_alphabet(secrets.token_urlsafe())
        return shortuuid.uuid()

def main():
    # Initiate the parser
    parser = argparse.ArgumentParser()
    # Add long and short argument
    parser.add_argument("--start", "-s", type=int, required=True, help="an integer for start page")
    parser.add_argument("--end", "-e", type=int, required=True, help="an integer for end page")
    # Read arguments from the command line
    args = parser.parse_args()
    if args.start <= 0:
        parser.error('start must be greater than zero.')
    elif args.end <= 0:
        parser.error('end must be greater than zero.')
    elif args.end < args.start:
        parser.error('start must be less than or equals to end.')
    
    obj = BitcoinTalkScrapper(start=args.start, end=args.end)
    obj.getAllPagePostsResponse()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- Finished in %s minutes ---" % str((time.time() - start_time)/60))
