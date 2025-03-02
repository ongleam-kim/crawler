from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
import time
import argparse
import threading
from threading import Thread, Lock
import os

HEADLESS=True
save_lock = Lock()  # 파일 저장을 위한 쓰레드 락

class SafetyKoreaCrawler:
    def __init__(self, index, headless=False):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--window-size=1920,1080')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.crawled_data = []
        self.existing_cert_numbers = set()
        self.output_path = f"output/{index}.json"  # 인덱스.json 형식으로 저장
        self.index = index
        
    def load_existing_data(self):
        """기존 데이터 파일을 로드합니다."""
        try:
            with save_lock:  # 파일 읽기에도 락 사용
                with open(self.output_path, "r", encoding="utf-8") as f:
                    self.crawled_data = json.load(f)
            print(f"기존 데이터 {len(self.crawled_data)}개 로드 완료")
            self.existing_cert_numbers = {
                item["인증정보"]["인증번호"].lower()
                for item in self.crawled_data 
                if "인증정보" in item and "인증번호" in item["인증정보"]
            }
        except FileNotFoundError:
            print("새로운 데이터 파일을 생성합니다.")
    
    def save_data(self):
        """수집된 데이터를 파일로 실시간으로 저장합니다."""
        try:
            with save_lock:  # 파일 쓰기 시 락 사용
                with open(self.output_path, "w", encoding="utf-8") as f:
                    json.dump(self.crawled_data, f, ensure_ascii=False, indent=4)
            print(f"[Thread {self.index}] {len(self.crawled_data)}개의 데이터 저장 완료")
        except Exception as e:
            print(f"[Thread {self.index}] 데이터 저장 중 오류 발생: {e}")
            self._save_backup()

    def _save_backup(self):
        """백업 파일 저장을 시도합니다."""
        try:
            with save_lock:  # 백업 파일 저장 시에도 락 사용
                with open(f"backup_{self.output_path}", "w", encoding="utf-8") as f:
                    json.dump(self.crawled_data, f, ensure_ascii=False, indent=4)
            print("데이터가 backup 파일로 저장되었습니다.")
        except:
            print("데이터 저장에 완전히 실패했습니다.")

    def wait_for_element(self, by, value, wait_type="presence", timeout=10):
        """요소가 나타날 때까지 대기합니다."""
        wait = WebDriverWait(self.driver, timeout)
        if wait_type == "presence":
            return wait.until(EC.presence_of_element_located((by, value)))
        elif wait_type == "clickable":
            return wait.until(EC.element_to_be_clickable((by, value)))
        elif wait_type == "invisible":
            return wait.until(EC.invisibility_of_element_located((by, value)))
        elif wait_type == "all_present":
            return wait.until(EC.presence_of_all_elements_located((by, value)))

    def parse_detail_page(self, html_content):
        """상세 페이지의 데이터를 파싱합니다."""
        soup = BeautifulSoup(html_content, 'html.parser')
        return {
            "인증정보": self._parse_key_value_table(soup, "인증정보 상세"),
            "제품정보": self._parse_key_value_table(soup, "제품정보 상세"),
            "제조공장": self._parse_list_table(soup, "제조공장 상세", ["번호", "제조공장", "제조국"]),
            "연관 인증 번호": self._parse_list_table(soup, "연관 인증 번호 상세", ["번호", "인증번호", "인증상태"])
        }

    def _parse_key_value_table(self, soup, caption_text):
        """키-값 테이블을 파싱합니다."""
        data = {}
        caption = soup.find('caption', string=lambda t: t and caption_text in t)
        if caption:
            table = caption.find_parent('table')
            for row in table.find_all('tr'):
                for th in row.find_all('th'):
                    key = th.get_text(strip=True)
                    td = th.find_next_sibling('td')
                    value = td.get_text(strip=True) if td else ""
                    data[key] = value
        return data

    def _parse_list_table(self, soup, caption_text, header_keys):
        """리스트 형태의 테이블을 파싱합니다."""
        items = []
        caption = soup.find('caption', string=lambda t: t and caption_text in t)
        if caption:
            table = caption.find_parent('table')
            for row in table.find_all('tr')[1:]:  # Skip header row
                cols = row.find_all(['th', 'td'])
                if len(cols) >= len(header_keys):
                    item = {}
                    for idx, key in enumerate(header_keys):
                        value = cols[idx].find('a').get_text(strip=True) if cols[idx].find('a') else cols[idx].get_text(strip=True)
                        item[key] = value
                    items.append(item)
        return items

    def process_row(self, row):
        """각 행의 데이터를 처리하고 실시간으로 저장합니다."""
        cert_number = row.find_element(By.CSS_SELECTOR, "td:last-child").text.strip().lower()
        
        if cert_number in self.existing_cert_numbers:
            print(f"[Thread {self.index}] Skip existing cert number: {cert_number}")
            return

        time.sleep(2)
        row.click()

        self.wait_for_element(By.CLASS_NAME, "contents_area")
        data = self.parse_detail_page(self.driver.page_source)
        
        if "인증정보" in data and "인증번호" in data["인증정보"]:
            cert_number = data["인증정보"]["인증번호"].lower()
            if cert_number not in self.existing_cert_numbers:
                self.crawled_data.append(data)
                self.existing_cert_numbers.add(cert_number)
                print(f"[Thread {self.index}] Added new cert number: {cert_number} [{len(self.crawled_data)}]")
                # 데이터가 추가될 때마다 저장
                self.save_data()

        time.sleep(2)
        self.driver.back()
        self.wait_for_element(By.CLASS_NAME, "tb_list")

    def crawl(self, index):
        """크롤링을 실행합니다."""
        try:
            self.driver.get("https://www.safetykorea.kr/release/itemSearch")
            self.load_existing_data()
            next_button = self.wait_for_element(By.XPATH, "//a[@title='다음 페이지']", "clickable")
            next_button.click()


            while True:
                rows = self.wait_for_element(By.CSS_SELECTOR, "table.tb_list tr[onclick]", "all_present")
                row = rows[index]
                try:
                    self.process_row(row)
                except Exception as e:
                    print(f"Row processing error: {e}")
                    continue

                try:
                    self.wait_for_element(By.ID, "loading", "invisible")
                    next_button = self.wait_for_element(By.XPATH, "//a[@title='다음 페이지']", "clickable")
                    next_button.click()
                    time.sleep(2)
                except Exception as e:
                    print(f"Navigation error: {e}")
                    break

        except KeyboardInterrupt:
            print("\n사용자에 의해 중단되었습니다.")
        except Exception as e:
            print(f"\n예상치 못한 오류: {e}")
        finally:
            self.driver.quit()

def run_crawler(index):
    """각 쓰레드에서 실행될 크롤러 함수"""
    try:
        crawler = SafetyKoreaCrawler(index, headless=HEADLESS)
        print(f"Thread {index}: 크롤링 시작 - 출력 파일: {index}.json")
        crawler.crawl(index)
    except Exception as e:
        print(f"Thread {index}: 오류 발생 - {e}")

def main():
    """멀티쓰레드로 크롤러를 실행합니다."""
    parser = argparse.ArgumentParser(description='Safety Korea 데이터 크롤러 (멀티쓰레드)')
    parser.add_argument('--threads', type=int, default=10,
                       help='실행할 쓰레드 수 (기본값: 10)')
    
    args = parser.parse_args()
    threads = []
    
    try:
        # 쓰레드 생성 및 시작
        for i in range(args.threads):
            t = Thread(target=run_crawler, args=(i,))
            t.start()
            threads.append(t)
            print(f"Thread {i} started")
        
        # 모든 쓰레드 종료 대기
        for t in threads:
            t.join()
            
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다.")
        # 프로그램 종료
        print("프로그램이 종료되었습니다.")

if __name__ == "__main__":
    main()