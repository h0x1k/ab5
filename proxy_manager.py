import requests
from bs4 import BeautifulSoup
import logging
import random
import time
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class ProxyManager:
    """
    Класс для поиска, проверки и выбора лучшего прокси-сервера.
    """
    def __init__(self):
        # FIX: Добавлены новые источники, убран фильтр по странам
        self.proxy_source_urls = {
            'free-proxy-list': 'https://free-proxy-list.net/',
            'sslproxies': 'https://www.sslproxies.org/',
            'advanced.name': 'https://advanced.name/ru/freeproxy'
        }
        self.user_agent = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
        }

    def _scrape_free_proxy_list_family(self, url: str) -> list:
        """Скрейпинг сайтов типа free-proxy-list.net и sslproxies.org."""
        proxies = []
        try:
            response = requests.get(url, headers=self.user_agent, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            table = soup.find('table', class_='table-striped')
            if not table:
                return []
            
            rows = table.find('tbody').find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) > 6 and cols[6].text.strip() == 'yes':
                    ip = cols[0].text.strip()
                    port = cols[1].text.strip()
                    proxies.append(f"{ip}:{port}")
        except Exception as e:
            logger.error(f"Ошибка при сборе прокси с {url}: {e}")
        return proxies

    def _scrape_advanced_name(self, url: str) -> list:
        """Скрейпинг сайта advanced.name."""
        proxies = []
        try:
            response = requests.get(url, headers=self.user_agent, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            table = soup.find('table', id='tbl_proxy_list')
            if not table:
                return []
            
            rows = table.find('tbody').find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) > 3 and 'HTTPS' in cols[3].text:
                    try:
                        # IP и порт закодированы в base64
                        ip_b64 = cols[1].get('data-ip')
                        port_b64 = cols[1].get('data-port')
                        if ip_b64 and port_b64:
                            ip = base64.b64decode(ip_b64).decode('utf-8')
                            port = base64.b64decode(port_b64).decode('utf-8')
                            proxies.append(f"{ip}:{port}")
                    except Exception:
                        continue
        except Exception as e:
            logger.error(f"Ошибка при сборе прокси с {url}: {e}")
        return proxies

    def _scrape_all_sources(self) -> list:
        """Собирает прокси со всех источников."""
        all_proxies = []
        
        # Словарь для сопоставления имени источника с функцией скрейпинга
        scrapers = {
            'free-proxy-list': self._scrape_free_proxy_list_family,
            'sslproxies': self._scrape_free_proxy_list_family,
            'advanced.name': self._scrape_advanced_name,
        }

        for name, url in self.proxy_source_urls.items():
            logger.info(f"Сбор прокси с {name} ({url})...")
            scraped = scrapers.get(name)(url)
            if scraped:
                all_proxies.extend(scraped)
                logger.info(f"Найдено {len(scraped)} прокси на {name}.")
        
        unique_proxies = list(set(all_proxies))
        random.shuffle(unique_proxies)
        logger.info(f"Всего найдено {len(unique_proxies)} уникальных прокси для проверки.")
        return unique_proxies

    def _validate_single_proxy(self, proxy: str) -> tuple | None:
        """
        Проверяет один прокси и измеряет его скорость.
        Возвращает кортеж (прокси, скорость) или None.
        """
        try:
            proxies_dict = {'https': f'http://{proxy}'} # Многие HTTPS прокси работают по HTTP-протоколу
            start_time = time.time()
            response = requests.get('https://google.com', proxies=proxies_dict, timeout=7)
            end_time = time.time()
            
            if response.status_code == 200:
                speed = end_time - start_time
                return (proxy, speed)
        except Exception:
            pass
        return None
    
    def get_best_working_proxy(self):
        """
        Находит все рабочие прокси, сортирует их по скорости и возвращает лучший.
        """
        logger.info("Поиск самого быстрого рабочего прокси...")
        scraped_proxies = self._scrape_all_sources()
        
        if not scraped_proxies:
            logger.error("Не удалось найти ни одного прокси для проверки.")
            return None

        working_proxies = []
        with ThreadPoolExecutor(max_workers=30) as executor:
            future_to_proxy = {executor.submit(self._validate_single_proxy, proxy): proxy for proxy in scraped_proxies}
            
            for future in as_completed(future_to_proxy):
                result = future.result()
                if result:
                    working_proxies.append(result)
                    logger.info(f"Найден рабочий прокси: {result[0]} со скоростью {result[1]:.2f} сек.")

        if not working_proxies:
            logger.error("Не удалось найти ни одного рабочего прокси после проверки всех найденных.")
            return None
        
        # Сортируем по скорости (второй элемент кортежа)
        working_proxies.sort(key=lambda x: x[1])
        
        best_proxy = working_proxies[0][0]
        best_speed = working_proxies[0][1]
        
        logger.info(f"Проверка завершена. Найдено {len(working_proxies)} рабочих прокси.")
        logger.info(f"Выбран самый быстрый прокси: {best_proxy} (скорость: {best_speed:.2f} сек).")
        
        return best_proxy
