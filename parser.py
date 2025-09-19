import logging
import time
import os
import random
import json
import tempfile
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sportschecker_parser.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SportscheckerParser:
    """
    Парсер для сайта Sportschecker.net с постоянной сессией и "человеческим" поведением.
    """
    def __init__(self, login, password):
        self.login = login
        self.password = password
        self.driver = None
        self.login_url = "https://ru.sportschecker.net/users/sign_in"
        self.valuebets_url = "https://ru.sportschecker.net/valuebets"
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        ]
        self.cookies_file = "cookies.json"
        self.last_login_fail_time = 0
        self.first_session = not os.path.exists(self.cookies_file)
        self.user_data_dir = None

    def _random_delay(self, min_seconds=1, max_seconds=3):
        """Создает случайную задержку."""
        time.sleep(random.uniform(min_seconds, max_seconds))

    def _save_screenshot(self, filename="screenshot_error.png"):
        """Сохраняет скриншот в папку 'screenshots'."""
        if not self.driver:
            return
        try:
            if not os.path.exists("screenshots"):
                os.makedirs("screenshots")
            screenshot_path = os.path.join("screenshots", filename)
            self.driver.save_screenshot(screenshot_path)
            logger.info(f"Скриншот сохранен: {screenshot_path}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении скриншота: {e}")

    def _is_driver_alive(self):
        """Проверяет, жив ли еще драйвер и открыт ли браузер."""
        if not self.driver:
            return False
        try:
            _ = self.driver.window_handles
            return True
        except WebDriverException:
            return False

    def _is_logged_in(self):
        """Проверяет, авторизован ли пользователь."""
        try:
            logout_link_selector = (By.CSS_SELECTOR, 'a[href="/users/sign_out"]')
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(logout_link_selector)
            )
            logger.info("Пользователь авторизован")
            return True
        except TimeoutException:
            logger.warning("Пользователь не авторизован")
            return False

    def _check_concurrent_session_error(self):
        """Проверяет наличие ошибки одновременного использования аккаунта."""
        try:
            error_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Учётная запись уже используется')]")
            if error_elements:
                logger.error("Обнаружена ошибка одновременного использования аккаунта")
                return True
        except:
            pass
        return False

    def _save_cookies(self):
        """Сохраняет куки в файл."""
        try:
            with open(self.cookies_file, 'w') as f:
                json.dump(self.driver.get_cookies(), f)
            logger.info("Куки успешно сохранены")
        except Exception as e:
            logger.error(f"Ошибка при сохранении куки: {e}")

    def _load_cookies(self):
        """Загружает куки из файла."""
        try:
            if os.path.exists(self.cookies_file):
                with open(self.cookies_file, 'r') as f:
                    cookies = json.load(f)
                
                self.driver.get(self.login_url)
                self.driver.delete_all_cookies()
                
                for cookie in cookies:
                    if 'expiry' in cookie:
                        cookie['expiry'] = int(cookie['expiry'])
                    try:
                        self.driver.add_cookie(cookie)
                    except:
                        continue
                
                logger.info("Куки успешно загружены")
                return True
        except Exception as e:
            logger.error(f"Ошибка при загрузке куки: {e}")
        return False

    def _setup_driver(self):
        """Настраивает и возвращает новый драйвер Chrome."""
        try:
            # Создаем уникальную временную директорию для профиля
            self.user_data_dir = tempfile.mkdtemp(prefix='chrome_profile_')
            
            options = webdriver.ChromeOptions()
            options.add_argument(f'--user-data-dir={self.user_data_dir}')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--headless')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--remote-debugging-port=0')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument(f'--user-agent={random.choice(self.user_agents)}')
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            
            logger.info("Драйвер успешно запущен")
            return driver
            
        except Exception as e:
            logger.error(f"Ошибка при запуске драйвера: {e}")
            if self.user_data_dir and os.path.exists(self.user_data_dir):
                shutil.rmtree(self.user_data_dir, ignore_errors=True)
            return None

    def _cleanup_driver(self):
        """Очищает ресурсы драйвера."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
        
        if self.user_data_dir and os.path.exists(self.user_data_dir):
            try:
                shutil.rmtree(self.user_data_dir, ignore_errors=True)
            except:
                pass
            self.user_data_dir = None

    def _perform_full_login(self):
        """Выполняет полный цикл входа."""
        if self.last_login_fail_time > 0 and (time.time() - self.last_login_fail_time) < 300:
            logger.info("Пауза после неудачного входа")
            return False

        logger.info("Выполняется полный цикл входа...")
        
        # Очищаем старый драйвер
        self._cleanup_driver()
        
        # Создаем новый драйвер
        self.driver = self._setup_driver()
        if not self.driver:
            self.last_login_fail_time = time.time()
            return False

        try:
            # Выполняем вход
            self.driver.get(self.login_url)
            self._random_delay(3, 5)

            # Ждем форму входа
            WebDriverWait(self.driver, 30).until(
                EC.visibility_of_element_located((By.ID, 'user_email'))
            )
            
            # Вводим логин
            email_field = self.driver.find_element(By.ID, 'user_email')
            email_field.clear()
            for char in self.login:
                email_field.send_keys(char)
                self._random_delay(0.1, 0.3)
            
            self._random_delay(1, 2)
            
            # Вводим пароль
            password_field = self.driver.find_element(By.ID, 'user_password')
            password_field.clear()
            for char in self.password:
                password_field.send_keys(char)
                self._random_delay(0.1, 0.3)
            
            self._random_delay(1, 2)
            
            # Нажимаем кнопку входа
            login_button = self.driver.find_element(By.ID, 'sign-in-form-submit-button')
            login_button.click()
            
            self._random_delay(3, 5)
            
            # Проверяем ошибки
            if self._check_concurrent_session_error():
                logger.error("Ошибка одновременного использования")
                self.last_login_fail_time = time.time()
                return False
            
            # Ждем подтверждения входа
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href="/users/sign_out"]'))
            )
            
            # Сохраняем куки
            self._save_cookies()
            self.first_session = False
            logger.info("Успешный вход на сайт")
            
            return True

        except Exception as e:
            logger.error(f"Ошибка во время входа: {e}")
            self._save_screenshot("login_error.png")
            self.last_login_fail_time = time.time()
            return False

    def _restore_session_with_cookies(self):
        """Восстанавливает сессию с помощью куки."""
        if not self._is_driver_alive():
            self.driver = self._setup_driver()
            if not self.driver:
                return False

        try:
            if self._load_cookies():
                self.driver.get(self.valuebets_url)
                self._random_delay(3, 5)
                
                if self._check_concurrent_session_error():
                    return False
                
                if self._is_logged_in():
                    logger.info("Сессия восстановлена с помощью куки")
                    return True
                else:
                    logger.warning("Куки устарели")
                    return False
            else:
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при восстановлении сессии: {e}")
            return False

    def get_predictions(self):
        """Основной метод для получения прогнозов."""
        try:
            # Управление сессией
            if self.first_session:
                if not self._perform_full_login():
                    return []
            else:
                if not self._restore_session_with_cookies():
                    if not self._perform_full_login():
                        return []

            # Переходим на страницу со ставками
            if self.driver.current_url != self.valuebets_url:
                self.driver.get(self.valuebets_url)
                self._random_delay(3, 5)

            # Обновляем таблицу
            try:
                filter_button = WebDriverWait(self.driver, 15).until(
                    EC.element_to_be_clickable((By.ID, 'ft'))
                )
                filter_button.click()
                self._random_delay(3, 5)
            except:
                logger.warning("Не удалось нажать кнопку фильтра")

            # Имитируем поведение пользователя
            self.driver.execute_script("window.scrollTo(0, 500);")
            self._random_delay(1, 2)
            self.driver.execute_script("window.scrollTo(0, 0);")
            self._random_delay(1, 2)

            # Парсим таблицу
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.ID, 'valuebets-table'))
            )
            
            table_rows = self.driver.find_elements(By.CSS_SELECTOR, '#valuebets-table > tbody.valuebet_record')
            
            if not table_rows:
                logger.info("Таблица пуста")
                return []

            predictions = []
            for row in table_rows:
                try:
                    # Парсим данные из строки
                    bookmaker_elem = row.find_element(By.CSS_SELECTOR, 'td.booker a')
                    bookmaker = bookmaker_elem.text.strip()
                    
                    # Ищем спорт
                    sport = ""
                    minor_spans = row.find_elements(By.CSS_SELECTOR, 'span.minor')
                    for span in minor_spans:
                        text = span.text.strip()
                        if '(' not in text and ')' not in text:
                            sport = text
                            break
                    
                    # Дата и время
                    date_elem = row.find_element(By.CSS_SELECTOR, 'td.time')
                    date = date_elem.text.strip().replace('\n', ' ')
                    
                    # Команды и турнир
                    event_elem = row.find_element(By.CSS_SELECTOR, 'td.event')
                    teams = event_elem.find_element(By.TAG_NAME, 'a').text.strip()
                    tournament = event_elem.find_element(By.TAG_NAME, 'span').text.strip()
                    
                    # Прогноз и коэффициенты
                    prediction_elem = row.find_element(By.CSS_SELECTOR, 'td.coeff')
                    prediction = prediction_elem.text.strip()
                    
                    odd_elem = row.find_element(By.CSS_SELECTOR, 'td.value')
                    odd = odd_elem.text.strip()
                    
                    value_elem = row.find_element(By.CSS_SELECTOR, 'td span.overvalue')
                    value = value_elem.text.strip()

                    predictions.append({
                        'bookmaker': bookmaker,
                        'sport': sport,
                        'date': date,
                        'tournament': tournament,
                        'teams': teams,
                        'prediction': prediction,
                        'odd': odd,
                        'value': value
                    })

                except Exception as e:
                    logger.warning(f"Ошибка парсинга строки: {e}")
                    continue
            
            logger.info(f"Спарсено {len(predictions)} прогнозов")
            return predictions

        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            self._save_screenshot("critical_error.png")
            return []

    def close(self):
        """Закрывает парсер и очищает ресурсы."""
        self._cleanup_driver()
        logger.info("Парсер закрыт")

# Пример использования
if __name__ == "__main__":
    # Инициализация парсера
    parser = SportscheckerParser('kosyakovsn@gmail.com', 'SC22332233')
    
    try:
        # Получение прогнозов
        predictions = parser.get_predictions()
        
        # Вывод результатов
        if predictions:
            print(f"Получено {len(predictions)} прогнозов:")
            for i, pred in enumerate(predictions, 1):
                print(f"{i}. {pred['teams']} - {pred['prediction']} ({pred['value']})")
        else:
            print("Прогнозы не найдены")
    
    finally:
        # Закрытие парсера
        parser.close()