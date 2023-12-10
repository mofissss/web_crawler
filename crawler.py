import bs4
import requests
from bs4 import BeautifulSoup
import sqlite3
import time


class Crawler:
    """
    Crawler: Класс поискового паука

    Атрибуты:
        connection: sqlite3.Connection - соединение с БД
        cursor: sqlite3.Cursor - интерфейс для перемещения по БД

    Методы:
        __init__(self, db_file_name, init_db) - инициализирует паука и открывает соединение с БД
        __del__(self) - закрывает соединение с БД
        dbcommit(self) - фиксирует изменения в БД
        dbinit(self) - создаёт базу данных
        crawl(self, url_list, max_depth=1) - обход страниц в ширину по заданной глубине, сбор и индексация данных
        get_url_html(self, html_code) - парсит HTML-код, удаляет лишние тэги и возвращает готовый HTML-код
        get_url_text(self, soup) - получает текст страницы и возвращает его в виде списка
        add_index(self, url_text, url) - индексирует страницу
    """

    def __init__(self, db_file_name: str, *args: list, init_db: bool = False) -> None:
        """
        Инициализирует паука и открывает соединение с БД

        Параметры:
            db_file_name: str - имя БД
            init_db: bool - флаг для создания БД
        """
        self.connection = sqlite3.connect(db_file_name)
        self.cursor = self.connection.cursor()
        print(f"{time.strftime('%H:%M:%S')}: Соединение с {db_file_name} открыто")
        if init_db:
            self.dbinit()

    def __del__(self) -> None:
        """Закрывает соединение с БД"""
        self.connection.close()
        print(f"{time.strftime('%H:%M:%S')}: Соединение с базой данных закрыто")

    def dbcommit(self) -> None:
        """Фиксирует изменения в БД"""
        self.connection.commit()

    def dbinit(self) -> None:
        """Создаёт базу данных"""
        self.cursor.execute("""DROP TABLE IF EXISTS word_list;""")
        self.cursor.execute("""DROP TABLE IF EXISTS url_list;""")
        self.cursor.execute("""DROP TABLE IF EXISTS word_location;""")
        self.cursor.execute("""DROP TABLE IF EXISTS link_between_url;""")
        self.cursor.execute("""DROP TABLE IF EXISTS link_word;""")

        self.cursor.execute("""CREATE TABLE word_list 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    word TEXT NOT NULL, -- слово
                                    is_filtered INTEGER -- флаг фильтрации
                                );""")
        self.cursor.execute("""CREATE TABLE url_list 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    url TEXT NOT NULL -- ссылка
                                );""")
        self.cursor.execute("""CREATE TABLE word_location 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    fk_word_id INTEGER NOT NULL, -- ключ из таблицы word_list
                                    fk_url_id INTEGER NOT NULL, -- ключ из таблицы url_list
                                    location INTEGER NOT NULL, -- индекс
                                    FOREIGN KEY (fk_word_id) REFERENCES word_list(id) 
                                    FOREIGN KEY (fk_url_id) REFERENCES url_list(id) 
                                );""")
        self.cursor.execute("""CREATE TABLE link_between_url 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    fk_from_url_id INTEGER NOT NULL, -- ключ из таблицы url_list
                                    fk_to_url_id INTEGER NOT NULL, -- ключ из таблицы url_list
                                    FOREIGN KEY (fk_from_url_id) REFERENCES url_list(id) 
                                    FOREIGN KEY (fk_to_url_id) REFERENCES url_list(id) 
                                );""")
        self.cursor.execute("""CREATE TABLE link_word 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    fk_word_id INTEGER NOT NULL, -- ключ из таблицы word_list
                                    fk_link_id INTEGER NOT NULL, -- ключ из таблицы url_list
                                    FOREIGN KEY (fk_word_id) REFERENCES word_list(id)
                                    FOREIGN KEY (fk_link_id) REFERENCES url_list(id)
                                );""")
        self.dbcommit()
        print(f"{time.strftime('%H:%M:%S')}: База данных создана")

    def crawl(self, url_list: list, max_depth: int = 1) -> None:
        """
        Собирает данные с страниц, начиная с заданного списка страниц,
        выполняет поиск в ширину до заданной глубины,
        индексируя все встречающиеся по пути страницы

        Параметры:
            url_list: list - список сайтов для обхода
            max_depth: int - глубина поиска
        """

        for current_depth in range(max_depth):
            print(f"{time.strftime('%H:%M:%S')}: Обход страниц: глубина {current_depth + 1}")
            new_url_list = set()

            for url in url_list:
                print(f"{time.strftime('%H:%M:%S')}: Обход {url}")
                try:
                    res = requests.get(url, timeout=10)
                    res.encoding = 'utf-8'
                    print(f"{time.strftime('%H:%M:%S')}: 200 - Доступ к ресурсу {url} был получен")
                except Exception as error:
                    print(f"{time.strftime('%H:%M:%S')}: 400 - Не удалось получить доступ к ресурсу: {url}")
                    print(error)
                    continue

                url_html = self.get_url_html(res.text)
                url_text = self.get_url_text(url_html)
                self.add_index(url_text, url)


    def get_url_html(self, html_code: str) -> bs4.BeautifulSoup:
        """
        Парсит HTML-код, удаляет лишние тэги и возвращает готовый HTML-код

        Параметры:
            html_code: str - исходный HTML-код

        Возвращаемые объекты:
            soup: bs4.BeautifulSoup - HTML-код страницы без лишних тэгов
        """
        list_unwanted_items = ['head', 'script', 'noscript', 'img', 'header', 'meta', 'footer', 'button']
        soup = BeautifulSoup(html_code, 'lxml')
        for tag in soup.find_all(list_unwanted_items):
            tag.decompose()

        return soup

    def get_url_text(self, soup: bs4.BeautifulSoup) -> list:
        """
        Получает текст страницы и возвращает его в виде списка

        Параметры:
            soup: bs4.BeautifulSoup - HTML-код страницы

        Возвращаемые объекты:
            text: list - список всех слов с страницы
        """
        text = []
        for word in soup.text.split():
            word = word.strip('«»—-().,!/\\\"\'!?#@:;*')
            if not word.isdigit() and word:
                text.append(word.lower())
        print(f"{time.strftime('%H:%M:%S')}: Получен текст страницы")

        return text

    def add_index(self, url_text: list, url: str) -> None:
        """
        Индексирует страницу

        Параметры:
            url_text: str - текст страницы
            url: str - адрес страницы
        """


cr = Crawler('temp.db')
# cr.crawl(['https://habr.com/ru/news/779568/'])
print(Crawler.__doc__)