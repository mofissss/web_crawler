import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re


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
        dbinit(self) - создаёт БД
        crawl(self, url_list, max_depth=1) - обход страниц в ширину по заданной глубине, сбор и индексация данных
        get_url_html(self, html_code) - парсит HTML-код, удаляет лишние тэги и возвращает готовый HTML-код
        get_url_text(self, soup) - получает текст страницы и возвращает его в виде списка
        get_id(self, table_name, field_name, value) - получение id объекта из таблицы и добавление в таблицу (если объект не записан)
        add_index(self, url_text, url) - индексирует страницу
        is_indexed(self, url) - Проверяет на наличие адреса страницы в БД
        add_link_ref(self, url_from, url_to, link_text) - добавляет в БД связь между страницами
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
        self.cursor.execute("""DROP TABLE IF EXISTS word_list""")
        self.cursor.execute("""DROP TABLE IF EXISTS url_list""")
        self.cursor.execute("""DROP TABLE IF EXISTS word_location""")
        self.cursor.execute("""DROP TABLE IF EXISTS link_between_url""")
        self.cursor.execute("""DROP TABLE IF EXISTS link_word""")

        self.cursor.execute("""CREATE TABLE word_list 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    word TEXT NOT NULL, -- слово
                                    is_filtered INTEGER -- флаг фильтрации
                                )""")
        self.cursor.execute("""CREATE TABLE url_list 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    url TEXT NOT NULL -- ссылка
                                )""")
        self.cursor.execute("""CREATE TABLE word_location 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    fk_word_id INTEGER NOT NULL, -- ключ из таблицы word_list
                                    fk_url_id INTEGER NOT NULL, -- ключ из таблицы url_list
                                    location INTEGER NOT NULL, -- индекс
                                    FOREIGN KEY (fk_word_id) REFERENCES word_list(id) 
                                    FOREIGN KEY (fk_url_id) REFERENCES url_list(id) 
                                )""")
        self.cursor.execute("""CREATE TABLE link_between_url 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    fk_from_url_id INTEGER NOT NULL, -- ключ из таблицы url_list
                                    fk_to_url_id INTEGER NOT NULL, -- ключ из таблицы url_list
                                    FOREIGN KEY (fk_from_url_id) REFERENCES url_list(id) 
                                    FOREIGN KEY (fk_to_url_id) REFERENCES url_list(id) 
                                )""")
        self.cursor.execute("""CREATE TABLE link_word 
                                (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT, -- первичный ключ
                                    fk_word_id INTEGER NOT NULL, -- ключ из таблицы word_list
                                    fk_link_id INTEGER NOT NULL, -- ключ из таблицы url_list
                                    FOREIGN KEY (fk_word_id) REFERENCES word_list(id)
                                    FOREIGN KEY (fk_link_id) REFERENCES url_list(id)
                                )""")
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
                print(f"{time.strftime('%H:%M:%S')}: Обход страницы - {url}")
                try:
                    res = requests.get(url, timeout=10)
                    res.encoding = 'utf-8'
                    print(f"{time.strftime('%H:%M:%S')}: 200 - Доступ к ресурсу {url} был получен")
                except Exception as error:
                    print(f"{time.strftime('%H:%M:%S')}: 400 - Не удалось получить доступ к ресурсу: {url}")
                    print(error)
                    continue

                soup = self.get_url_html(res.text)
                url_text = self.get_url_text(soup)
                self.add_index(url_text, url)

                for link in soup.find_all('a'):
                    if 'href' not in link.attrs:
                        continue

                    new_link = link['href']
                    if not new_link:
                        continue

                    if new_link[0] == '/':  # формирование ссылки при относительной ссылке
                        new_link = re.search(r'https://[a-z]+(?:\.[a-z]+)*', url)[0] + new_link

                    if new_link[:5] == 'https' and not self.is_indexed(new_link):
                        new_url_list.add(new_link)
                        link_text = self.get_url_text(link)

                        if link_text is None:
                            continue

                        self.add_link_ref(url, new_link, link_text)

            print(f"{time.strftime('%H:%M:%S')}: Получены все ссылки с страницы")
            url_list = new_url_list
            self.dbcommit()
            print(f"{time.strftime('%H:%M:%S')}: Обход в глубину {current_depth + 1} - завершен")

    def get_url_html(self, html_code: str) -> BeautifulSoup:
        """
        Парсит HTML-код, удаляет лишние тэги и возвращает готовый HTML-код

        Параметры:
            html_code: str - исходный HTML-код

        Возвращаемые объекты:
            soup: BeautifulSoup - HTML-код страницы без лишних тэгов
        """
        list_unwanted_items = ['head', 'script', 'noscript', 'img', 'header', 'meta', 'footer', 'button']
        soup = BeautifulSoup(html_code, 'lxml')
        for tag in soup.find_all(list_unwanted_items):
            tag.decompose()

        return soup

    def get_url_text(self, soup: BeautifulSoup) -> list:
        """
        Получает текст страницы и возвращает его в виде списка

        Параметры:
            soup: BeautifulSoup - HTML-код страницы

        Возвращаемые объекты:
            text: list - список всех слов с страницы
        """
        text = []
        for word in soup.text.split():
            word = word.strip('«»—-().,!/\\\"\'!?#@:;*')
            if not word.isdigit() and word:
                text.append(word.lower())

        return text

    def get_id(self, table_name: str, field_name: str, value: str) -> int:
        """
        Получение id объекта из таблицы и добавление в таблицу (если объект не записан)

        Параметры:
            table_name: str - название таблицы
            field_name: str - название атрибута
            value: str - искомое значение

        Возвращаемые значения:
            id: int - идентификатор записи
        """
        value = value.replace('\'', '\'\'')  # добавляем экранирование для '
        sql_query = self.cursor.execute(f"""SELECT id FROM {table_name} WHERE {field_name}='{value}'""").fetchone()
        if sql_query is not None:
            return sql_query[0]
        else:
            req_sql = self.cursor.execute(f"""INSERT INTO {table_name} ({field_name}) VALUES ('{value}')""")
            self.dbcommit()
            return req_sql.lastrowid

    def add_index(self, url_text: list, url: str) -> None:
        """
        Индексирует страницу

        Параметры:
            url_text: str - текст страницы
            url: str - адрес страницы
        """
        url_id = self.get_id('url_list', 'url', url)
        sql_query = self.cursor.execute(f"""SELECT COUNT(*) FROM word_location WHERE fk_url_id = {url_id}""").fetchone()

        if sql_query[0] > 0 or not url_text:
            return None

        for i in range(len(url_text)):
            word = url_text[i]
            word_id = self.get_id('word_list', 'word', word)
            self.cursor.execute(
                f"""INSERT INTO word_location(fk_word_id, fk_url_id, location) VALUES ({word_id}, {url_id}, {i})""")
        self.dbcommit()

        print(f"{time.strftime('%H:%M:%S')}: Страница проиндексирована")

    def is_indexed(self, url: str) -> bool:
        """
        Проверяет на наличие адреса страницы в базе данных

        Параметры:
            url: str - адрес страницы
        """
        sql_query = self.cursor.execute(f"""SELECT id FROM url_list WHERE url='{url}'""").fetchone()
        if sql_query is not None:
            return True
        return False

    def add_link_ref(self, url_from: str, url_to: str, link_text: list) -> None:
        """
        Добавляет в БД связь между страницами

        Параметры:
            url_from: str - адрес родительской страницы
            url_to: str - адрес дочерней страницы
            link_text: list - список слов ссылки
        """
        url_from_id = self.get_id('url_list', 'url', url_from)
        url_to_id = self.get_id('url_list', 'url', url_to)

        sql_query = self.cursor.execute(f"""SELECT id FROM link_between_url 
                                            WHERE fk_from_url_id='{url_from_id}' AND fk_to_url_id='{url_to_id}'""").fetchone()

        if sql_query is not None:
            url_id = sql_query[0]
        else:
            sql_query = self.cursor.execute(
                f"""INSERT INTO link_between_url(fk_from_url_id, fk_to_url_id) VALUES('{url_from_id}','{url_to_id}')""")
            self.dbcommit()
            url_id = sql_query.lastrowid

        for i in range(len(link_text)):
            word = link_text[i]
            word_id = self.get_id('word_list', 'word', word)
            self.cursor.execute(
                f"""INSERT INTO link_word(fk_word_id, fk_link_id) VALUES ('{word_id}', '{url_id}');""")
        self.dbcommit()
