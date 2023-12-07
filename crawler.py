import re
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
        __init__(self, db_file_name, init_db) - инициализация паука и БД, открытие соединения с БД
        __del__(self) - закрытие соединения с БД
        dbcommit(self) - зафиксировать изменения в БД
        crawl(self, url_list, max_depth=1) - обход страниц в ширину по заданной глубине, сбор и индексация данных
        dbinit(self) - создать БД
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

cr = Crawler('temp.db', init_db=True)