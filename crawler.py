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

    Методы:
        __init__(self, db_file_name) - инициализация паука и открытие соединения с БД
        __del__(self) - закрытие соединения с БД
        dbcommit(self) - зафиксировать изменения в БД
    """

    def __init__(self, db_file_name: str) -> None:
        """
        Инициализирует паука и открывает соединение с БД

            Параметры:
                dbFileName: str - имя БД
        """
        self.connection = sqlite3.connect(db_file_name)
        print(f"{time.strftime('%H:%M:%S')}: Соединение с {db_file_name} открыто")

    def __del__(self) -> None:
        """Закрывает соединение с БД"""
        self.connection.close()
        print(f"{time.strftime('%H:%M:%S')}: Соединение с базой данных закрыто")

    def dbcommit(self) -> None:
        """Зафиксировать изменения в БД"""
        self.connection.commit()