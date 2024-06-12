import os
import mysql.connector

from mysql.connector import errorcode
from scrapy.exceptions import DropItem
from dotenv import load_dotenv

load_dotenv()


class MySqlPipeline:

    def __init__(self):
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.database = os.getenv("DB_SCHEMA")

    def open_spider(self, spider):
        self.check_db_exists()

    def check_db_exists(self):
        """Checks if the database exists and creates it if not"""
        db_conn = mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
        )
        db_conn.cursor().execute(
            f"""CREATE DATABASE IF NOT EXISTS {os.getenv("DB_SCHEMA")}"""
        )

    def create_db_connection(self):
        """Creates a connection to the database"""
        self.conn = mysql.connector.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_SCHEMA"),
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        return item
