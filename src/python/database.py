import os
import mysql.connector

from dotenv import load_dotenv

load_dotenv()

class Database:
    def __init__(self):
        self.create_conn()
        self.create_tables()

    def create_db(self):
        """Creates a batabase if it not does not exists"""
        db_conn = mysql.connector.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )

        db_conn.cursor().execute(
            f"""CREATE DATABASE IF NOT EXISTS {os.getenv("DB_SCHEMA")}"""
        )

    def create_conn(self):
        """Creates a connection and cursor with the database that is stored in the object"""
        self.create_db()
        self.establish_conn()
        self.cursor = self.conn.cursor()

    def establish_conn(self):
        """Establishes wanted connections"""
        self.conn = mysql.connector.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_SCHEMA"),
        )

    def create_tables(self):
        """Create tables in database if not yet exists"""

        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Issuers (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS People (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Roles (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Transactions (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Dates (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Instruments (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Currencies (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

    def close(self):
        """Close both the cursor and connection to the database"""
        self.cursor.close()
        self.conn.close()
