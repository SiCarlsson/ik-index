import os
import mysql.connector

from dotenv import load_dotenv

load_dotenv()


class Database:
    def __init__(self):
        self.create_conn()
        self.create_tables()

    ###
    #   Creates a batabase if it not does not exists
    ###
    def create_db(self):
        db_conn = mysql.connector.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )

        db_conn.cursor().execute(
            f"""CREATE DATABASE IF NOT EXISTS {os.getenv("DB_SCHEMA")}"""
        )

    ###
    #   Creates a connection and cursor with the database that is stored in the object
    ###
    def create_conn(self):
        # Makes sure that there is an existing database
        self.create_db()

        # Establishes wanted connections
        self.conn = mysql.connector.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_SCHEMA"),
        )
        self.cursor = self.conn.cursor()

    ###
    #   Create tables in database if not yet exists
    ###
    def create_tables(self):
        # Issuers table
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Issuers (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        # People table
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS People (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        # Roles Positions
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Roles (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        # Transactions Positions
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Transactions (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        # Dates Positions
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Dates (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        # Instruments Positions
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Instruments (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

        # Currencies Positions
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Currencies (
                id INT AUTO_INCREMENT PRIMARY KEY
            )"""
        )

    ###
    #   Close both the cursor and connection to the database
    ###
    def close(self):
        self.cursor.close()
        self.conn.close()
