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
        # Instruments table
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Instruments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            namn VARCHAR(150),
            typ VARCHAR(75),
            isin VARCHAR(75)
            )"""
        )

        # Persons table
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Persons (
            id INT AUTO_INCREMENT PRIMARY KEY,
            namn VARCHAR(150)
            )"""
        )

        # Transactions Table
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Transactions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            pub_datum DATE,
            emittent VARCHAR(100),
            person_id INT,
            befattning VARCHAR(175),
            narstaende VARCHAR(100),
            karaktar VARCHAR(100),
            instrument_id INT,
            datum DATE,
            volym INT,
            volymenhet VARCHAR(50),
            pris DECIMAL(14,6),
            valuta VARCHAR(25),
            FOREIGN KEY (id) REFERENCES Persons(id),
            FOREIGN KEY (id) REFERENCES Instruments(id)
            )"""
        )

    ###
    #   Close both the cursor and connection to the database
    ###
    def close(self):
        self.cursor.close()
        self.conn.close()
