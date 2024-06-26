import os
import mysql.connector

from scrapy.exceptions import DropItem
from dotenv import load_dotenv

load_dotenv()


class DataCleansePipeline:
    def __init__(self):
        pass



    """OPEN SPIDER"""

    def open_spider(self, spider):
        """Method called when the spider is opened"""
        pass



    """PROCESS ITEM"""

    def process_item(self, item, spider):
        """Method called for every item pipeline component"""
        item["name"] = self.remove_duplicate_spaces(item["name"])
        item["role"] = self.remove_xa0(item["role"])

        item["volume"] = self.remove_xa0(item["volume"])
        item["volume"] = self.swap_decimal(item["volume"])

        item["price"] = self.remove_all_spaces(item["price"])
        item["price"] = self.swap_decimal(item["price"])

        item["related"] = self.handle_related_none(item["related"])

        item["status"] = self.handle_status_none(item["status"])

        return item

    def remove_duplicate_spaces(self, field):
        """Removes all duplicated spaces

        Args:
            item (str): item field that should be converted

        Returns:
            str: converted string
        """
        return " ".join(field.split())

    def remove_all_spaces(self, field):
        """Removes all spaces from a string

        Args:
            field (str): item field that should be converted

        Returns:
            str: converted string
        """
        temp = field.replace(" ", "")
        return self.remove_xa0(temp)

    def remove_xa0(self, field):
        """Renoves \xa0 in a string

        Args:
              field (str): item field that should be converted

          Returns:
              str: converted string
        """
        try:
            return field.replace("\xa0", "")
        except:
            return field.replace("\xa0", " ")

    def swap_decimal(self, field):
        """Swaps all decimals to dots

        Args:
             field (str): item field that should be converted

         Returns:
             str: converted string
        """
        return field.replace(",", ".")

    def handle_related_none(self, field):
        """Unifies all false boolean values

        Args:
              field (str): item field that should be converted

          Returns:
              str: converted string
        """
        if field is None:
            field = "Nej"

        return field

    def handle_status_none(self, field):
        """Unifies all None boolean values

        Args:
              field (str): item field that should be converted

          Returns:
              str: converted string
        """
        if field is None:
            return "NaN"
        else:
            return field



    """CLOSE SPIDER"""

    def close_spider(self, spider):
        """Method called when the spider is closed"""
        pass


class MySqlPipeline:
    def __init__(self):
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.database = os.getenv("DB_SCHEMA")
        self.conn = None
        self.cursor = None



    """OPEN SPIDER"""

    def open_spider(self, spider):
        """Method called when the spider is opened"""
        self.check_db_exists()
        self.create_db_connection()
        self.add_db_tables()

    def check_db_exists(self):
        """Checks if the database exists and creates it if not."""
        try:
            db_conn = mysql.connector.connect(
                user=self.user,
                password=self.password,
                host=self.host,
            )
            cursor = db_conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def create_db_connection(self):
        """Creates a connection to the database."""
        try:
            self.conn = mysql.connector.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
            )
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def add_db_tables(self):
        """Create necessary tables in the database."""
        self.create_instruments_table()
        self.create_roles_table()
        self.create_people_table()
        self.create_dates_table()
        self.create_currencies_table()
        self.create_transactions_table()

    def create_instruments_table(self):
        """Create instruments table if not exists."""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Instruments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(200),
            type VARCHAR(75),
            isin VARCHAR(40) UNIQUE,
            issuer VARCHAR(255)
            )"""
        )

    def create_roles_table(self):
        """Create roles table if not exists."""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Roles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            role VARCHAR(255)
            )"""
        )

    def create_people_table(self):
        """Create people table if not exists."""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS People (
            id INT AUTO_INCREMENT PRIMARY KEY,
            role_id INT,
            name VARCHAR(255),
            related BOOLEAN,
            FOREIGN KEY (role_id) REFERENCES Roles(id)
            )"""
        )

    def create_dates_table(self):
        """Create dates table if not exists."""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Dates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE UNIQUE
            )"""
        )

    def create_currencies_table(self):
        """Create currencies table if not exists."""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Currencies (
            id INT AUTO_INCREMENT PRIMARY KEY,
            currency VARCHAR(10) UNIQUE
            )"""
        )

    def create_transactions_table(self):
        """Create transactions table if not exists."""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Transactions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            people_id INT,
            instrument_id INT,
            purchase_date_id INT,
            publication_date_id INT,
            nature_of_purchase VARCHAR(100),
            volume INT,
            volume_unit VARCHAR(50),
            price DECIMAL(14, 6),
            currency_id INT,
            FOREIGN KEY (people_id) REFERENCES People(id),
            FOREIGN KEY (instrument_id) REFERENCES Instruments(id),
            FOREIGN KEY (purchase_date_id) REFERENCES Dates(id),
            FOREIGN KEY (publication_date_id) REFERENCES Dates(id),
            FOREIGN KEY (currency_id) REFERENCES Currencies(id)
            )"""
        )



    """PROCESS ITEM"""

    def process_item(self, item, spider):
        """Method called for every item pipeline component"""
        self.dates_entries(item)
        self.instruments_entries(item)
        return item

    def instruments_entries(self, item):
        """Inserts a record into the item table

        Args:
            item (scrapy.Item): The currently scraped item

        Raises:
            DropItem: Item could not be inserted into table
        """
        self.cursor.execute(
            f"""SELECT * FROM Instruments WHERE isin = %s""", (item["isin"],)
        )
        current_isin_exits = self.cursor.fetchone()

        if not current_isin_exits:
            try:
                self.cursor.execute(
                    f"""
                    INSERT INTO Instruments
                    (name, type, isin, issuer)
                    VALUES
                    (%s, %s, %s, %s)""",
                    (
                        item["instrument_name"],
                        item["instrument_type"],
                        item["isin"],
                        item["issuer"],
                    ),
                )

                self.conn.commit()

            except mysql.connector.Error as err:
                raise DropItem(f"Error at Instruements, inserting: {err}")

    def dates_entries(self, item):
        """Inserts a record into the dates table

        Args:
            item (scrapy.Item): The currently scraped item

        Raises:
            DropItem: Item could not be inserted into table
        """
        self.cursor.execute(
            f"""SELECT * FROM Dates WHERE date = %s""", (item["publication_date"],)
        )
        current_date_exits = self.cursor.fetchone()

        if not current_date_exits:
            try:
                self.cursor.execute(
                    f"""
                    INSERT INTO Dates
                    (date)
                    VALUES
                    (%s)""",
                    (item["publication_date"],),
                )

                self.conn.commit()

            except mysql.connector.Error as err:
                raise DropItem(f"Error at Instruements, inserting: {err}")



    """CLOSE SPIDER"""

    def close_spider(self, spider):
        """Method called when the spider is closed"""
        self.close_db_connection()

    def close_db_connection(self):
        """Close both the cursor and the connection to the database."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except mysql.connector.Error as err:
            print(f"Error closing connection: {err}")
