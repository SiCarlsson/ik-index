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
        # Non-dependet tables
        self.create_companies_table()
        self.create_roles_table()
        self.create_dates_table()
        self.create_currencies_table()

        # Dependet tables
        self.create_instruments_table()
        self.create_people_table()

        # Multi-dependet tables
        self.create_transactions_table()

    def create_instruments_table(self):
        """Create instruments table if not exists."""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Instruments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT,
            name VARCHAR(200),
            type VARCHAR(75),
            isin VARCHAR(40) UNIQUE,
            FOREIGN KEY (company_id) REFERENCES Companies(id)
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

    def create_companies_table(self):
        """Create people table if not exists."""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Companies (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
            )"""
        )

    def create_people_table(self):
        """Create people table if not exists."""
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS People (
            id INT AUTO_INCREMENT PRIMARY KEY,
            role_id INT,
            company_id INT,
            name VARCHAR(255),
            FOREIGN KEY (role_id) REFERENCES Roles(id),
            FOREIGN KEY (company_id) REFERENCES Companies(id)
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
            related VARCHAR(20),
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
        # Non-dependet tables
        self.dates_entries(item)
        self.curerncies_entries(item)
        self.roles_entries(item)
        self.companies_entries(item)

        # Dependet tables
        self.instruments_entries(item)
        self.people_entries(item)

        # Multi-dependet tables
        self.transactions_entries(item)

        return item

    def companies_entries(self, item):
        """Inserts a record into the companies table

        Args:
            item (scrapy.Item): The currently scraped item

        Raises:
            DropItem: Item could not be inserted into table
        """
        self.cursor.execute(
            f"""SELECT * FROM Companies WHERE name = %s""", (item["issuer"],)
        )
        current_company_exits = self.cursor.fetchone()

        if not current_company_exits:
            try:
                self.cursor.execute(
                    f"""
                    INSERT INTO Companies
                    (name)
                    VALUES
                    (%s)""",
                    (item["issuer"],),
                )

                self.conn.commit()

            except mysql.connector.Error as err:
                raise DropItem(f"Error at Companies, inserting: {err}")

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
                    (company_id, name, type, isin)
                    VALUES
                    (%s, %s, %s, %s)""",
                    (
                        self.extract_company_id(item["issuer"]),
                        item["instrument_name"],
                        item["instrument_type"],
                        item["isin"],
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
                raise DropItem(f"Error at Dates, inserting: {err}")

    def curerncies_entries(self, item):
        """Inserts a record into the currencies table

        Args:
            item (scrapy.Item): The currently scraped item

        Raises:
            DropItem: Item could not be inserted into table
        """
        self.cursor.execute(
            f"""SELECT * FROM Currencies WHERE currency = %s""", (item["currency"],)
        )
        current_currency_exits = self.cursor.fetchone()

        if not current_currency_exits:
            try:
                self.cursor.execute(
                    f"""
                    INSERT INTO Currencies
                    (currency)
                    VALUES
                    (%s)""",
                    (item["currency"],),
                )

                self.conn.commit()

            except mysql.connector.Error as err:
                raise DropItem(f"Error at Currencies, inserting: {err}")

    def roles_entries(self, item):
        """Inserts a record into the currencies table

        Args:
            item (scrapy.Item): The currently scraped item

        Raises:
            DropItem: Item could not be inserted into table
        """
        self.cursor.execute(f"""SELECT * FROM Roles WHERE role = %s""", (item["role"],))
        current_role_exits = self.cursor.fetchone()

        if not current_role_exits:
            try:
                self.cursor.execute(
                    f"""
                    INSERT INTO Roles
                    (role)
                    VALUES
                    (%s)""",
                    (item["role"],),
                )

                self.conn.commit()

            except mysql.connector.Error as err:
                raise DropItem(f"Error at Roles, inserting: {err}")

    def people_entries(self, item):
        """Inserts a record into the people table

        Args:
            item (scrapy.Item): The currently scraped item

        Raises:
            DropItem: Item could not be inserted into table
        """
        self.cursor.execute(
            f"""SELECT * FROM People WHERE company_id = %s AND name = %s""",
            (self.extract_company_id(item["issuer"]), item["name"]),
        )
        current_role_exits = self.cursor.fetchone()

        if not current_role_exits:
            try:
                self.cursor.execute(
                    f"""
                    INSERT INTO People
                    (role_id, company_id, name)
                    VALUES
                    (%s, %s, %s)""",
                    (
                        self.extract_role_id(item["role"]),
                        self.extract_company_id(item["issuer"]),
                        item["name"],
                    ),
                )

                self.conn.commit()

            except mysql.connector.Error as err:
                raise DropItem(f"Error at People, inserting: {err}")

    def transactions_entries(self, item):
        """Inserts a record into the transactions table

        Args:
            item (scrapy.Item): The currently scraped item

        Raises:
            DropItem: Item could not be inserted into table
        """
        try:
            self.cursor.execute(
                f"""
                INSERT INTO Transactions
                (people_id, instrument_id)
                VALUES
                (%s, %s)""",
                (
                    self.extract_person_id(item["issuer"], item["name"]),
                    self.extract_instrument_id(
                        item["issuer"],
                        item["instrument_name"],
                        item["instrument_type"],
                        item["isin"],
                    ),
                ),
                # (people_id, instrument_id, purchase_date_id, publication_date_id, nature_of_purchase, related, volume, volume_unit, price, currency_id)
            )

            self.conn.commit()

        except mysql.connector.Error as err:
            raise DropItem(f"Error at Transactions, inserting: {err}")

    def extract_role_id(self, role):
        """Retrieves the role_id from the database corresponding to the current role

        Args:
            role (str): The role of the current item

        Returns:
            str: The role_id
        """
        self.cursor.execute(
            f"""SELECT id FROM Roles WHERE role = %s""",
            (role,),
        )
        return self.cursor.fetchone()[0]

    def extract_company_id(self, company_name):
        """Retrieves the company_id from the database corresponding to the current company name

        Args:
            role (str): The name of the issuer of the current item

        Returns:
            str: The company_id
        """
        self.cursor.execute(
            f"""SELECT id FROM Companies WHERE name = %s""",
            (company_name,),
        )
        return self.cursor.fetchone()[0]

    def extract_person_id(self, company_name, person_name):
        """Retrieves the people_id from the database corresponding to the current item

        Args:
            company_name (str): The name of the issuer of the current item
            person_name (str): The name of the person of the current item

        Returns:
            str: The person_id
        """
        self.cursor.execute(
            f"""SELECT id FROM People WHERE name = %s AND company_id = %s""",
            (person_name, self.extract_company_id(company_name)),
        )
        return self.cursor.fetchone()[0]

    def extract_instrument_id(self, company_name, name, type, isin):
        """Retrieves the people_id from the database corresponding to the current item

        Args:
            company_name (str): The name of the issuer of the current item
            name (str): The name of the instrument of the current item
            type (str): The type of the instrument of the current item
            isin (str): The isin of the current item

        Returns:
            str: The instrument_id
        """
        self.cursor.execute(
            f"""SELECT id FROM Instruments WHERE company_id = %s AND name = %s AND type = %s AND isin = %s""",
            (
                self.extract_company_id(company_name),
                name,
                type,
                isin,
            ),
        )
        return self.cursor.fetchone()[0]

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
