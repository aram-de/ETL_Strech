import csv
import logging
from datetime import date
from datetime import datetime
from datetime import timedelta
from io import StringIO

import boto3
import pandas as pd
import redshift_connector
import StringIO


col_names = [
    "timestamp",
    "store_name",
    "customer_name",
    "products",
    "total_price",
    "payment_method",
    "card_number",
]


bucket_name = "delon8-group5"
s3_client = boto3.client("s3", endpoint_url="https://s3.eu-west-1.amazonaws.com")


class HeadersWrongError(Exception):
    """A custom error class in case we are importing a file
    with the wrong headers"""

    pass


class ColumnsWrongError(Exception):

    """A custom error class in case we are importing a file
    with more or less columns than there should be"""

    pass


def handler(event, context):
    def check_if_number_of_columns_right(
        bucket_name: str, object_key: str, expected_cols: list
    ) -> bool:

        """This function takes a path to the file and a list with headers
        checks if the file has the same number of columns as the list of headers.
        Returns True if it does and false if it does not"""

        s3_resource = boto3.resource("s3")
        s3_object = s3_resource.Object(bucket_name, object_key)

        data = s3_object.get()["Body"].read().decode("utf-8").splitlines()

        return len(list(csv.reader(data))) == len(expected_cols)

    def check_headers_match_expected(
        bucket_name: str, object_key: str, expected_col_names: list = col_names
    ) -> bool:

        """This function takes in a file through file_path and expected column headers and outputs
        a Boolean True if all expected headers are present and are in the right order,
        or False if there are missing headers"""

        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        df_s3_data = pd.read_csv(response["Body"], sep=",")

        actual_column_names = list(df_s3_data.columns)
        return actual_column_names == expected_col_names

    yesterday = datetime.now() - timedelta(1)
    yesterday = str(yesterday)[0:10]
    yesterday = yesterday.replace("-", "/")
    yesterday = yesterday.replace("/0", "/")

    def collect_names_of_files_in_bucket(bucket_name: str) -> list:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=yesterday)
        content_list = response["Contents"]
        object_key_list = []
        for s3_object in content_list:
            object_key_list.append(s3_object["Key"])
        print(f"You have collected these files: {object_key_list}")
        return object_key_list

    # def check_if_file_logged(object_key):
    #     query_to_insert_file_name = f"INSERT INTO public.file_log(file_name) VALUES ('{object_key}');"
    # cursor = connection_to_db.cursor()
    # cursor.execute(query_to_insert_file_name)
    # connection_to_db.commit()

    # This version assumes there are no headers in the CSV files
    def turn_file_into_dataframe(bucket_name, object_key, col_names) -> pd.DataFrame:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        df_s3_data = pd.read_csv(response["Body"], sep=",", names=col_names)
        return df_s3_data

    # def check_if_headers_present(bucket_name, object_key) -> bool:

    #     """This function takes a path to the file and checks
    #     if the file has headers at all. Returns True if it does
    #     and false if it does not"""

    #     # s3 = boto3.resource('s3')
    #     # obj = s3.Object(bucket_name, object_key)
    #     # csvfile = obj.get()['Body'].read().decode('utf-8')
    #     #         # response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    #     # # csvfile = response['Body'].decode("utf-8")

    #     response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    #     dataframe = pd.read_csv(response["Body"], sep=",")
    #     buffer = StringIO.StringIO()
    #     dataframe.to_csv(buffer)
    #     sniffer = csv.Sniffer()
    #     has_header = sniffer.has_header(buffer.read(2048))
    #     return has_header

    # def turn_file_into_dataframe(
    #     bucket_name, object_key, col_names: list = col_names
    # ) -> pd.DataFrame:

    #     """This function takes in a file through a file_path and column headers that default to
    #     a value, in case the file has no headers and returns a dataframe with the headers provided
    #     , or errors"""

    #     try:
    #         if not check_if_number_of_columns_right(bucket_name, object_key, col_names):
    #             raise ColumnsWrongError(
    #                 "The number of columns in the file does not match the number of columns expected"
    #             )

    #         elif not check_if_headers_present(
    #             bucket_name, object_key
    #         ) or check_headers_match_expected(bucket_name, object_key, col_names):
    #             response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    #             dataframe = pd.read_csv(response["Body"], sep=",", names=col_names)
    #             return dataframe

    #         else:  # this happens if the number of columns is right and there are headers but they do not match our expected headers
    #             raise HeadersWrongError(
    #                 "The headers in the data do not match the expected headers"
    #             )
    #     except ColumnsWrongError as error:
    #         return error
    #     except HeadersWrongError as error:
    #         return error

    def splitting_products_column(df):
        df["products"] = df["products"].str.split(", ")
        col = "products"

        increment = 0
        for i in range(len(df[col])):
            current = df[col].iloc[i]

            # making seperate columns for price and size
            # increment = 0
            for x in current:
                # print(x.split()[1:-2])
                products_split_list = x.split()[1:-2]
                df.loc[increment:, "size"] = x.split()[0]
                df.loc[increment:, "product_type"] = " ".join(products_split_list)
                df.loc[increment:, "price"] = x.split()[-1]
                increment += 1
        return df

    def remove_columns_from_df(dataframe, columns_to_drop) -> pd.DataFrame:

        """Removes specified columns from a dataframe"""
        try:
            # dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
            dataframe.drop(columns_to_drop, axis=1, inplace=True)

            return dataframe
        except Exception as e:
            print(e)
            print("dataframe does not contain specified columns")

    def connect_to_database(connection):
        """
        function to connect the the database, has 1 arguemnt connection which is a dict for the
        parameters to connect to the database.
        If successful it will return conn
        """
        try:
            print("connecting")
            conn = redshift_connector.connect(**connection)
            print("Connection done")
        except Exception as e:
            print(e)
            print("not done")
        return conn

    connection = {
        "database": "NAMEOFYOURDATABASE",
        "user": "NAME OF YOUR USER",
        "password": "YOURPASSWORD",
        "host": "YOURHOST",
        "port": "YOURPORT",
    }
    connecting_to_db = connect_to_database(connection)

    def create_table(conn, query):
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            print("Query for table creation executed")
        except Exception as e:
            print(e)
            print("table not made")

    # CREATING TABLES FROM HERE

    # query_create_stores = '''CREATE TABLE IF NOT EXISTS stores (store_id INT IDENTITY(1,1) PRIMARY KEY, store_name VARCHAR(100) UNIQUE NOT NULL);'''
    # create_table(connecting_to_db,query_create_stores)

    # query_create_payment = '''CREATE TABLE IF NOT EXISTS payment_methods (
    # payment_id INT IDENTITY(1,1) PRIMARY KEY,
    # payment_method VARCHAR(100) UNIQUE NOT NULL);'''
    # create_table(connecting_to_db,query_create_payment)

    # #CHANGED MONEY TO NUMERIC(5, 2)
    # query_create_transaction = '''CREATE TABLE IF NOT EXISTS transactions (
    # transaction_id INT IDENTITY(1,1) PRIMARY KEY,
    # store_id INT,
    # timestamp TIMESTAMP NOT NULL,
    # total_price  NUMERIC(5,2) NOT NULL,
    # FOREIGN KEY(store_id)
    # REFERENCES stores(store_id)
    # );'''
    # create_table(connecting_to_db,query_create_transaction)

    # query_create_products = '''CREATE TABLE IF NOT EXISTS products(
    # product_id INT IDENTITY(1,1) PRIMARY KEY,
    # store_name VARCHAR(100) NOT NULL,
    # size VARCHAR(100) NOT NULL,
    # product_type VARCHAR (100) NOT NULL,
    # price NUMERIC(5,2) NOT NULL
    # ); '''
    # create_table(connecting_to_db,query_create_products)

    # END OF MAKING TABLES

    def insert_values_in_table(conn, df, table_used):
        """Loading data into DB"""

        tuples = [tuple(x) for x in df.to_numpy()]
        values_to_enter = str(tuples).strip("[]").replace(",)", ")")

        columns = ",".join(list(df.columns))
        query = f"INSERT INTO public.{table_used} ({columns}) VALUES {values_to_enter};"
        """Example query that should be generated
        INSERT INTO payment_methods(payment_method) VALUES ('CARD'),... SINGLE QUOTES ARE IMPORTANT IN THE VALUES"""
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            conn.commit()
        except (Exception, redshift_connector.DatabaseError) as error:
            print(error)

    list_of_files = collect_names_of_files_in_bucket(bucket_name)

    to_drop_products = ["customer_name", "card_number", "timestamp"]
    to_drop_stores = [
        "timestamp",
        "products",
        "total_price",
        "payment_method",
        "customer_name",
        "card_number",
    ]
    to_drop_payment = [
        "timestamp",
        "products",
        "total_price",
        "store_name",
        "customer_name",
        "card_number",
    ]
    to_drop_transaction = [
        "products",
        "store_name",
        "customer_name",
        "card_number",
        "payment_method",
    ]

    for object_key in list_of_files:
        # print(f"Headers for {object_key} are as expected",  check_headers_match_expected(bucket_name, object_key, col_names))
        # print(f"Headers for {object_key} are present is", check_if_headers_present(bucket_name, object_key))

        #     # """Creating dataframes for each table"""
        df = turn_file_into_dataframe(bucket_name, object_key, col_names)
        df_payment = turn_file_into_dataframe(bucket_name, object_key, col_names)
        df_transaction = turn_file_into_dataframe(bucket_name, object_key, col_names)
        clean_dataframe = remove_columns_from_df(
            turn_file_into_dataframe(bucket_name, object_key, col_names),
            to_drop_products,
        )
        df_products = splitting_products_column(clean_dataframe)

        #     # """Cleaning & transforming dataframes"""
        clean_df_store1 = remove_columns_from_df(df, to_drop_stores)
        clean_df_store = clean_df_store1.drop_duplicates(inplace=False)

        clean_df_payment1 = remove_columns_from_df(df_payment, to_drop_payment)
        clean_df_payment = clean_df_payment1.drop_duplicates(inplace=False)

        # Dealing with transaction df
        clean_df_transaction = remove_columns_from_df(
            df_transaction, to_drop_transaction
        )
        # This changes timestampt to the right format, should go in own function, maybe?
        clean_df_transaction["timestamp"] = pd.to_datetime(
            clean_df_transaction.timestamp
        )  # this fills the column with timestamp object which did not work in the query
        clean_df_transaction["timestamp"] = clean_df_transaction["timestamp"].astype(
            str
        )  # this turns the timestamp object into strings which work in the query

        # Dealing with products df
        clean_df_products = df_products
        drop_p = ["products", "total_price", "payment_method"]
        clean_df_products.drop(drop_p, axis=1, inplace=True)

    # insert_values_in_table(connecting_to_db,clean_df_store,'stores')
    # insert_values_in_table(connecting_to_db,clean_df_payment,'payment_methods')
    # insert_values_in_table(connecting_to_db,clean_df_products,'products')
    # insert_values_in_table(connecting_to_db,clean_df_transaction,'transactions')

    # CREATING A CSV FILE FROM A DATAFRAME
    # csv_buffer = StringIO()
    # clean_df_store.to_csv(csv_buffer)
    # s3_resource = boto3.resource('s3')
    # s3_resource.Object(bucket_name, 'melon/clean_df_store.csv').put(Body=csv_buffer.getvalue())
