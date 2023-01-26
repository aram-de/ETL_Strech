import pandas as pd
import redshift_connector

from products import all_product_dict
from src.connecting import connecting_to_db
from src.queries_create_tables import query_payment
from src.queries_create_tables import query_products
from src.queries_create_tables import query_sales
from src.queries_create_tables import query_store
from src.queries_create_tables import query_transactions
from transform import bucket_name
from transform import get_values_already_present
from transform import process_file_for_loading


connection_details = {
    "database": "NAMEOFYOURDATABASE",
    "user": "NAME OF YOUR USER",
    "password": "YOURPASSWORD",
    "host": "YOURHOST",
    "port": "YOURPORT",
}


def handler(event, context):
    object_key = event["Records"][0]["s3"]["object"]["key"]

    def main():
        cursor = connecting_to_db.cursor()

        # CREATING TABLES
        run_query(cursor, query_store)
        run_query(cursor, query_payment)
        run_query(cursor, query_products)
        run_query(cursor, query_transactions)
        run_query(cursor, query_sales)

        products_df = pd.DataFrame(
            all_product_dict.items(), columns=["product_name", "price"]
        )

        dict_of_tables = process_file_for_loading(object_key, cursor)
        # list of tables is a dictionary
        # {"customer_basket_table" : customer_basket_table, "payment_methods_table" : payment_methods_table,
        # "products_table" : products_table, "sales_table" : sales_table, "store_name_table" : store_name_table}

        present_stores = get_values_already_present(cursor, "stores", "store_name")
        frame_to_insert_new_stores = delete_duplicates_if_already_in_db(
            dict_of_tables["store_name_table"], present_stores
        )
        if not frame_to_insert_new_stores.empty:
            insert_values_in_table(cursor, frame_to_insert_new_stores, "stores")
            print("Inserted into stores")

        present_pay_methods = get_values_already_present(
            cursor, "payment_methods", "payment_method"
        )
        frame_to_insert_new_payment_methods = delete_duplicates_if_already_in_db(
            dict_of_tables["payment_methods_table"], present_pay_methods
        )
        if not frame_to_insert_new_payment_methods.empty:
            insert_values_in_table(
                cursor, frame_to_insert_new_payment_methods, "payment_methods"
            )
            print("Inserted into payment methods")

        present_products = get_values_already_present(
            cursor, "products", "product_name"
        )

        if get_values_already_present(cursor, "products", "product_name") == []:
            insert_values_in_table(cursor, products_df, "products")
            print("Inserted into products")

        cursor.execute(f"SELECT COUNT(*) FROM transactions")
        count = cursor.fetchone()[0]
        count_variable = int(count)
        dict_of_tables["sales_table"]["transaction_id"] += count_variable
        # print("Trying to inserted into transactions")
        # load_transactions_to_db(cursor, dict_of_tables["transactions_table"])
        column_as_list = dict_of_tables["sales_table"].product_name.values.tolist()
        column_as_list_of_unique_values = set(column_as_list)
        for value in column_as_list_of_unique_values:
            cursor.execute(
                f"""SELECT product_id FROM products WHERE products.product_name = '{value}'"""
            )
            product_id = cursor.fetchone()[0]
            dict_of_tables["sales_table"]["product_name"] = dict_of_tables[
                "sales_table"
            ]["product_name"].replace([value], product_id)
        dict_of_tables["sales_table"] = dict_of_tables["sales_table"].rename(
            columns={"product_name": "product_id"}
        )

        insert_values_in_table(cursor, dict_of_tables["sales_table"], "sales")

        # print("Inserted into transactions")

        # load_sales_to_db(cursor, dict_of_tables["sales_table"])

        # customer_basket_new_frame = dict_of_tables["customer_basket_table"]
        # customer_basket_new_frame.replace(store_id, store_id+1)
        # customer_basket_new_frame.at[0,'NAME']='Safa'
        # insert_values_in_table(cursor, dict_of_tables["transactions_table"], "transactions")
        # print(dict_of_tables["sales_table"])

        print("Trying to inserted into sales")
        # insert_values_in_table(cursor, dict_of_tables["sales_table"], "sales")
        print("Inserted into sales")

        # THIS IS TO replace store names with store ids in transactions table
        column_as_list = dict_of_tables["transactions_table"].store_name.values.tolist()
        column_as_list_of_unique_values = set(column_as_list)
        for value in column_as_list_of_unique_values:
            cursor.execute(
                f"""SELECT store_id FROM stores WHERE stores.store_name = '{value}'"""
            )
            store_id = cursor.fetchone()[0]
            dict_of_tables["transactions_table"]["store_name"] = dict_of_tables[
                "transactions_table"
            ]["store_name"].replace([value], store_id)
        dict_of_tables["transactions_table"] = dict_of_tables[
            "transactions_table"
        ].rename(columns={"store_name": "store_id"})

        # THIS IS TO replace payment method  with payment ids in transactions table
        column_as_list = dict_of_tables[
            "transactions_table"
        ].payment_method.values.tolist()
        column_as_list_of_unique_values = set(column_as_list)
        for value in column_as_list_of_unique_values:
            cursor.execute(
                f"""SELECT payment_method_id FROM payment_methods WHERE payment_methods.payment_method = '{value}'"""
            )
            payment_method_id = cursor.fetchone()[0]
            dict_of_tables["transactions_table"]["payment_method"] = dict_of_tables[
                "transactions_table"
            ]["payment_method"].replace([value], payment_method_id)
        dict_of_tables["transactions_table"] = dict_of_tables[
            "transactions_table"
        ].rename(columns={"payment_method": "payment_method_id"})

        insert_values_in_table(
            cursor, dict_of_tables["transactions_table"], "transactions"
        )

        print(dict_of_tables["sales_table"])

        cursor.close()
        connecting_to_db.close()

    def load_transactions_to_db(cursor, df):
        # create connection

        tuples = [
            tuple(row) for row in df.to_numpy()
        ]  # create a tuple for each row of data, ready to insert

        try:
            for row in tuples:
                print("155")
                cursor.execute(
                    f"""SELECT store_id FROM stores WHERE stores.store_name = '{row[1]}'"""
                )
                print("157")
                store_id = cursor.fetchone()[0]
                print(f"store id is {store_id}")
                print("159")
                cursor.execute(
                    f"""SELECT payment_method_id FROM payment_methods WHERE payment_methods.payment_method = '{row[3]}'"""
                )
                print("161")
                payment_method_id = cursor.fetchone()[0]
                print(f"payment method id is {payment_method_id}")
                print("163")
                cursor.execute(
                    f"""INSERT INTO transactions(timestamp, store_id, total_price, payment_method_id) VALUES ('{row[0]}',
                {store_id}, {row[2]}, {payment_method_id});"""
                )
                print("168")
            connecting_to_db.commit()
        except (Exception, redshift_connector.DatabaseError) as error:
            print("Error: %s" % error)
            return
        print("The data has been inserted")

    def load_sales_to_db(cursor, df):
        # create connection

        tuples = [
            tuple(row) for row in df.to_numpy()
        ]  # create a tuple for each row of data, ready to insert
        print("162 has run")
        try:
            print("165 has run")
            for row in tuples:
                print("167 has run")

                cursor.execute(
                    f"""SELECT transaction_id FROM transactions WHERE transactions.transaction_id = '{row[0]}'"""
                )
                transaction_id = cursor.fetchone()[0]
                cursor.execute(
                    f"""SELECT product_id FROM products WHERE products.product_name = '{row[1]}'"""
                )
                product_id = cursor.fetchone()[0]
                cursor.execute(
                    f"""INSERT INTO sales(transaction_id,product_id) VALUES ({transaction_id}, {product_id});"""
                )
                print("174 has run")
            connecting_to_db.commit()
            print("176 has run")
        except (Exception, redshift_connector.DatabaseError) as error:
            print("Error: %s" % error)
            return
        print("The data has been inserted")

    def delete_duplicates_if_already_in_db(frame_to_insert, list_of_present_values):
        # clean_frame_to_insert = deepcopy(frame_to_insert)
        for value in list_of_present_values:
            if value in frame_to_insert.values:
                frame_to_insert = frame_to_insert[frame_to_insert.values != value]
                # clean_frame_to_insert = clean_frame_to_insert[clean_frame_to_insert.values != value ]
        return frame_to_insert

    def run_query(cursor, query):
        try:
            cursor.execute(query)
            connecting_to_db.commit()
            print("Successfully run query")
        except (Exception, redshift_connector.DatabaseError) as error:
            print(f"Following query failed {query}")
            print(error)

    def insert_values_in_table(cursor, df, table_name):
        """Loading data into DB"""
        columns = ",".join(list(df.columns))
        asterisk = ", ".join(["%s"] * len(df.columns))

        query = f"INSERT INTO public.{table_name} ({columns}) VALUES ({asterisk});"
        """--> INSERT INTO payment_methods(payment_method) VALUES ('CARD'),..."""

        cursor = connecting_to_db.cursor()

        try:
            for index, row in df.iterrows():
                values = tuple(row)
                cursor.execute(query, values)
            connecting_to_db.commit()
            print(f"A DATAFRAME HAS BEEN INSERTED")
            return

        except (Exception, redshift_connector.DatabaseError) as error:
            print("THERE WAS AN ERROR INSERTING")
            print(f"QUERY TO INSERT IS {query}")
            print(error)

    main()
    # if __name__ == "__main__":
    #    main()
