import boto3
import pandas as pd

from src.connecting import connect_to_database

bucket_name = "REPLACEWITHYOURBUCKETNAME"
s3_client = boto3.client("s3", endpoint_url="https://s3.eu-west-1.amazonaws.com")

connection_details = {
    "database": "NAMEOFYOURDATABASE",
    "user": "NAME OF YOUR USER",
    "password": "YOURPASSWORD",
    "host": "YOURHOST",
    "port": "YOURPORT",
}


def remove_columns_from_df(
    dataframe: pd.DataFrame, columns_to_drop: list
) -> pd.DataFrame:

    """Removes specified columns from a dataframe"""

    try:
        dataframe.drop(columns_to_drop, axis=1, inplace=True)
        dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"])
        return dataframe
    except Exception as e:
        print(e)
        print("dataframe does not contain specified columns")


def splitting_products_column(df):

    """Cleansing products column"""

    col = "products"
    # Turn products into a list
    df[col] = df[col].str.split(", ")

    # Spliting the products into individual row
    df = df.explode(col)

    # Splitting from the last ' - '
    df[col] = df[col].str.rsplit(" - ", n=1)

    # Two new columns
    df[["product_name", "price"]] = pd.DataFrame(df.products.tolist(), index=df.index)

    # drop old columns
    df = df.drop(columns=[col])

    # Rearrange columns
    new_col = [
        "timestamp",
        "store_name",
        "product_name",
        "price",
        "total_price",
        "payment_method",
    ]
    df = df[new_col]
    return df


def get_values_already_present(cursor, table_name, value):
    query_to_get_already_inserted_values = f"SELECT {value} FROM  public.{table_name};"
    cursor.execute(query_to_get_already_inserted_values)
    result = cursor.fetchall()
    list_of_inserted_values = []
    for row in result:
        list_of_inserted_values.append(row[0])
    return list_of_inserted_values


col_names = [
    "timestamp",
    "store_name",
    "customer_name",
    "products",
    "total_price",
    "payment_method",
    "card_number",
]

columns_to_drop = ["customer_name", "card_number"]


# collect list of files from bucket
# list_of_file_names = collect_names_of_files_in_bucket(bucket_name)

# Check if the files in the list files are already logged


def process_file_for_loading(object_key, cursor):

    # Turning csv into df
    dataframe_file = turn_file_into_dataframe(bucket_name, object_key, col_names)

    # Dropping privacy columns
    clean_df = remove_columns_from_df(dataframe_file, columns_to_drop)

    # Turning the timestamp into a string which is needed by redshift
    clean_df["timestamp"] = clean_df["timestamp"].astype(str)

    # Splitting products into individual columns
    clean_split_df = splitting_products_column(clean_df)

    # 1. Creating a payment method df
    payment_methods_table = pd.DataFrame(
        clean_split_df["payment_method"].unique(), columns=["payment_method"]
    )

    # # 2. Creating a store name df
    store_name_table = pd.DataFrame(
        clean_split_df["store_name"].unique(), columns=["store_name"]
    )

    # 3. Creating a product df
    products_table = pd.DataFrame(
        clean_split_df[["product_name", "price"]].drop_duplicates(),
        columns=["product_name", "price"],
    )

    # 4. Creating a transactions_table

    transactions_table = pd.DataFrame(
        clean_df[["timestamp", "store_name", "total_price", "payment_method"]],
        columns=["timestamp", "store_name", "total_price", "payment_method"],
    )

    # 5. Creating a sales df
    sales_table = pd.DataFrame(
        clean_split_df, columns=["transaction_id", "product_name"]
    )
    sales_table["transaction_id"] = sales_table.index + 1

    return {
        "transactions_table": transactions_table,
        "payment_methods_table": payment_methods_table,
        "products_table": products_table,
        "sales_table": sales_table,
        "store_name_table": store_name_table,
    }
