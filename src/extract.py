def turn_file_into_dataframe(bucket_name, object_key, col_names) -> pd.DataFrame:

    """Take a CSV file with the right number of columns and returns a data frame"""

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        df_s3_data = pd.read_csv(response["Body"], sep=",", names=col_names)
        return df_s3_data
    except FileNotFoundError as e:
        print(f"There was no file with the object key: {object_key}")
        return e
