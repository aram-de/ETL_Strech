import redshift_connector


def connect_to_database(connection_details):
    """
    function to connect the the database, has 1 arguemnt connection which is a dict for the
    parameters to connect to the database.
    If successful it will return conn
    """

    try:
        print("connecting")
        connection = redshift_connector.connect(**connection_details)
        print("Connection done")
    except Exception as e:
        print(e)
        print("Connection failed")
    return connection


connection_details = {
    "database": "NAMEOFYOURDATABASE",
    "user": "NAME OF YOUR USER",
    "password": "YOURPASSWORD",
    "host": "YOURHOST",
    "port": "YOURPORT",
}

connecting_to_db = connect_to_database(connection_details)
