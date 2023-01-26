from extract_clean_load import *

#happy path

#Reads file with no headers but right number of columns, returns a frame with headers
def test_file_has_no_headers_but_columns_right():
    file_path = "data/mockFile.csv"
    assert type(turn_file_into_dataframe(file_path)) == pd.DataFrame


# #Reads file with headers, returns a frame with headers
def test_file_has_correct_headers():
    file_path = "data/mockFileWithHeaders.csv"
    assert type(turn_file_into_dataframe(file_path)) == pd.DataFrame

#Reads correctly formatted TXT
def test_file_is_txt_but_correct_data_with_headers():
    file_path = "data/mockFileWrongType.txt"
    assert type(turn_file_into_dataframe(file_path)) == pd.DataFrame

#Reads correctly formatted file with no extension
def test_file_is_has_no_extension_but_correct_data_with_headers():
    file_path = "data/mockFileWrongType"
    assert type(turn_file_into_dataframe(file_path)) == pd.DataFrame


# #unhappy path

# #There is no file
def test_file_missing():
    file_path = "data/NoFile.csv"
    assert type(turn_file_into_dataframe(file_path)) == FileNotFoundError
  

# #There is a file with wrong number of columns
def test_file_is_missing_SOME_headers():
    file_path = "data/mockFileWithMissingHeaders.csv"
    assert type(turn_file_into_dataframe(file_path)) == ColumnsWrongError

# #There is a file with right number of columns but wrong headings
def test_file_has_wrong_headings_but_right_number_of_columns():
    file_path = "data/mockFileWithWrongHeaders.csv"
    assert type(turn_file_into_dataframe(file_path)) == HeadersWrongError