from extract import *

#happy path

#Reads file with no headers, returns a frame with headers
def test_file_has_no_headers():
    file_path = "data/mockFile.csv"
    assert type(turn_file_into_dataframe(file_path)) == pd.DataFrame
        
# #Reads file with headers, returns a frame with headers
def test_file_has_headers():
    file_path = "data/mockFileWithHeaders.csv"
    assert type(turn_file_into_dataframe(file_path)) == pd.DataFrame

#Reads correctly formatted TXT
def test_file_is_txt_but_correct_data():
    file_path = "data/mockFileWrongType.txt"
    assert type(turn_file_into_dataframe(file_path)) == pd.DataFrame

#Reads correctly formatted file with no extension
def test_file_is_has_no_extension_but_correct_data():
    file_path = "data/mockFileWrongType"
    assert type(turn_file_into_dataframe(file_path)) == pd.DataFrame


# #unhappy path

# #There is no file
def test_file_missing():
    file_path = "data/NoFile.csv"
    assert type(turn_file_into_dataframe(file_path)) == FileNotFoundError
  

# #There is no file
def test_file_is_missing_SOME_headers():
    file_path = "data/NoFile.csv"
    assert type(turn_file_into_dataframe(file_path)) == Error

   
  