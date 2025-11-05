from ETL.extract import extract_data_file, extract_genre_file, extract_item_file
from ETL.transform import transform
from ETL.load import load_to_mysql

def main():
    extract_data_file()
    extract_genre_file()
    extract_item_file()
    transform()
    load_to_mysql()
    
if __name__ == "__main__":
    print("x")
    main()
    print("y")