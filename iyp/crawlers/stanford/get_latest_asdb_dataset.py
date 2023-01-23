from datetime import datetime
import re
import requests
import bs4
from bs4 import BeautifulSoup

def get_latest_asdb_dataset_url(asdb_stanford_data_url: str, file_name_format: str):
    latest_date: datetime = get_asdb_latest_date(asdb_stanford_data_url)
    lastest_asdbdataset_url = get_asdb_dataset_full_url(asdb_stanford_data_url, latest_date, file_name_format)
    return lastest_asdbdataset_url
    
def get_asdb_dataset_full_url(asdb_stanford_data_url: str, latest_date: str, file_name_format: str) -> str:
    file_name: str = get_file_name_composed_by_date(latest_date, file_name_format)
    asdb_dataset_url: str = get_asdb_dataset_resource_url(asdb_stanford_data_url, file_name)
    return asdb_dataset_url

def get_asdb_dataset_resource_url(asdb_stanford_data_url: str, file_name: str) -> str:
    asdb_stanford_data_url: str = asdb_stanford_data_url.replace('#', '')
    full_url: str = f'{asdb_stanford_data_url}/{file_name}'
    return full_url

def get_file_name_composed_by_date(date: datetime, file_name_format: str) -> str:
    dateset_file_name: str = date.strftime(file_name_format)
    return dateset_file_name
    
def get_asdb_latest_date(asdb_stanford_data_url: str) -> datetime:
    latest_date_text: str = get_asdb_latest_date_text(asdb_stanford_data_url)
    date: datetime = get_date_from_text(latest_date_text)
    return date
    
def get_asdb_latest_date_text(asdb_stanford_data_url: str) -> str:
    response = requests.get(asdb_stanford_data_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    latest_date_element: bs4.element.Tag = soup.find("div",class_="col-md-12").find("p")
    latest_date_text: str = latest_date_element.text
    return latest_date_text

def get_date_from_text(text: str) -> datetime:
    date_regex = re.compile(r'\d{1,2}/\d{1,2}/\d{4}')
    date_string: str = date_regex.search(text).group()
    date_pattern = '%m/%d/%Y'
    date: datetime = datetime.strptime(date_string, date_pattern)
    return date

    
if __name__ == "__main__":
    asdb_dataset_url = get_latest_asdb_dataset_url('https://asdb.stanford.edu/#data', '%Y-%m_categorized_ases.csv')
    print(asdb_dataset_url)