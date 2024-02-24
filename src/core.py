
import pandas
import random
import string
from typing import Union

# data management related
def get_data(csv_path:str) -> Union[pandas.DataFrame, None]:
    """ Generic function for reading data from csv file. """
    try:
        data = pandas.read_csv(csv_path)
        return data
    except Exception as e:
        print('get_data >>>', str(e))
        return None

def save_data(dataframe:pandas.DataFrame, csv_path:str) -> bool:
    """ Generic function for reading data from csv file. """
    try:
        dataframe.to_csv(
            path_or_buf = csv_path,
            index = False,
            encoding = 'utf-8'
        )
        return True
    except Exception as e:
        print('save_data >>>', str(e))
        return False

def insert_ids(dataframe:pandas.DataFrame, starting_id:int) -> pandas.DataFrame:
    _dataframe = dataframe.copy()
    _dataframe.insert(0, 'id', range(starting_id, starting_id + len(_dataframe)))
    return _dataframe


# utils
def get_week_day_name(day:int) -> Union[str, None]:
    DAYS_OF_WEEK = {
        0 : "Monday",
        1 : "Tuesday",
        2 : "Wednesday",
        3 : "Thursday",
        4 : "Friday",
        5 : "Saturday",
        6 : "Sunday"
    }
    return DAYS_OF_WEEK.get(day, None)

def get_month_name(month:int) -> Union[str, None]:
    MONTHS = {
        1  : "January",     7  : "July",
        2  : "February",    8  : "August",
        3  : "March",       9  : "September",
        4  : "April",       10 : "October",
        5  : "May",         11 : "November",
        6  : "June",        12 : "December"
    }
    return MONTHS.get(month, None)

def get_semester(month:int) -> int:
    return 1 if month < 7 else 2

def get_mock_phone_number() -> str:
    """ Returns a mock phone number similar to the mexican format. """
    return ''.join([
        '+52',
        ''.join(random.sample(string.digits, 10))
    ])

def get_mock_email() -> str:
    """ Returns a mock email adress. """
    return ''.join([
        ''.join(random.sample(string.ascii_letters, 10)),
        '@',
        ''.join(random.sample(string.ascii_lowercase, 6)),
        '.com'
    ])

# translation tables
INVALID_VOCALS:dict[int,int] = str.maketrans(
    'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜà',
    'aeiouAEIOUaeiouAEIOUa'
)


# generic ETL main class
class ETL:
    """ Generic definitios for an ETL procedure with Pandas. """

    input_csv_path: Union[str,None] = None
    output_csv_path: Union[str,None] = None
    data: Union[pandas.DataFrame, None] = None

    def __init__(self, input_csv_path, output_csv_path) -> bool:
        self.input_csv_path = input_csv_path
        self.output_csv_path = output_csv_path
        return (
            True 
            if isinstance(self.input_csv_path, str)
            and isinstance(self.output_csv_path, str)
            else False
        )
    
    def extract(self) -> bool:
        self.data = get_data(csv_path = self.input_csv_path)
        return True if isinstance(self.data, pandas.DataFrame) else False
    
    # defined inside implementation eventually.
    def transform(self) -> bool: ...

    def load(self) -> bool:
        result = save_data(
            dataframe = self.data,
            csv_path = self.output_csv_path
        )
        return result


