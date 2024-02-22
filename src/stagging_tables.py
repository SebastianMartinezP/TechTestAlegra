# pip install pandas

import pandas
import random
import string
from typing import Union, List, Any



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


# translation tables
INVALID_VOCALS:dict[int,int] = str.maketrans('áéíóúÁÉÍÓÚäëïöüÄËÏÖÜà', 'aeiouAEIOUaeiouAEIOUa')



class Customer(ETL):
    def __init__(self, input_csv_path, output_csv_path) -> bool:
        super().__init__(input_csv_path, output_csv_path)

    def extract(self) -> None: 
        result = super().extract()
        print('Customer.extract > ', str(result))

    def transform(self):
        
        df = self.data.copy()
        
        df['customer_id'] = df['ID']
        df['segment_name'] = df['Segmento']
        
        # cleaning invalid vocals
        df['name'] = df['Nombre'].map(lambda x : x.translate(INVALID_VOCALS))
        df['location_name'] = df['Ubicacion'].map(lambda x : x.translate(INVALID_VOCALS))
        
        # simulating a phone number and email input
        df['phone_number'] = df['ID'].map(lambda x : get_mock_phone_number())
        df['email'] = df['ID'].map(lambda x : get_mock_email())
        
        # selecting only clean fields
        df = df[[
            'customer_id', 'name', 'location_name',
            'segment_name', 'phone_number', 'email'
        ]]
        
        self.data = df

    def load(self) -> None: 
        result = super().load()
        print('Customer.load > ', str(result))

    
if __name__ == "__main__":

    # region customers

    customers:Customer  = Customer(
        input_csv_path  = "docs/input_files/customers.csv",
        output_csv_path = "docs/output_stagging/customers.csv"
    )

    customers.extract()
    customers.transform()
    customers.load()

    # endregion customers