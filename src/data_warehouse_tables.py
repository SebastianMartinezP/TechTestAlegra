import pandas
import random
from datetime import datetime
from typing import Union, List
import core

class Customer(core.ETL):
    starting_id: Union[int, None] = None

    def __init__(self, input_csv_path, output_csv_path, starting_id:int) -> bool:
        super().__init__(input_csv_path, output_csv_path)
        self.starting_id = starting_id

    def extract(self) -> None: 
        result = super().extract()
        print('Customer.extract > ', str(result))

    def transform(self) -> None:
        try:
            df = self.data.copy()
            
            
            today = str(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"))

            df['ingestion_date'] = today
            df['last_modified_date'] = today

            # inserting ids for the dimension on the datawarehouse.
            df = core.insert_ids(
                dataframe = df,
                starting_id = self.starting_id
            )

            self.data = df
            print('Customer.transform > True')
        except Exception as e:
            print('Customer.transform > ERROR: ', str(e))

    def load(self) -> None: 
        result = super().load()
        print('Customer.load > ', str(result))

class Product(core.ETL):
    starting_id: Union[int, None] = None

    def __init__(self, input_csv_path, output_csv_path, starting_id:int) -> bool:
        super().__init__(input_csv_path, output_csv_path)
        self.starting_id = starting_id

    def extract(self) -> None: 
        result = super().extract()
        print('Product.extract > ', str(result))

    def transform(self) -> None:
        try:
            df = self.data.copy()
            
            today = str(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"))

            df['ingestion_date'] = today
            df['last_modified_date'] = today

            # inserting ids for the dimension on the datawarehouse.
            df = core.insert_ids(
                dataframe = df,
                starting_id = self.starting_id
            )
            
            self.data = df
            print('Product.transform > True')
        except Exception as e:
            print('Product.transform > ERROR: ', str(e))

    def load(self) -> None: 
        result = super().load()
        print('Product.load > ', str(result))

class Time(core.ETL):
    starting_id: Union[int, None] = None
    def __init__(self, input_csv_path, output_csv_path, starting_id:int) -> bool:
        super().__init__(input_csv_path, output_csv_path)
        self.starting_id = starting_id

    def extract(self) -> None: 
        result = super().extract()
        print('Time.extract > ', str(result))

    def transform(self) -> None:
        try:
            df = self.data.copy()

            # 1 - obtaining first and last dates from the invoices.

            df = df[['invoice_date']]
            # getting only date part of the whole string.
            df['invoice_date'] = df['invoice_date'].map(lambda x : x[:10])
            # making it a sorted list with unique values.
            dates:List = df['invoice_date'].unique().tolist()
            dates.sort()
            # get first and last items.
            dates = [dates[0], dates[-1]]

            # 2 - Calculating whole time dimension,

            start_date = pandas.Timestamp(dates[0])
            end_date = pandas.Timestamp(dates[1])

            time_range = pandas.date_range(
                start = start_date,
                end   = end_date,
                freq  = "1min" # frequence down to 1 min due to file size limitation.
            )

            time_df = pandas.DataFrame(data = {'date' : time_range})

            time_df['year'] = time_df['date'].dt.year
            time_df['quarter'] = time_df['date'].dt.quarter
            time_df['semester'] = time_df['date'].dt.month.map(core.get_semester)
            time_df['month'] = time_df['date'].dt.month
            time_df['month_string'] = time_df['date'].dt.month.map(core.get_month_name)
            time_df['day'] = time_df['date'].dt.day
            time_df['day_of_week_string'] = time_df['date'].dt.day_of_week.map(core.get_week_day_name)
            time_df['hour_24'] = time_df['date'].dt.hour
            time_df['hour_12'] = time_df['date'].dt.hour.map(lambda x : abs(x - 12))
            time_df['minutes'] = time_df['date'].dt.minute
            time_df['seconds'] = time_df['date'].dt.second
            time_df['max_date_ingested'] = dates[1]
            time_df['min_date_ingested'] = dates[0]

            today = str(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"))

            time_df['ingestion_date'] = today
            time_df['last_modified_date'] = today

            # inserting ids for the dimension on the datawarehouse.
            time_df = core.insert_ids(
                dataframe = time_df,
                starting_id = self.starting_id
            )

            self.data = time_df
            print('Time.transform > True')
        except Exception as e:
            print('Time.transform > ERROR: ', str(e))

    def load(self) -> None: 
        result = super().load()
        print('Time.load > ', str(result))

class PaymentMethod(core.ETL):
    starting_id: Union[int, None] = None

    def __init__(self, input_csv_path, output_csv_path, starting_id:int) -> bool:
        super().__init__(input_csv_path, output_csv_path)
        self.starting_id = starting_id

    def extract(self) -> None: 
        # OVERRIDING EXISTING ETL METHOD, there's no provided data.
        #result = super().extract()

        self.data  = pandas.DataFrame([
            {
                'payment_method_id': 1111,
                'method': 'Visa',
                'description':'a way to pay things... i dont know.'
            },
            {
                'payment_method_id': 2222,
                'method': 'Cash',
                'description':'another way to pay things...'
            },
            {
                'payment_method_id': 3333,
                'method': 'Bank Transfer',
                'description':'the 3rd way to pay.'
            },
            {
                'payment_method_id': 4444,
                'method': 'Check',
                'description':'the last one, finally.'
            }
        ])
        
        print('PaymentMethod.extract > True')

    def transform(self) -> None:
        try:
            df = self.data.copy()

            today = str(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"))

            df['ingestion_date'] = today
            df['last_modified_date'] = today

            # inserting ids for the dimension on the datawarehouse.
            df = core.insert_ids(
                dataframe = df,
                starting_id = self.starting_id
            )

            
            self.data = df
            print('PaymentMethod.transform > True')
        except Exception as e:
            print('PaymentMethod.transform > ERROR: ', str(e))

    def load(self) -> None: 
        result = super().load()
        print('PaymentMethod.load > ', str(result))

class Invoice(core.ETL):
    starting_id: Union[int, None] = None

    def __init__(self, input_csv_path, output_csv_path, starting_id:int) -> bool:
        super().__init__(input_csv_path, output_csv_path)
        self.starting_id = starting_id

    def extract(self) -> None: 
        result = super().extract()
        print('Invoice.extract > ', str(result))

    def transform(self) -> None:
        try:
            df = self.data.copy()
        
            df['invoice_date'] = df['invoice_date'].map(
                lambda x : datetime.strftime(
                    datetime.strptime(x, "%Y-%m-%d"),
                    "%Y-%m-%d %H:%M:%S"
                )
            )
            # since we dont have payment_methods_id, we create them randomly
            df['payment_method_id'] = df['invoice_id'].map(
                lambda x : random.choice(
                    [1111, 2222, 3333, 4444]
                )
            )

            # Calculating the ammount of the whole invoice (detail could be spread across more than one row.)

            df = df.rename(columns = {'total_invoice' : 'total_per_product'})
            
            df_total_invoice = df.groupby('invoice_id')['total_per_product'].sum().to_frame()

            df_total_invoice = df_total_invoice.rename(
                columns = {'total_per_product' : 'total_invoice'}
            )

            df = df.merge(df_total_invoice, on='invoice_id', how='left')
            


            # get id's for inserting new data.

            # time ids
            df_time = core.get_data(
                csv_path = "docs/output_data_warehouse/time_dim.csv"
            )

            # since client data only haves date data, we trunc the dataset only to those
            # columns wich doesnt have temporal data. (faster)
            
            df_time = df_time.loc[
                (df_time['hour_24'] == 0) &
                (df_time['minutes'] == 0) &
                (df_time['seconds'] == 0)
            ]
            df_time = df_time[['id', 'date']]
            df_time = df_time.rename(columns = {
                'id' : 'time_dim_id', # new field in fact_invoice
                'date' : 'invoice_date' # field existing inside fact_invoice.
            })

            
            # customer ids
            df_customer = core.get_data(
                csv_path = "docs/output_data_warehouse/customers_dim.csv"
            )
            df_customer = df_customer[['id', 'customer_id']]
            df_customer = df_customer.rename(
                columns = {
                    'id' : 'customer_dim_id',
                    'customer_id': 'client_id'
                }
            )


            # product ids
            df_product = core.get_data(
                csv_path = "docs/output_data_warehouse/products_dim.csv"
            )
            df_product = df_product[['id', 'product_id']]
            df_product = df_product.rename(
                columns = {'id' : 'product_dim_id'}
            )


            # payment_method ids
            df_payment_method = core.get_data(
                csv_path = "docs/output_data_warehouse/payment_method_dim.csv"
            )
            df_payment_method = df_payment_method[['id', 'payment_method_id']]
            df_payment_method = df_payment_method.rename(
                columns = {'id' : 'payment_method_dim_id'}
            )



            # assigning the new id's to every row.
            df = df.merge(df_time,     on = 'invoice_date', how = 'left')
            df = df.merge(df_customer, on = 'client_id',    how = 'left')
            df = df.merge(df_product,  on = 'product_id',   how = 'left')    
            df = df.merge(df_payment_method,    on = 'payment_method_id', how = 'left')




                        
            today = str(datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"))

            df['ingestion_date'] = today
            df['last_modified_date'] = today

            # inserting ids for the dimension on the datawarehouse.
            df = core.insert_ids(
                dataframe = df,
                starting_id = self.starting_id
            )

            df = df[[
                'id',
                'invoice_id',
                'time_dim_id',
                'customer_dim_id',
                'product_dim_id',
                'payment_method_dim_id',
                'product_quantity',
                'total_per_product',
                'total_invoice',
                'currency_type',
                'ingestion_date',
                'last_modified_date'
            ]]


            df = df.rename(
                columns = {
                    'time_dim_id'       : 'time_id',
                    'customer_dim_id'   : 'customer_id',
                    'product_dim_id'    : 'product_id',
                    'payment_method_dim_id' : 'payment_method_id',
                }
            )

            
            self.data = df
            print('Invoice.transform > True')
        except Exception as e:
            print('Invoice.transform > ERROR: ', str(e))

    def load(self) -> None: 
        result = super().load()
        print('Invoice.load > ', str(result))


if __name__ == "__main__":

    customers:Customer  = Customer(
        input_csv_path  = "docs/output_stagging/customers.csv",
        output_csv_path = "docs/output_data_warehouse/customers_dim.csv",
        starting_id     = 0 # last id from the database.
    )
    customers.extract()
    customers.transform()
    customers.load()
    
    products:Product  = Product(
        input_csv_path  = "docs/output_stagging/products.csv",
        output_csv_path = "docs/output_data_warehouse/products_dim.csv",
        starting_id     = 0 # last id from the database.
    )
    products.extract()
    products.transform()
    products.load()

    # getting the time dimension data from the invoices date
    time:Time = Time(
        input_csv_path  = "docs/output_stagging/invoices.csv",
        output_csv_path = "docs/output_data_warehouse/time_dim.csv",
        starting_id     = 0 # last id from the database.
    )
    time.extract()
    time.transform()
    time.load()

    payment_method:PaymentMethod  = PaymentMethod(
        input_csv_path  = "... theres no data for this dimension, completely invented :D",
        output_csv_path = "docs/output_data_warehouse/payment_method_dim.csv",
        starting_id     = 0 # last id from the database.
    )
    payment_method.extract()
    payment_method.transform()
    payment_method.load()

    invoices:Invoice  = Invoice(
        input_csv_path  = "docs/output_stagging/invoices.csv",
        output_csv_path = "docs/output_data_warehouse/fact_invoices.csv",
        starting_id     = 0 # last id from the database.
    )
    invoices.extract()
    invoices.transform()
    invoices.load()