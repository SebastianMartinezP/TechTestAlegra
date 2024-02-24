# pip install pandas
import core

class Customer(core.ETL):
    def __init__(self, input_csv_path, output_csv_path) -> bool:
        super().__init__(input_csv_path, output_csv_path)

    def extract(self) -> None: 
        result = super().extract()
        print('Customer.extract > ', str(result))

    def transform(self) -> None:
        try:
            df = self.data.copy()
            
            df['customer_id'] = df['ID']
            df['segment_name'] = df['Segmento']
            
            # cleaning invalid vocals
            df['name'] = df['Nombre'].map(lambda x : x.translate(core.INVALID_VOCALS))
            df['location_name'] = df['Ubicacion'].map(lambda x : x.translate(core.INVALID_VOCALS))
            
            # simulating a phone number and email input
            df['phone_number'] = df['ID'].map(lambda x : core.get_mock_phone_number())
            df['email'] = df['ID'].map(lambda x : core.get_mock_email())
            
            # selecting only clean fields
            df = df[[
                'customer_id', 'name', 'location_name',
                'segment_name', 'phone_number', 'email'
            ]]
            
            self.data = df
            print('Customer.transform > True')
        except Exception as e:
            print('Customer.transform > ERROR: ', str(e))

    def load(self) -> None: 
        result = super().load()
        print('Customer.load > ', str(result))


class Product(core.ETL):
    def __init__(self, input_csv_path, output_csv_path) -> bool:
        super().__init__(input_csv_path, output_csv_path)

    def extract(self) -> None: 
        result = super().extract()
        print('Product.extract > ', str(result))

    def transform(self) -> None:
        try:
            df = self.data.copy()
            
            df['product_id'] = df['ID']
            df['name'] = df['Nombre']
            df['price'] = df['Precio']

            # cleaning invalid vocals
            df['category'] = df['Categoria'].map(
                lambda x : x.translate(core.INVALID_VOCALS)
            )
            
            # simulating currency type
            df['currency_type'] = df['ID'].map(lambda x : "MXN")
            
            # selecting only clean fields
            df = df[[
                'product_id', 'name',
                'price', 'category',
                'currency_type'
            ]]
            
            self.data = df
            print('Product.transform > True')
        except Exception as e:
            print('Product.transform > ERROR: ', str(e))

    def load(self) -> None: 
        result = super().load()
        print('Product.load > ', str(result))


class Invoice(core.ETL):
    def __init__(self, input_csv_path, output_csv_path) -> bool:
        super().__init__(input_csv_path, output_csv_path)

    def extract(self) -> None: 
        result = super().extract()
        print('Invoice.extract > ', str(result))

    def transform(self) -> None:
        try:
            df = self.data.copy()
        
            df['invoice_id'] = df['ID']
            df['invoice_date'] = df['Fecha']
            df['client_id'] = df['ClienteID']
            df['product_id'] = df['ProductoID']
            df['total_invoice'] = df['Total']

            # applying tranformations
            df['product_quantity'] = df['Cantidad'].fillna(0)

            # simulating currency type
            df['currency_type'] = df['ID'].map(lambda x : "MXN")

            # selecting only clean fields
            df = df[[
                'invoice_id', 'invoice_date',
                'product_quantity',
                'total_invoice',
                'currency_type',
                'client_id', 'product_id'
            ]]
            
            self.data = df
            print('Invoice.transform > True')
        except Exception as e:
            print('Invoice.transform > ERROR: ', str(e))

    def load(self) -> None: 
        result = super().load()
        print('Invoice.load > ', str(result))


if __name__ == "__main__":

    customers:Customer  = Customer(
        input_csv_path  = "docs/input_files/customers.csv",
        output_csv_path = "docs/output_stagging/customers.csv"
    )
    customers.extract()
    customers.transform()
    customers.load()

    products:Product  = Product(
        input_csv_path  = "docs/input_files/products.csv",
        output_csv_path = "docs/output_stagging/products.csv"
    )
    products.extract()
    products.transform()
    products.load()

    invoices:Invoice  = Invoice(
        input_csv_path  = "docs/input_files/invoices.csv",
        output_csv_path = "docs/output_stagging/invoices.csv"
    )
    invoices.extract()
    invoices.transform()
    invoices.load()