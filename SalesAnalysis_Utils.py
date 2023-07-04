class CategoricalColumns(Enum):
    PRODUCT = "Product"
    QUANTITY_ORDERED = "Quantity Ordered"
    STREET_NAME = "Street Name"
    CITY = "City"
    STATE_CODE = "State Code"
    POSTAL_CODE = "Postal Code"
    HOUSE_NUMBER = "House Number"
    PRODUCT_CATEGORY = "Prouct Category"
    ORDER DAY = "Order Month"
    ORDER MONTH = "Order Day"
    ORDER HOUR = "Order Hour"
    ORDER YEAR = "Order Year"
    
class AddressColumns(Enum):
    STREET_NAME = "Street Name"
    CITY = "City"
    STATE_CODE = "State Code"
    POSTAL_CODE = "Postal Code"
    
class NumericalColumns(Enum):
    TOTAL_COST = "Total Cost"
    QUANTITY_ORDERED = "Quantity Ordered"
    PRICE EACH = 'Price Each'
    
class SalesDataPreprocessing:

    def __init__(self):
    
def split_purchase_address(self, df_to_process: DataFrame) -> DataFrame:
    df_address_split = df_to_process["Purchase Address"].str.split(",", n=3, expand=True)
    df_address_split.columns = ["Street Name", "City", "State and Postal Code"]
    
    df_state_postal_split = (
        df_address_split["State and Postal Code"]
        .str.strip()
        .str.split(" ", n=2, expand=True)
    )
    df_state_postal_split.columns = ["State Code", "Postal Code"]
    
    return pd.concat([df_to_process, df_address_split, df_state_postal_split], axis=1)

def split_order_date(self, df_to_process: DataFrame) -> DataFrame:
    df_to_process['Order Month'] = pd.to_datetime(df_to_process['Order Date']).dt.month
    df_to_process['Order Day'] = pd.to_datetime(df_to_process['Order Date']).dt.day
    df_to_process['Order Hour'] = pd.to_datetime(df_to_process['Order Date']).dt.hour
    df_to_process['Order Year'] = pd.to_datetime(df_to_process['Order Date']).dt.year
    
    return df_to_process

def convert_numerical_column_types(self, df_to_process: DataFrame) -> DataFrame:
    df_to_process["Quantity Ordered"] = df_to_process["Quantity Ordered"].astype(int)
    df_to_process["Price Each"] = df_to_process["Price Each"].astype(float)
    df_to_process["Order ID"] = df_to_process["Order ID"].astype(int)
    
    return df_to_process

def calculate_total_order_cost(self, df_to_process: DataFrame) -> DataFrame:
    df_to_process["Total Cost"] = df_to_process["Quantity Ordered"] * df_to_process["Price Each"]
    return df_to_process

def read_raw_data(self, file_path: str, chunk_size: int=1000) -> DataFrame:
    csv_reader = pd.read_csv(file_path, chunksize=chunk_size)
    processed_chunks = []

    # append the processed chunk to the list
    for chunk in csv_reader:
        chunk = chunk.drop(['Unnamed: 0'], axis=1)
        chunk = chunk.drop_duplicates()
        chunk = chunk.loc[chunk["Order ID"] != "Order ID"].dropna()
        processed_chunks.append(chunk)

    # concatenate the processed chunks into a single DataFrame
    return pd.concat(processed_chunks, axis=0)