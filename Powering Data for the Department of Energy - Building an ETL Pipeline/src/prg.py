import pandas as pd
import json

def extract_tabular_data(file_path: str):
    """Extract data from a tabular file_format, with pandas."""
    extention=file_path.split('.')[-1]
    if extention=='csv':
        return pd.read_csv(file_path)
    elif extention=='parquet':
        return pd.read_parquet(file_path)
    else:
        raise ValueError('Warning: Invalid file extension. Please try with .csv or .parquet!')
    ##return pd.read_csv(file_path)

def extract_json_data(file_path):
    """Extract and flatten data from a JSON file.
    data=pd.read_json(file_path)
    df=data['energySource']
    df=pd.json_normalize(df)
    df.rename(columns={'id':'energySource.id',
                       'description':'energySource.description',
                      'capability':'energySource.capability',
                      'capabilityUnits':'energySource.capabilityUnits'},inplace=True)
    raw_electricity_capability_df=pd.concat([data, df], axis=1)
    raw_electricity_capability_df = raw_electricity_capability_df.drop('energySource', axis=1)
    return raw_electricity_capability_df """
    
    with open(file_path, "r") as json_file:
        raw_data = json.load(json_file)
    
    
    return pd.json_normalize(raw_data)


def transform_electricity_sales_data(raw_data: pd.DataFrame):
    """
    Transform electricity sales to find the total amount of electricity sold
    in the residential and transportation sectors.
    
    To transform the electricity sales data, you'll need to do the following:
    - Drop any records with NA values in the `price` column. Do this inplace.
    - Only keep records with a `sectorName` of "residential" or "transportation".
    - Create a `month` column using the first 4 characters of the values in `period`.
    - Create a `year` column using the last 2 characters of the values in `period`.
    - Return the transformed `DataFrame`, keeping only the columns `year`, `month`, `stateid`, `price` and `price-units`.
    """
    ## Drop any records with NA values in the `price` column. Do this inplace.
    raw_data.dropna(subset=['price'],inplace=True)
    ## Only keep records with a `sectorName` of "residential" or "transportation".
    raw_data_clean=raw_data[(raw_data['sectorName']=='residential') | (raw_data['sectorName']=='transportation')]
    ## Create a `month` column using the first 4 characters of the values in `period`.
    ## Create a `year` column using the last 2 characters of the values in `period`.
    def get_year(period):
        year=period.split('-')[0]
        return year
    def get_month(period):
        year=period.split('-')[1]
        return year
    raw_data_clean['year']=raw_data_clean['period'].apply(get_year)
    raw_data_clean['month']=raw_data_clean['period'].apply(get_month)
    ## Return the transformed `DataFrame`, keeping only the columns `year`, `month`, `stateid`, `price` and `price-units`.

    transformed_df=raw_data_clean[['year','month','stateid','price','price-units']]
    return transformed_df

def load(dataframe: pd.DataFrame, file_path: str):
    """Load a DataFrame to a file in either CSV or Parquet format."""
    extention=file_path.split('.')[-1]
    if extention=='csv':
        return dataframe.to_csv(file_path)
    elif extention=='parquet':
        return dataframe.to_parquet(file_path)
    else:
        raise ValueError(f'Warning: {file_path} is not a valid file type. Please try again!_')

# Ready for the moment of truth? It's time to test the functions that you wrote!
raw_electricity_capability_df = extract_json_data("../dataset/electricity_capability_nested.json")
raw_electricity_sales_df = extract_tabular_data("../dataset/electricity_sales.csv")

cleaned_electricity_sales_df = transform_electricity_sales_data(raw_electricity_sales_df)

load(raw_electricity_capability_df, "../dataset/loaded__electricity_capability.parquet")
load(cleaned_electricity_sales_df, "../dataset/loaded__electricity_sales.csv")