import os
import logging
import pandas as pd
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector as sf
from sqlalchemy import create_engine
from constants import DIM_CODES,DIM_COUNTRY,DIM_FLOW,DIM_QUANTITY_NAME,DIM_YEAR,FACT_TRADES, BATCH_SIZE


logging.basicConfig(format="%(levelname)s - %(asctime)s - Message: %(message)s", datefmt="%Y-%m-%d", level=logging.DEBUG)
load_dotenv()




def connect_oltp_db() -> object | None:
    """
    Connects to the OLTP database.
    
    Returns:
        object: Engine object for the database connection.
    """
    try:
        engine = create_engine(f"postgresql+psycopg2://{os.getenv('user')}:{os.getenv('password')}@myDB:{os.getenv('port')}/{os.getenv('dbname')}")
        return engine
    except Exception as err:
        logging.info(f"DB connection error: {err}")
        return None




def extract() -> pd.DataFrame:
    """
    Extracts data from various sources.
    
    Returns:
        pd.DataFrame: Extracted dataframes.
    """
    
    src_dir = Path.cwd() / "etl" / "src"
    file_paths = [path for path in src_dir.glob("*.*")]

    # Extract data from OLTP database
    df_trades = pd.read_sql_query("SELECT * FROM trades LIMIT 1000000", connect_oltp_db())
    
    # Extract data from CSV file
    df_codes = pd.read_csv(file_paths[0])
    
    # Extract data from JSON file
    df_country = pd.read_json(file_paths[1])

    # Validate the data
    logging.info(f"Shape of trades: {df_trades.shape}")
    logging.info(f"Shape of codes: {df_codes.shape}")
    logging.info(f"Shape of countries: {df_country.shape}")

    return df_codes, df_country, df_trades




def clean_codes(row: int, df_parent: pd.DataFrame) -> tuple:
    """
    Cleans codes and retrieves parent information.

    Args:
        row (int): Code to be cleaned.
        df_parent (pd.DataFrame): Parent DataFrame.

    Returns:
        tuple: Cleaned code and parent information.
    """

    code_str = str(row)
    parent_code = None
    
    if len(code_str) == 11:
        code_str = code_str[:5]
        parent_code = code_str[:1]
    else:
        code_str = code_str[:6]
        parent_code = code_str[:2]

    try:
        parent = df_parent.loc[df_parent["Code_comm"] == parent_code, "Description"].values[0]
    except Exception as err:
        logging.error(err)
        parent = None
    
    return int(code_str), parent




def transform_codes(df_codes: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms code DataFrame.

    Args:
        df_codes (pd.DataFrame): DataFrame containing codes.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    
    # Create a new DataFrame with filtered data
    df_parent = df_codes[df_codes["Level"] == 2].copy()

    # Filter NaN values from the 'Code_comm' column
    df_codes = df_codes.loc[df_codes["Code_comm"].notnull(),:]
    
    # Add columns
    df_codes[["clean_code", "category"]] = df_codes.apply(lambda row: clean_codes(row["Code"], df_parent), axis=1, result_type="expand")

    # Filter non-null data and columns
    df_codes = df_codes.loc[df_codes["clean_code"].notnull(),["clean_code","Description","category"]]

    # Create a unique identifier
    df_codes["id_code"] = df_codes.index
    
    df_codes = df_codes[["id_code","clean_code","Description","category"]]

    return df_codes




def transform_country(df_country: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms country DataFrame.

    Args:
        df_country (pd.DataFrame): DataFrame containing country data.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    
    # Add a unique ID
    df_country["id_country"] = df_country.index + 1

    # Filter necessary columns
    df_country = df_country[["id_country","alpha-3","country","region","sub-region"]]

    # Filter null values
    df_country = df_country.loc[df_country["alpha-3"].notnull(),:]

    return df_country




def create_dimension(data: pd.Series, id_name: str) -> pd.DataFrame:
    """
    Creates a dimension DataFrame.

    Args:
        data (pd.Series): Data for the dimension.
        id_name (str): Name for the ID column.

    Returns:
        pd.DataFrame: Dimension DataFrame.
    """
    list_ids = list(range(1, len(data) + 1))

    return pd.DataFrame({id_name: list_ids, "value": data})




def transform(df_codes: pd.DataFrame, df_country: pd.DataFrame, df_trades: pd.DataFrame) -> tuple:
    """
    Transforms dataframes.

    Args:
        df_codes (pd.DataFrame): DataFrame containing codes.
        df_country (pd.DataFrame): DataFrame containing country data.
        df_trades (pd.DataFrame): DataFrame containing trade data.

    Returns:
        tuple: Transformed dataframes.
    """
    
    dim_codes = transform_codes(df_codes)
    dim_country = transform_country(df_country)
    
    # Merge tables
    df_trades_clean = df_trades.merge(dim_country[["alpha-3", "id_country"]],how="left",left_on="country_code", right_on="alpha-3")
    dim_country = dim_country.rename(columns={"alpha-3": "alpha_3", "sub-region": "sub_region"})
    df_trades_clean = df_trades_clean.merge(dim_codes[["clean_code","id_code"]], how="left", left_on="comm_code",right_on="clean_code")

    # Create dimensions
    dim_year = create_dimension(df_trades_clean["year"].unique(), "id_year")
    dim_flow = create_dimension(df_trades_clean["flow"].unique(), "id_flow")
    dim_quantity_name = create_dimension(df_trades_clean["quantity_name"].unique(), "id_quantity_name")

    # Merge dimensions to the trades table
    df_trades_clean = df_trades_clean.merge(dim_year, how="left", left_on="year",right_on="value")
    df_trades_clean = df_trades_clean.merge(dim_flow, how="left", left_on="flow",right_on="value")
    df_trades_clean = df_trades_clean.merge(dim_quantity_name, how="left", left_on="quantity_name",right_on="value")

    # Create the fact table
    df_trades_clean["id_fact_trades"] = df_trades_clean.index + 1
    df_trades_clean.dropna(inplace=True)
    df_fac = df_trades_clean[["id_fact_trades","id_country","id_code","id_year", "id_flow","id_quantity_name","trade_usd","kg","quantity"]].copy()

    # Validate data duplicates
    logging.info(f"Shape of dim_codes: {dim_codes.shape}")
    logging.info(f"Shape of dim_country: {dim_country.shape}")
    logging.info(f"Shape of dim_year: {dim_year.shape}")
    logging.info(f"Shape of dim_flow: {dim_flow.shape}")
    logging.info(f"Shape of dim_quantity_name: {dim_quantity_name.shape}")
    logging.info(f"Shape of fac_trades: {df_fac.shape}")

    return df_fac, dim_codes, dim_country, dim_flow, dim_quantity_name, dim_year




def snowflake_connection():
    """
    Establishes connection to Snowflake.

    Returns:
        tuple: Cursor and connection objects.
    """
    conn = sf.connect(
        account=os.getenv("snowflake_account"),
        user=os.getenv("snowflake_user"),
        password=os.getenv('snowflake_password'),
        warehouse=os.getenv("snowflake_warehouse"),
        database=os.getenv("snowflake_database"),
        role=os.getenv("snowflake_role")
    )

    # Create a cursor to execute SQL commands
    cur = conn.cursor()

    return cur, conn




def insert_data_into_snowflake(data_frame : pd.DataFrame, table_name : str):
    """
    Insert data into Snowflake table in batches to handle large datasets.

    Args:
        data_frame (pd.DataFrame): DataFrame containing data to be inserted.
        table_name (str): Name of the table to insert into.
    """
    try:
        logging.info("Inserting data into Snowflake...")
        cursor, connection = snowflake_connection()

        # Get the column names of the DataFrame
        column_names = list(data_frame.columns)

        # Prepare data for bulk insertion
        data_values = [tuple(row) for row in data_frame.values]

        # Generate the column string for insertion
        columns_string = ', '.join(column_names)

        # Generate the placeholder string for insertion
        placeholders = ', '.join(['%s'] * len(column_names))

        # Execute bulk insertion into the final table in Snowflake
        insert_query = f"INSERT INTO {table_name} ({columns_string}) VALUES ({placeholders})"
        cursor.executemany(insert_query, data_values)

        # Commit the transaction
        connection.commit()
        logging.info("Data insertion into Snowflake completed successfully.")
    except Exception as err:
        logging.error(f"Error inserting data into Snowflake.{err}")
        connection.rollback()
        handle_insert_error(data_frame, table_name)
        connection.commit()
    finally:
        # Close the cursor and the connection
        cursor.close()
        connection.close()




def handle_insert_error(data_frame : pd.DataFrame, table_name : str):
    """
    Handles insertion error by dividing the data into smaller batches and retrying.

    Args:
        data_frame (pd.DataFrame): DataFrame containing data to be inserted.
        table_name (str): Name of the table to insert into.
    """
    
    data_batches = []
    counter = 0
    columns = list(data_frame.columns)

    for row in data_frame.values:
        data_batches.append(list(row))
        counter += 1
        if counter == BATCH_SIZE:
            insert_data_into_snowflake(pd.DataFrame(data_batches, columns=columns), table_name)
            counter = 0
            data_batches = []

    # Insert remaining data if any
    if data_batches:
        insert_data_into_snowflake(pd.DataFrame(data_batches, columns=columns), table_name)

        


def load(dim_codes: pd.DataFrame, dim_country: pd.DataFrame, dim_flow: pd.DataFrame, dim_quantity_name: pd.DataFrame, dim_year: pd.DataFrame, df_fac: pd.DataFrame):
    """
    Loads data into Snowflake.

    Args:
        dim_codes (pd.DataFrame): DataFrame for codes dimension.
        dim_country (pd.DataFrame): DataFrame for country dimension.
        dim_flow (pd.DataFrame): DataFrame for flow dimension.
        dim_quantity_name (pd.DataFrame): DataFrame for quantity name dimension.
        dim_year (pd.DataFrame): DataFrame for year dimension.
        df_fac (pd.DataFrame): DataFrame for fact table.
    """
    insert_data_into_snowflake(dim_codes, DIM_CODES)
    insert_data_into_snowflake(dim_country, DIM_COUNTRY)
    insert_data_into_snowflake(dim_flow, DIM_FLOW)
    insert_data_into_snowflake(dim_quantity_name, DIM_QUANTITY_NAME)
    insert_data_into_snowflake(dim_year, DIM_YEAR)
    insert_data_into_snowflake(df_fac, FACT_TRADES)
