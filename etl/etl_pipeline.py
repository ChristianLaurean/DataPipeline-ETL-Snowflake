import logging
from pipeline_utils import extract, transform, load

logging.basicConfig(format="%(levelname)s - %(asctime)s - Message: %(message)s", datefmt="%Y-%m-%d", level=logging.DEBUG)

try:
    logging.info("Starting Pipeline")
    
    # Extract data
    df_codes, df_country, df_trades = extract()
    
    # Transform data
    df_fac, dim_codes, dim_country, dim_flow, dim_quantity_name, dim_year = transform(df_codes, df_country, df_trades)
    
    # Load data
    load(dim_codes, dim_country, dim_flow, dim_quantity_name, dim_year, df_fac)
    
    
    logging.info("Successfully extracted, transformed, and loaded data")
except Exception as err:
    logging.info(f"Pipeline failed with error: {err}")
