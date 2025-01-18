import os
import sqlite3
import zipfile
import pandas as pd
import yaml
import logging

########################## Load Configurations ##########################
data_incoming_foldername = 'data_incoming'
data_outgoing_foldername = 'data_outgoing'
log_foldername = 'logs'
log_filename = 'cnpj_merger.log'
config_foldername = 'config'
config_filename = 'config.yaml'

path_script = os.path.abspath(__file__)
path_script_dir = os.path.dirname(path_script)
path_project = os.path.dirname(path_script_dir)
path_incoming = os.path.join(path_project, data_incoming_foldername)
path_outgoing = os.path.join(path_project, data_outgoing_foldername)
path_log_dir = os.path.join(path_project, log_foldername)
path_log = os.path.join(path_log_dir, log_filename)
path_config_dir = os.path.join(path_project, config_foldername)
path_config = os.path.join(path_config_dir, config_filename)

# Ensure outgoing and log folders exist
os.makedirs(path_log_dir, exist_ok=True)
os.makedirs(path_outgoing, exist_ok=True)

with open(path_config, 'r') as file:
    config = yaml.safe_load(file)

csv_sep = config['csv_sep']
csv_dec = config['csv_dec']
csv_quote = config['csv_quote']
csv_enc = config['csv_enc']
dtypes = config['dtypes']

########################## Functions ##########################

def create_table_from_dtype(conn, table_name, dtype_dict):
    """Create a table in SQLite based on dtype_dict."""
    columns = [f"{col} {convert_dtype_to_sql(dtype)}" for col, dtype in dtype_dict.items()]
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
    conn.execute(create_table_query)

def convert_dtype_to_sql(dtype):
    """Convert pandas dtype to SQLite type."""
    if dtype == "str":
        return "TEXT"
    elif dtype in ("int", "float"):
        return "REAL"
    else:
        raise ValueError(f"Unsupported dtype: {dtype}")

def process_zip_to_sqlite(zip_file_path, table_name, dtype_dict, conn, chunk_size=1000000):
    """Process ZIP file, read CSV in chunks, and insert into SQLite."""
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_file_list = zip_ref.namelist()
        if len(zip_file_list) != 1:
            logging.warning(f"ZIP file {zip_file_path} contains multiple files. Skipping.")
            return

        with zip_ref.open(zip_file_list[0]) as csvfile:
            reader = pd.read_csv(
                csvfile,
                sep=csv_sep,
                decimal=csv_dec,
                quotechar=csv_quote,
                dtype=dtype_dict,
                encoding='ISO-8859-1',  # ou 'ISO-8859-1'
                header=None,
                chunksize=chunk_size
            )
            for chunk in reader:
                chunk.columns = dtype_dict.keys()  # Assign column names
                chunk.to_sql(table_name, conn, if_exists="append", index=False)

########################## Main ##########################

logging.basicConfig(filename=path_log, level=logging.INFO, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
logging.info('Starting script')
print('Starting script')

# Create SQLite database
sqlite_db_path = os.path.join(path_outgoing, 'data.db')
conn = sqlite3.connect(sqlite_db_path)

# Process each prefix and associated files
for prefix, dtype_dict in dtypes.items():
    table_name = prefix.lower()
    create_table_from_dtype(conn, table_name, dtype_dict)
    for root, _, files in os.walk(path_incoming):
        for filename in files:
            if filename.endswith(".zip") and filename.startswith(prefix.title()):
                zip_file_path = os.path.join(root, filename)
                logging.info(f"Processing {filename}")
                print(f"Processing {filename}")
                process_zip_to_sqlite(zip_file_path, table_name, dtype_dict, conn)

# Close SQLite connection
conn.close()
logging.info('Script finished')
print('Script finished')
