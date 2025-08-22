import pandas as pd
import yaml
import sqlalchemy
from sqlalchemy import create_engine, text
import pyodbc
from logger import setup_logger
from urllib.parse import quote_plus
import hashlib
from cryptography.fernet import Fernet

logger = setup_logger()

# Encryption key and cipher setup
encryption_key = b'gR6SMvY_sacACYRTVHU8-nvfP1ZupxazhKVV-mzH68U='
cipher = Fernet(encryption_key)

# Function to decrypt passwords
def decrypt_password(encrypted_password, cipher):
    return cipher.decrypt(encrypted_password.encode()).decode()

def load_config():
    with open("config.yaml", 'r') as f:
        return yaml.safe_load(f)

def get_sybase_connection(sybase_config):
    decrypted_db_password = decrypt_password(sybase_config['password'], cipher)
    driver = "SQL Anywhere 17"  # change this to your actual installed driver
    return pyodbc.connect(f'Driver={driver};Server={sybase_config["ims_service_name"]};Database={sybase_config["database"]};UID={sybase_config["user"]};PWD={decrypted_db_password};')
   
def get_postgres_engine(cfg):
    decrypted_db_password = decrypt_password(cfg['password'], cipher)
    password = quote_plus(decrypted_db_password)  # encodes special chars like @
    url = f"postgresql://{cfg['user']}:{password}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(url)

def execute_postgres_procedures(engine, procedures):
    success = True  # Track overall success/failure
    with engine.begin() as conn:
        for proc in procedures:
            schema = proc.get("schema", "public")
            name = proc["name"]
            args = proc.get("arguments", [])
            arg_placeholders = ", ".join([f":arg{i}" for i in range(len(args))])
            sql = text(f"CALL {schema}.{name}({arg_placeholders})") if args else text(f"CALL {schema}.{name}()")

            logger.info(f"Executing PostgreSQL procedure: {schema}.{name}({', '.join(map(str, args))})")
            try:
                conn.execute(sql, {f"arg{i}": arg for i, arg in enumerate(args)})
            except Exception as e:
                logger.error(f"Failed to execute PostgreSQL procedure {schema}.{name}: {e}")
                success = False
    return success


def create_indexes(engine, table_name, index_columns, index_prefix, schema='public'):
    #index_prefix = cfg.get('index_prefix', 'idx')

    if not index_columns:
        return

    with engine.begin() as conn:
        existing_indexes = conn.execute(text("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = :table_name AND schemaname = :schema
        """), {'table_name': table_name, 'schema': schema}).fetchall()

        existing_index_names = {row[0].lower() for row in existing_indexes}

        for column in index_columns:
            base_name = f"{index_prefix}_{column}_idx"
            if len(base_name) > 63:
                hash_suffix = hashlib.md5(base_name.encode()).hexdigest()[:6]
                index_name = f"{index_prefix}_{column[:20]}_{hash_suffix}"
            else:
                index_name = base_name

            index_name = index_name.lower()

            if index_name not in existing_index_names:
                print(f"Creating index: {index_name} on {column}")
                conn.execute(text(f"""
                    CREATE INDEX "{index_name}"
                    ON "{schema}"."{table_name}" ("{column}");
                """))
            else:
                print(f"Index {index_name} already exists. Skipping.")

def execute_sybase_query(cursor, item):
    if item['type'].lower() == 'view':
        query = f"SELECT * FROM dba.{item['name']};"
        logger.info(f"Executing Sybase view - {item['name']}")
        cursor.execute(query)
    else:  # stored procedure
        args = item.get("arguments", [])
        placeholders = ", ".join(["?" for _ in args])  # assuming pyodbc
        print(placeholders);
        query = f"CALL dba.{item['name']}({placeholders});"
        logger.info(f"Executing Sybase procedure - {item['name']} with args {args}")
        cursor.execute(query, args)


    # Loop through results until we find a result set with a description
    while cursor.description is None:
        if not cursor.nextset():
            logger.error(f"No result set returned for {item['name']}")
            return pd.DataFrame()
        
    columns = [col[0] for col in cursor.description]
    expected_col_count = len(columns)
    data = cursor.fetchall()
    if data:
        print("First row type:", type(data[0]))
        print("First row length:", len(data[0]))

    # Filter out any extra rows like export messages
    filtered_data = [tuple(row) for row in data if len(row) == expected_col_count]
    invalid_rows = [row for row in data if len(row) != expected_col_count]
    if invalid_rows:
        logger.warning(f"{len(invalid_rows)} invalid rows removed from result of {item['name']}")

    if not filtered_data:
        logger.warning(f"No valid rows returned by {item['name']}")
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(filtered_data, columns=columns)


def sync_to_postgres(df, table_name, engine):
    with engine.begin() as conn:
        print('inside the postgres load block')
        # Create schema if not exists
        df.head(0).to_sql(table_name, conn, if_exists='append', index=False)
        # Truncate table
        conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))
        # Load data
        df.to_sql(table_name, conn, if_exists='append', index=False)
        logger.info(f"Loaded {len(df)} rows into PostgreSQL table '{table_name}'")

def main():
    config = load_config()
    # print(pyodbc.drivers());
    sybase_conn = get_sybase_connection(config['sybase'])
    postgres_engine = get_postgres_engine(config['postgres'])

    cursor = sybase_conn.cursor()
    
    for item in config['queries']:
        try:
            df = execute_sybase_query(cursor, item)
            #print(df.iloc[0])
            if not df.empty:
                target_table = item.get('target_table', item['name'])  # fallback to source name
                sync_to_postgres(df, target_table , postgres_engine)
                create_indexes(postgres_engine, target_table, item.get('index_columns', []),item.get('index_prefix', 'idx'))
            else:
                logger.warning(f"No data returned from {item['type']} - {item['name']}")
        except Exception as e:
            logger.error(f"Error processing {item['name']}: {e}")

    cursor.close()
    sybase_conn.close()

# Execute PostgreSQL procedure to load the data from staging to the main table
    proc_success = execute_postgres_procedures(postgres_engine, config.get("postgres_procedures", []))

    if not proc_success:
        logger.error("One or more PostgreSQL procedures failed.")

    logger.info("Data transfer completed.")

# if __name__ == "__main__":
#     main()
