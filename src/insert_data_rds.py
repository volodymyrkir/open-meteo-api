import json
import logging

import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


def get_secret(client, secret_name: str) -> dict:
    """
    Gets secret from AWS Secrets Manager.

    Args:
        client: Boto3 client object.
        secret_name (str): Secret name.

    Returns:
        dict: Dictionary of key-value secret pairs.
    """
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])

    except client.exceptions.ResourceNotFoundException as resource_exc:
        logging.error(f"No credentials found for {secret_name}")
        raise resource_exc
    except Exception as ex:
        logging.error("An error occurred")
        raise ex


def create_connection_string() -> str:
    """
    Creates connection string based on the secrets from AWS Secrets Manager.

    Returns:
        str: Connection string.
    """
    client = boto3.client('secretsmanager')
    sensitive_secrets = get_secret(client, 'rds!db-517abda2-6501-4879-b919-c79908a76560')
    db_configuration = get_secret(client, 'dbinstance')

    rds_user, password = sensitive_secrets['username'], sensitive_secrets['password']
    rds_host, database_name = db_configuration['host'], db_configuration['dbname']
    return f'mysql+pymysql://{rds_user}:{password}@{rds_host}:3306/{database_name}'


def insert_df_to_db(
        df: pd.DataFrame,
        table_name: str,
        conn_str: str,
        mappings: dict[str, list[str]]
) -> None:
    """
    Inserts dataframe to database.
        Creates connection and handles integrity errors.

    Args:
        df (pd.DataFrame): Dataframe to be inserted.
        table_name (str): Target table name.
        conn_str (str): Connection string to the database.
        mappings (dict[str, list[str]]): Dictionary of table's schema.
    """
    with create_engine(conn_str).begin() as conn:
        try:
            df.to_sql('temp', con=conn, if_exists='replace', index=False)
            columns = ','.join(mappings[table_name])
            query = f"""
                    INSERT IGNORE INTO {table_name} ({columns})
                    SELECT {columns} FROM temp;
            """
            conn.execute(text(query))
            logging.info(f'Loaded {len(df)} records to table {table_name}.')
            conn.execute(text('DROP TABLE temp;'))
        except IntegrityError:
            logging.error('New records violating the primary key '
                          f'in table {table_name}, skipping.')
