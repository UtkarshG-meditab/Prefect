from prefect import flow, task, get_run_logger
from prefect_email import EmailServerCredentials, email_send_message
from prefect import get_run_logger
from script import (
    load_config,
    get_sybase_connection,
    get_postgres_engine,
    execute_sybase_query,
    sync_to_postgres,
    create_indexes,
    execute_postgres_procedures
)
import datetime


@task
def send_email_notification(subject: str, message: str, email_cfg: dict):
    logger = get_run_logger()
    try:
        creds = EmailServerCredentials.load(email_cfg["block_name"])
        recipients = email_cfg["recipients"]

        for recipient in recipients:
            logger.info(f"Sending email to {recipient}...")
            result = email_send_message.with_options(name=f"notify {recipient}").submit(
                email_server_credentials=creds,
                subject=subject,
                msg=message,
                email_to=recipient
            )
            result.result()  # Wait synchronously
        logger.info("All emails sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")

@task
def extract_and_load():
    logger = get_run_logger()
    config = load_config()
    sybase_conn = get_sybase_connection(config['sybase'])
    postgres_engine = get_postgres_engine(config['postgres'])
    cursor = sybase_conn.cursor()

    for item in config['queries']:
        try:
            df = execute_sybase_query(cursor, item)
            if not df.empty:
                table = item.get("target_table", item['name'])
                sync_to_postgres(df, table, postgres_engine)
                create_indexes(postgres_engine, table, item.get('index_columns', []), item.get('index_prefix', 'idx'))
            else:
                logger.warning(f"No data from {item['name']}")
        except Exception as e:
            logger.error(f"Error in {item['name']}: {e}")

    cursor.close()
    sybase_conn.close()
    return postgres_engine, config


@task
def run_postgres_procedures(postgres_engine, config):
    success = execute_postgres_procedures(postgres_engine, config.get("postgres_procedures", []))
    if not success:
        raise Exception("One or more PostgreSQL procedures failed.")
    return "Data sync complete."



@flow(name="Sybase-to-Postgres ETL Flow", log_prints=True)
def main_etl_flow():
    config = load_config()
    email_cfg = config.get("email", {})
    subject = email_cfg.get("subject", "ETL Status")

    try:
        postgres_engine, config = extract_and_load()
        run_postgres_procedures(postgres_engine, config)

        # Success Email
        send_email_notification(
            subject=f"{subject}",
            message="ETL process completed successfully.",
            email_cfg=email_cfg
        )
    except Exception as e:
        # Failure Email
        send_email_notification(
            subject=f"{subject}",
            message=f"ETL process failed with error:\n{str(e)}",
            email_cfg=email_cfg
        )
        raise


# if __name__ == "__main__":
#     main_etl_flow()