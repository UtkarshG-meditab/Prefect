from prefect.blocks.system import Secret
from prefect import flow, task, get_run_logger
from cryptography.fernet import Fernet
import pyodbc
import smtplib
from datetime import datetime

from Interface_Utility_Export_Transfer_Email_Functions import (
    load_config,
    decrypt_password,
    execute_queries,
    encrypt_files_with_gnupg,
    connect_to_sftp,
    transfer_files_sftp,
    send_email,
)


@task
def task_load_config(config_path: str):
    logger = get_run_logger()
    logger.info("Loading configuration...")
    logger.info(f'Config file path:{config_path}')
    server_config, script_config, sftp_config, email_config  = load_config(config_path)
    logger.info("Configuration loaded successfully.")
    return server_config, script_config, sftp_config, email_config


@task
def task_db_and_export(server_config, script_config, from_date_str, to_date_str, data_folder, backup_folder, cipher):
    logger = get_run_logger()
    logger.info("Connecting to database and exporting data...")

    decrypted_db_password = decrypt_password(server_config['ims_db_password'], cipher)
    exported_file_count, exported_files, skip_file_count, error_messages, file_export_successful = 0, [], 0, [], False

    try:
        conn = pyodbc.connect(
            f'Driver={server_config["Driver_name"]};'
            f'Server={server_config["ims_service_name"]};'
            f'Database={server_config["ims_db_name"]};'
            f'UID={server_config["ims_db_user_name"]};'
            f'PWD={decrypted_db_password};'
        )
        logger.info("Database connection established successfully.")
        cursor = conn.cursor()

        error_messages, file_export_successful, exported_file_count, exported_files, skip_file_count = execute_queries(
            cursor, script_config, from_date_str, to_date_str, data_folder, backup_folder
        )

        logger.error(f"Error Message in task_db_and_export: {error_messages}")
        logger.info(f"File Export count: {exported_file_count}")
        logger.info(f"Exported Files: {exported_files}")
        logger.info(f"Skip file count: {skip_file_count}")
        cursor.close()
        conn.close()
        logger.info("Database connection closed.")

    except Exception as e:
        logger.error(f"Database Error: {e}")
        error_messages.append("Database connection failed.")

    return error_messages, file_export_successful, exported_file_count, exported_files, skip_file_count


@task
def task_encrypt_files(script_config, data_folder):
    logger = get_run_logger()
    logger.info("Checking if encryption is required...")
    if script_config.get('encrypt_files', 'N').upper() == 'Y':
        encryption_successful, encrypted_files = encrypt_files_with_gnupg(data_folder, script_config)
        if not encryption_successful:
            raise Exception("GPG encryption failed. Aborting interface.")
        logger.info(f"Encryption completed for files: {encrypted_files}")
        return encrypted_files
    logger.info("Encryption not required, skipping.")
    return []


@task
def task_sftp_transfer(server_config, sftp_config, script_config, data_folder, cipher, db_connection_successful, file_export_successful):
    logger = get_run_logger()
    files_uploaded = files_not_uploaded = backup_files_moved = 0
    uploaded_files = failed_files = backup_files = []
    error_messages = []

    try:
        if db_connection_successful and file_export_successful and server_config.get('file_transfer_using_SFTP', '').upper() == "Y":
            decrypted_sftp_password = decrypt_password(sftp_config['sftp_password'], cipher)
            ssh_client = connect_to_sftp(
                sftp_config['hostname'], sftp_config['username'],
                decrypted_sftp_password, sftp_config['PPK_file_path'], int(sftp_config['port'])
            )
            if ssh_client:
                ftp_client = ssh_client.open_sftp()
                files_uploaded, files_not_uploaded, backup_files_moved, uploaded_files, failed_files, backup_files, error_messages = transfer_files_sftp(
                    data_folder, ftp_client, server_config, sftp_config, script_config
                )
                ftp_client.close()
                ssh_client.close()
    except Exception as e:
        logger.error(f"SFTP Error: {e}")
        error_messages.append(f"Failed to transfer files over SFTP: {e}")

    return files_uploaded, files_not_uploaded, backup_files_moved, uploaded_files, failed_files, backup_files, error_messages


@task
def task_send_email(email_config, cipher, exported_file_count, exported_files,
                    files_uploaded, files_not_uploaded, uploaded_files, failed_files,
                    error_messages, backup_files_moved, backup_files, skip_file_count):
    logger = get_run_logger()
    logger.info("Preparing to send email...")

    decrypted_email_password = decrypt_password(email_config['smtp_encrypted_password'], cipher)

    try:
        smtp_connection = smtplib.SMTP(email_config['email_smtp'], email_config['email_port'])
        smtp_connection.starttls()
        smtp_connection.login(email_config['email_sender'], decrypted_email_password)

        total_file_count = int(email_config['total_file_count'])
        formatted_date = datetime.now().strftime('%d %b %Y')
        email_subject = f"{email_config['client_name']} | {email_config['interface_name']} | {formatted_date}"

        send_email(
            smtp_connection,
            email_config['email_sender'],
            email_config['email_recipients'],
            email_subject,
            exported_file_count,
            exported_files,
            files_uploaded,
            files_not_uploaded,
            uploaded_files,
            failed_files,
            error_messages,
            total_file_count,
            email_config['send_when_successful'],
            backup_files_moved,
            backup_files,
            skip_file_count
        )
        smtp_connection.quit()
        logger.info("Email sent successfully.")
    except Exception as e:
        logger.error(f"Error sending email: {e}")


@flow(name="custom_interface_etl_flow")
def custom_interface_etl_flow(config_path: str):

    logger = get_run_logger()

    logger.info(f"ETL flow started using config: {config_path}")

    # Encryption setup
    # Load encryption key from Prefect block
    secret_block = Secret.load("custom-interface-password-encryption-key")
    encryption_key = secret_block.get()
    cipher = Fernet(encryption_key)


    # Task 1: Load config
    server_config, script_config, sftp_config, email_config = task_load_config(config_path)

    data_folder = server_config['folder_path']
    backup_folder = server_config['backup_path']
    from_date_str = script_config.get('from_date', 'today')
    to_date_str = script_config.get('to_date', 'today')

    # Task 2: DB + Export
    error_messages, file_export_successful, exported_file_count, exported_files, skip_file_count = task_db_and_export(
        server_config, script_config, from_date_str, to_date_str, data_folder, backup_folder, cipher
    )

    # Task 3: Encrypt files if needed
    encrypted_files = task_encrypt_files(script_config, data_folder)

    # Task 4: SFTP transfer
    files_uploaded, files_not_uploaded, backup_files_moved, uploaded_files, failed_files, backup_files, error_messages_sftp = task_sftp_transfer(
        server_config, sftp_config, script_config, data_folder, cipher, True, file_export_successful
    )
    error_messages.extend(error_messages_sftp)

    # Task 5: Send Email
    task_send_email(
        email_config, cipher,
        exported_file_count, exported_files,
        files_uploaded, files_not_uploaded,
        uploaded_files, failed_files,
        error_messages,
        backup_files_moved, backup_files,
        skip_file_count
    )

    logger.info("ETL flow completed successfully")

#
# if __name__ == "__main__":
#     etl_flow()
