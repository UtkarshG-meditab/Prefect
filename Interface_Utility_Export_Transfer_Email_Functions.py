import os
import sys
import pyodbc
import shutil
import smtplib
import paramiko
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
from sys import exit
import pandas as pd
# import openpyxl
import importlib.util
import gnupg
import subprocess
import json

# Load the configuration from the specified file
# Load the configuration from a JSON file
def load_config(config_path: str):
    """
    Load configuration from a JSON file.
    Accepts either a relative path or an absolute path.
    Returns (server_config, script_config, sftp_config, email_config).
    """
    try:
        # Resolve relative to absolute if needed
        if not os.path.isabs(config_path):
            config_path = os.path.join(os.getcwd(), config_path)

        with open(config_path, "r") as f:
            config = json.load(f)

        return (
            config["server_config"],
            config["script_config"],
            config["sftp_config"],
            config["email_config"],
        )

    except Exception as e:
        print(f"Error loading configuration from {config_path}: {e}")
        raise


# Logging configuration
def setup_logging(config_file_path, file_name):
    # Get the directory of the config file
    config_dir = os.path.dirname(config_file_path)
    log_file_name = os.path.join(config_dir, f"{file_name}.log")
    logging.basicConfig(filename=log_file_name, level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')


# Function to decrypt passwords
def decrypt_password(encrypted_password, cipher):
    return cipher.decrypt(encrypted_password.encode()).decode()


# Connect to the database and execute queries
def execute_queries(cursor, script_config, from_date_str, to_date_str, data_folder, backup_folder):
    error_messages = []
    file_export_successful = True
    date_format = script_config.get('date_format', '%m%d%Y')
    exported_file_count = 0     # Counter for the number of files exported
    exported_files = []
    skip_file_count = 0

    # Iterate over each item in the script execution order
    for item in script_config['execution_order']:
        item = item.lower()

        if item in script_config:
            for file_prefix, query_name in script_config[item].items():
                # Generate filename based on the prefix and date range
                extension = script_config.get('extension', 'csv')
                # Parse the from and to dates
                from_date = parse_date(from_date_str, date_format) if from_date_str else None
                to_date = parse_date(to_date_str, date_format) if to_date_str else None
                filename = generate_filename(file_prefix, from_date, to_date, extension, script_config)
                file_path = os.path.join(data_folder, filename)
                backup_file_path = os.path.join(backup_folder, filename)
                
                # Skip the file if it already exists
                if os.path.exists(file_path) or os.path.exists(backup_file_path):
                    logging.info(f"File {filename} already exists. Skipping export.")
                    skip_file_count += 1
                    continue  # Move to the next file if it exists

                try:
                    # Execute procedure or view query based on item type
                    if item == 'procedures':
                        query = f'CALL dba.{query_name}(?, ?, ?);'  # Pass filename and date arguments
                        cursor.execute(query, (file_path, from_date, to_date))

                        # Export results to file
                        if extension == 'xlsx':
                            export_data_to_file(cursor, file_path, extension, script_config)
                    elif item == 'views':
                        query = f'SELECT * FROM dba.{query_name};'
                        cursor.execute(query)
                        export_success = export_data_to_file(cursor, file_path, extension, script_config)

                        if export_success:
                            logging.info(f'{filename} successfully exported at the "{data_folder}" path.')
                            exported_file_count += 1
                            exported_files.append(filename)
                        else:
                            logging.info(f'{filename} was not exported (0 rows and allow_empty_export=N).')
                            skip_file_count += 1
                        

                except pyodbc.Error as e:
                    # Handle query execution errors
                    error_messages.append(f"Failed to export file: {filename} due to database error.")
                    logging.error(f"File Export Error: {e}")
                    file_export_successful = False

                except Exception as e:
                    error_messages.append(f"Failed to export file: {filename} {e}")
                    logging.error(f"File Export Error: {e}")
                    file_export_successful = False

    return error_messages, file_export_successful, exported_file_count, exported_files, skip_file_count


# Generate the filename based on date range and prefix
def generate_filename(file_prefix, from_date, to_date, extension, script_config):
    date_format = script_config.get('date_format', '%m%d%Y')
    file_name_separator = script_config.get('file_name_separator', '_')

    # If from_date and to_date are datetime objects, format them to the desired date format
    if isinstance(from_date, datetime):
        from_date = from_date.strftime(date_format)
    if isinstance(to_date, datetime):
        to_date = to_date.strftime(date_format)

    if from_date and from_date != 'today':
        if file_prefix:
            return f"{file_prefix}{file_name_separator}{from_date}{file_name_separator}{to_date}.{extension}"
        else:
            return f"{from_date}{file_name_separator}{to_date}.{extension}"
    else:
        if file_prefix:
            return f"{file_prefix}{file_name_separator}{to_date}.{extension}"
        else:
            return f"{to_date}.{extension}"
        

# Date parsing function to handle custom date formats or relative date values
def parse_date(date_str, date_format):
    if not date_str:
        return None
    
    date_str = date_str.lower()

    # Parse relative dates like 'today + x days'
    if date_str.startswith('today'):
        if '+' in date_str:
            days_offset = int(date_str.split('+')[-1].strip())
            return datetime.now() + timedelta(days=days_offset)
        elif '-' in date_str:
            days_offset = int(date_str.split('-')[-1].strip())
            return datetime.now() - timedelta(days=days_offset)
        else:
            return datetime.now()
    else:
        try:
            # Parse absolute dates using the provided format
            return datetime.strptime(date_str, date_format)
        except ValueError:
            logging.error(f"Invalid date format: {date_str}")
            exit()


def export_files(df, file_path, quote_style, extension, separator = None):
    quoting = 1 if quote_style == '"' else 3        # 1: QUOTE_ALL for double quotes, 3: QUOTE_NONE for single quotes
    escapechar = '\\' 
    if extension == 'xlsx':                            # handle excel export
        df.to_excel(file_path, index = False, engine = 'openpyxl')
    elif extension == 'csv':                           # handle csv export
        df.to_csv(file_path, index = False, quoting = quoting, sep = separator, escapechar = escapechar)
    elif extension == 'txt':
        sep = separator if separator else '\t'
        df.to_csv(file_path, index=False, sep = sep, lineterminator = '\n', quoting = quoting, escapechar = escapechar)
    else:
        logging.error(f"Unsupported file format: {extension} for file: {file_path}. Supported formats are xlsx, csv, txt.")
        return False
    
    return True



# Export the query result to a file (CSV or XLSX)
def export_data_to_file(cursor, file_path, extension, script_config):
    rows = cursor.fetchall()        # Fetch all rows from the cursor
    columns = [desc[0] for desc in cursor.description]      # Get column names

    allow_empty = script_config.get("allow_empty_export", "N").upper()

    if not columns:
        logging.error(f"No columns found in the query result for {file_path}.")
        return False

    if not rows:  # 0 rows case
        if allow_empty == "Y":
            logging.info(f"Query returned 0 rows. Exporting headers only to {file_path}.")
            df = pd.DataFrame(columns=columns)   # empty DataFrame, just headers
            quote_style = script_config.get('quote_style', "'")
            separator = script_config.get('separator', ',')
            export_files(df, file_path, quote_style, extension, separator)
            return True
        else:
            logging.info(f"Query returned 0 rows. Skipping export for {file_path} (per config).")
            return False

    # Normal case: rows exist
    rows = [list(row) for row in rows]
    df = pd.DataFrame(rows, columns=columns)
    quote_style = script_config.get('quote_style', "'")
    separator = script_config.get('separator', ',')
    export_files(df, file_path, quote_style, extension, separator)

    return True
        
def encrypt_files_with_gnupg(data_folder, script_config):
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # gpg_path = script_config['gpg_binary_path']
    public_key_path = script_config['pgp_key_file_path']
    # gpg_home = script_config['gpg_home_directory']

    # # Ensure GPG home exists
    # os.makedirs(gpg_home, exist_ok=True)

    # # Set environment variable to define GPG home
    # os.environ['GNUPGHOME'] = gpg_home

    # Initialize GPG
    gpg = gnupg.GPG()#.GPG(binary=gpg_path, homedir=gpg_home, ignore_homedir_permissions=True)

    # Import the public key
    try:
        with open(public_key_path, 'rb') as f:
            import_result = gpg.import_keys(f.read())
            if not import_result.count:
                logging.error("Failed to import public key.")
                return False, []
            else:
                logging.info(f"Public key imported. Fingerprints: {import_result.fingerprints}")
    except Exception as e:
        logging.error(f"Error importing public key: {e}")
        return False, []

    # Use the first available key from the keyring for encryption
    recipient_key = import_result.fingerprints[0]
    logging.info(f"Using recipient key with fingerprint: {recipient_key} for encryption.")

    # Encrypt files
    encrypted_files = []
    for filename in os.listdir(data_folder):
        if filename.endswith(script_config['extension']):
            file_path = os.path.join(data_folder, filename)
            encrypted_path = file_path + ".gpg"

            try:
                with open(file_path, 'rb') as f:
                    status = gpg.encrypt_file(
                        f,
                        recipients=[recipient_key],
                        output=encrypted_path,
                        always_trust=True
                    )

                if status.ok:
                    logging.info(f"Encrypted {filename} -> {encrypted_path}")
                    encrypted_files.append(encrypted_path)
                else:
                    logging.error(f"Encryption failed for {filename}: {status.status}")
                    return False, []

            except Exception as e:
                logging.error(f"Exception encrypting {filename}: {e}")
                return False, []

    return True, encrypted_files


# Connect to SFTP server
def connect_to_sftp(hostname, username, password=None, ppk_file_path=None, port=22):
    error_messages= []
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect using either password or private key
        if ppk_file_path:
            private_key = paramiko.RSAKey.from_private_key_file(ppk_file_path)
            ssh_client.connect(hostname=hostname, username=username, pkey=private_key, port=port)
        elif password:
            ssh_client.connect(hostname=hostname, username=username, password=password, port=port)
        else:
            logging.error("Neither password nor PPK file provided.")

        logging.info('SFTP Connection successful!')
        return ssh_client
    except Exception as e:
        # Log SFTP connection errors
        error_messages.append("Error connecting to SFTP.")
        logging.error(f"SFTP Error: {e}")
        return None


# Transfer files via SFTP
def transfer_files_sftp(localdirectory, ftp_client, server_config, sftp_config, script_config):
    files_uploaded = 0
    files_not_uploaded = 0
    backup_files_moved = 0
    uploaded_files = []
    failed_files = []
    backup_files = []
    error_messages = []

    try:
        for filename in os.listdir(localdirectory):

            if script_config['encrypt_files'] == 'Y' and filename.endswith('.gpg'):
                local_file_path = os.path.join(localdirectory, filename)
                remote_file_path = os.path.join(sftp_config['remotedirectory'], filename)

                try:
                    # Upload encrypted file
                    ftp_client.put(local_file_path, remote_file_path, confirm=False)
                    logging.info(f'{filename} uploaded successfully.')
                    files_uploaded += 1
                    uploaded_files.append(filename)

                    # Move encrypted file to backup
                    backup_file_path = os.path.join(server_config['backup_path'], filename)
                    shutil.move(local_file_path, backup_file_path)
                    logging.info(f'{filename} moved to backup successfully')
                    backup_files_moved += 1
                    backup_files.append(filename)

                    # Move original unencrypted file to backup
                    original_filename = filename.rsplit('.', 1)[0]
                    original_file_path = os.path.join(localdirectory, original_filename)
                    original_backup_path = os.path.join(server_config['backup_path'], original_filename)

                    if os.path.exists(original_file_path):
                        try:
                            shutil.move(original_file_path, original_backup_path)
                            logging.info(f'{original_filename} (original) moved to backup successfully')
                            backup_files_moved += 1
                            backup_files.append(original_filename)
                        except Exception as e:
                            logging.error(f'Failed to move original file {original_filename} to backup. Error: {e}')
                except Exception as e:
                    error_messages.append(f'File {filename} not uploaded over SFTP.')
                    logging.error(f'File {filename} not uploaded over SFTP. Error: {e}')
                    files_not_uploaded += 1
                    failed_files.append(filename)

            elif script_config['encrypt_files'] == 'N' and filename.endswith(script_config['extension']):
                local_file_path = os.path.join(localdirectory, filename)
                remote_file_path = os.path.join(sftp_config['remotedirectory'], filename)

                try:
                    ftp_client.put(local_file_path, remote_file_path, confirm=False)
                    logging.info(f'{filename} uploaded successfully.')
                    files_uploaded += 1
                    uploaded_files.append(filename)

                    # Move the file to the backup directory
                    backup_file_path = os.path.join(server_config['backup_path'], filename)
                    shutil.move(local_file_path, backup_file_path)
                    logging.info(f'{filename} moved to backup successfully')
                    backup_files_moved += 1
                    backup_files.append(filename)
                except Exception as e:
                    error_messages.append(f'File {filename} not uploaded over SFTP.')
                    logging.error(f'File {filename} not uploaded over SFTP. Error: {e}')
                    files_not_uploaded += 1
                    failed_files.append(filename)
    except Exception as e:
        error_messages.append(f"Error during SFTP file transfer: {e}")
        logging.error(f"SFTP Error: {e}")

    return files_uploaded, files_not_uploaded, backup_files_moved, uploaded_files, failed_files, backup_files, error_messages


# Send an email notification with file upload details
def send_email(smtp_connection, email_sender, email_recipients, email_subject, exported_file_count, exported_files, files_uploaded, files_not_uploaded, 
               uploaded_files, failed_files, error_messages, total_file_count,send_when_successful, backup_files_moved, backup_files, skip_file_count):

    msg = MIMEMultipart()
    recipient = email_recipients.split(',')
    msg['From'] = email_sender
    msg['To'] = ', '.join(recipient)
    msg['Subject'] = email_subject

    # Prepare the email body
    email_body = "Hello,\n\n"
    if error_messages:
        email_body += "Errors encountered during the process:\n" + "\n".join(error_messages) + "\n\n"
    
    email_body += f"Total Files to be exported: {total_file_count}\n"
    email_body += f"Total Files exported: {exported_file_count} - {exported_files}\n"
    email_body += f"Total Files uploaded over the SFTP: {files_uploaded} - {uploaded_files}\n"
    email_body += f"Total Files trasferred into the backup: {backup_files_moved} - {backup_files}\n"
    email_body += f"Files not uploaded: {files_not_uploaded} - {failed_files}\n"
    email_body += "\nThanks."

    msg.attach(MIMEText(email_body, 'plain'))

    if skip_file_count != total_file_count:
        if send_when_successful == 'Y' or files_not_uploaded > 0 or exported_file_count < total_file_count or backup_files_moved != exported_file_count:
            try:
                smtp_connection.sendmail(email_sender, recipient, msg.as_string())
                logging.info(f"Email sent to: {', '.join(recipient)}")
            except Exception as e:
                logging.error(f"Error sending email: {e}")