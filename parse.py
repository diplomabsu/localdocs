import os
import fitz
import psycopg2
import argparse
import logging
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

SUPPORTED_EXTENSIONS = {".txt", ".pdf", ".md"}

TABLE_NAME = "extracted_files"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logging.info("Successfully connected to the database.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during DB connection: {e}")
        return None

def setup_database(conn):
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    id SERIAL PRIMARY KEY,
                    file_path TEXT UNIQUE NOT NULL, -- Ensure uniqueness
                    file_extension VARCHAR(10) NOT NULL,
                    content TEXT,
                    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
        logging.info(f"Table '{TABLE_NAME}' checked/created successfully.")
        return True
    except Exception as e:
        logging.error(f"Error setting up database table '{TABLE_NAME}': {e}")
        conn.rollback()
        return False

def insert_file_data(conn, file_path, extension, content):
    """Inserts extracted data into the database, skipping duplicates based on file_path."""
    if not conn:
        logging.error("Cannot insert data: No database connection.")
        return False

    sql = f"""
        INSERT INTO {TABLE_NAME} (file_path, file_extension, content)
        VALUES (%s, %s, %s)
        ON CONFLICT (file_path) DO NOTHING; -- Skip if file_path already exists
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (file_path, extension, content))
            conn.commit()
            if cur.rowcount > 0:
                 logging.debug(f"Successfully inserted data for: {file_path}")
                 return True
            else:
                 logging.info(f"Skipped duplicate file: {file_path}")
                 return False
    except Exception as e:
        logging.error(f"Error inserting data for {file_path}: {e}")
        conn.rollback()
        return False

def extract_text_from_txt(file_path):
    """Extracts text from a .txt file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except UnicodeDecodeError:
        logging.warning(f"UTF-8 decoding failed for {file_path}. Trying latin-1.")
        try:
            with open(file_path, 'r', encoding='latin-1') as f:
                return f.read()
        except Exception as e:
             logging.error(f"Could not read TXT file {file_path} even with fallback: {e}")
             return None
    except Exception as e:
        logging.error(f"Error reading TXT file {file_path}: {e}")
        return None

def extract_text_from_md(file_path):
    """Extracts text from a .md file (treating it as plain text)."""
    return extract_text_from_txt(file_path)

def extract_text_from_pdf(file_path):
    """Extracts text from a .pdf file using PyMuPDF."""
    text = ""
    try:
        with fitz.open(file_path) as doc:
            if doc.is_encrypted and not doc.authenticate(''):
                 logging.warning(f"Skipping password-protected PDF (without password): {file_path}")
                 return None
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                text += page.get_text("text")
        return text
    except fitz.fitz.FileNotFoundError:
         logging.error(f"PDF file not found (fitz error): {file_path}")
         return None
    except fitz.fitz.FileDataError as e:
        logging.error(f"Error processing PDF file (likely corrupted or invalid format) {file_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error reading PDF file {file_path}: {e}")
        return None

def process_directory(directory_path, conn):
    """Walks through a directory, extracts text from supported files, and stores it."""
    if not conn:
        logging.error("Cannot process directory: No database connection.")
        return

    processed_count = 0
    skipped_count = 0
    error_count = 0
    duplicate_count = 0

    logging.info(f"Starting processing directory: {directory_path}")
    for root, _, files in os.walk(directory_path):
        for filename in files:
            try:
                file_path = os.path.join(root, filename)
                file_ext = os.path.splitext(filename)[1].lower()

                if file_ext in SUPPORTED_EXTENSIONS:
                    logging.info(f"Processing file: {file_path}")
                    content = None
                    if file_ext == ".txt":
                        content = extract_text_from_txt(file_path)
                    elif file_ext == ".md":
                        content = extract_text_from_md(file_path)
                    elif file_ext == ".pdf":
                        content = extract_text_from_pdf(file_path)

                    if content is not None:
                        if insert_file_data(conn, file_path, file_ext, content):
                             processed_count += 1
                        else:
                             duplicate_count +=1
                    else:
                        logging.warning(f"Could not extract text from: {file_path}")
                        error_count += 1
                else:
                    # logging.debug(f"Skipping unsupported file type: {file_path}") # Uncomment for verbose skipping logs
                    skipped_count += 1
            except Exception as e:
                logging.error(f"Unhandled error processing file {filename} in {root}: {e}")
                error_count += 1


    logging.info("--- Processing Summary ---")
    logging.info(f"Successfully processed and inserted: {processed_count} files")
    logging.info(f"Skipped (unsupported type):        {skipped_count} files")
    logging.info(f"Skipped (already in DB):          {duplicate_count} files")
    logging.info(f"Errors (extraction/processing):   {error_count} files")
    logging.info("--------------------------")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract text from files in a directory and store in PostgreSQL.")
    parser.add_argument("directory", help="The path to the directory to scan.")
    args = parser.parse_args()

    if not os.path.isdir(args.directory):
        logging.error(f"Error: Provided path '{args.directory}' is not a valid directory.")
        exit(1)

    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
         logging.error("Error: Database configuration is missing. Ensure .env file exists and is correctly populated or environment variables are set.")
         exit(1)

    db_conn = None
    try:
        db_conn = get_db_connection()
        if db_conn:
            if setup_database(db_conn):
                process_directory(args.directory, db_conn)
            else:
                logging.error("Failed to set up the database table. Aborting.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during script execution: {e}")
    finally:
        if db_conn:
            db_conn.close()
            logging.info("Database connection closed.")
