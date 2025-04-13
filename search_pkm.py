import psycopg2
import psycopg2.extras
from psycopg2 import sql
import os
import logging
from dotenv import load_dotenv
import argparse

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

SUPPORTED_EXTENSIONS = {".pdf", ".md", ".txt"}
TABLE_NAME = "extracted_files"
SQL_SETUP_FILE = "setup_fts_enhanced.sql"
DEFAULT_SEARCH_LIMIT = 10
HEADLINE_OPTIONS = 'StartSel=***, StopSel=***, MaxFragments=1, MaxWords=35, MinWords=15, HighlightAll=TRUE'
FTS_CONFIG_EN = 'public.fts_english_unaccent'
FTS_CONFIG_RU = 'public.fts_russian_unaccent'
FTS_CONFIG_SIMPLE = 'pg_catalog.simple'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    conn = None
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        logging.error("Database credentials not fully configured in .env file.")
        return None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        with conn.cursor() as cur:
            cur.execute("SET client_min_messages TO NOTICE;")
        conn.commit()
        logging.info("Successfully connected to the database.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during DB connection: {e}")
        return None

def execute_sql_setup(conn, sql_file=SQL_SETUP_FILE):
    if not os.path.exists(sql_file):
        logging.error(f"SQL setup file not found: {sql_file}")
        return False
    try:
        if hasattr(conn, 'notices'):
             conn.notices.clear()

        with conn.cursor() as cur:
            with open(sql_file, 'r') as f:
                sql_script = f.read()
                cur.execute(sql_script)
        conn.commit()
        logging.info(f"Successfully executed SQL setup script: {sql_file}")

        if hasattr(conn, 'notices'):
             for notice in conn.notices:
                 logging.info(f"DB Notice: {notice.strip()}")
             conn.notices.clear()
        else:
             logging.info("No notices received from database (or notice capture not supported by driver/connection setup).")

        return True
    except (psycopg2.Error, IOError) as e:
        logging.error(f"Error executing SQL setup script: {e}")
        conn.rollback()
        if hasattr(conn, 'notices'):
             for notice in conn.notices:
                 logging.warning(f"DB Notice (during error): {notice.strip()}")
             conn.notices.clear()
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during SQL setup: {e}")
        conn.rollback()
        return False

def search_documents(conn, query_string: str, language: str = 'english', limit: int = DEFAULT_SEARCH_LIMIT):
    if not query_string:
        logging.warning("Search query cannot be empty.")
        return []

    valid_languages = {'english', 'russian', 'both', 'simple'}
    if language not in valid_languages:
        logging.error(f"Invalid language specified: {language}. Choose from {valid_languages}.")
        return None

    results = []
    query = None
    params = ()
    headline_opts = HEADLINE_OPTIONS

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            if language == 'english':
                fts_config = FTS_CONFIG_EN
                tsvector_col = 'content_tsv_en'
                query = sql.SQL("""
                    SELECT
                        file_path,
                        ts_rank_cd({tsvector_col}, query) as rank,
                        ts_headline(%s, content, query, %s) as headline
                    FROM
                        {table}, plainto_tsquery(%s, %s) query
                    WHERE
                        query @@ {tsvector_col}
                        AND file_extension IN %s
                    ORDER BY rank DESC
                    LIMIT %s;
                """).format(
                    table=sql.Identifier(TABLE_NAME),
                    tsvector_col=sql.Identifier(tsvector_col)
                )
                params = (fts_config, headline_opts, fts_config, query_string, tuple(SUPPORTED_EXTENSIONS), limit)

            elif language == 'russian':
                fts_config = FTS_CONFIG_RU
                tsvector_col = 'content_tsv_ru'
                query = sql.SQL("""
                    SELECT
                        file_path,
                        ts_rank_cd({tsvector_col}, query) as rank,
                        ts_headline(%s, content, query, %s) as headline
                    FROM
                        {table}, plainto_tsquery(%s, %s) query
                    WHERE
                        query @@ {tsvector_col}
                        AND file_extension IN %s
                    ORDER BY rank DESC
                    LIMIT %s;
                """).format(
                    table=sql.Identifier(TABLE_NAME),
                    tsvector_col=sql.Identifier(tsvector_col)
                )
                params = (fts_config, headline_opts, fts_config, query_string, tuple(SUPPORTED_EXTENSIONS), limit)

            elif language == 'simple':
                fts_config = FTS_CONFIG_SIMPLE
                tsvector_col = 'content_tsv_simple'
                query = sql.SQL("""
                    SELECT
                        file_path,
                        ts_rank_cd({tsvector_col}, query) as rank,
                        ts_headline(%s, content, query, %s) as headline
                    FROM
                        {table}, plainto_tsquery(%s, %s) query
                    WHERE
                        query @@ {tsvector_col}
                        AND file_extension IN %s
                    ORDER BY rank DESC
                    LIMIT %s;
                """).format(
                    table=sql.Identifier(TABLE_NAME),
                    tsvector_col=sql.Identifier(tsvector_col)
                )
                params = (fts_config, headline_opts, fts_config, query_string, tuple(SUPPORTED_EXTENSIONS), limit)

            elif language == 'both':
                en_fts_config = FTS_CONFIG_EN
                ru_fts_config = FTS_CONFIG_RU
                query = sql.SQL("""
                    SELECT
                        file_path,
                        COALESCE(ts_rank_cd(content_tsv_en, query_en), 0) +
                        COALESCE(ts_rank_cd(content_tsv_ru, query_ru), 0) as rank,
                        ts_headline(%s, content, query_en, %s) as headline
                    FROM
                        {table},
                        plainto_tsquery(%s, %s) query_en,
                        plainto_tsquery(%s, %s) query_ru
                    WHERE
                        (query_en @@ content_tsv_en OR query_ru @@ content_tsv_ru)
                        AND file_extension IN %s
                    ORDER BY rank DESC
                    LIMIT %s;
                """).format(table=sql.Identifier(TABLE_NAME))
                params = (en_fts_config, headline_opts, en_fts_config, query_string, ru_fts_config, query_string, tuple(SUPPORTED_EXTENSIONS), limit)

            if query and params:
                logging.debug(f"Executing search query with language '{language}': {cur.mogrify(query, params)}")
                cur.execute(query, params)
                results = [dict(row) for row in cur.fetchall()]
            else:
                 logging.error(f"Could not construct query for language '{language}'.")
                 return None

        logging.info(f"Found {len(results)} results for query '{query_string}' (language: {language}).")
        return results

    except psycopg2.Error as e:
        logging.error(f"Database error during search: {e}")
        if query and conn and not conn.closed:
             try:
                 safe_query_repr = conn.cursor().mogrify(query, params).decode('utf-8', errors='ignore')
                 logging.error(f"Failed query (parameters interpolated for debugging): {safe_query_repr}")
             except Exception as log_e:
                 logging.error(f"Failed query (could not mogrify): {query.as_string(conn) if query else 'N/A'}. Error during mogrify: {log_e}")
        else:
             logging.error(f"Failed query: Query object was not properly formed or connection closed ({'conn closed' if conn and conn.closed else 'conn ok/None'}).")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during search: {e}")
        if query and conn and not conn.closed:
             try:
                 safe_query_repr = conn.cursor().mogrify(query, params).decode('utf-8', errors='ignore')
                 logging.error(f"Failed query context (parameters interpolated): {safe_query_repr}")
             except Exception as log_e:
                 logging.error(f"Failed query context (could not mogrify): {query.as_string(conn) if query else 'N/A'}. Error during mogrify: {log_e}")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local PKM Search Engine using PostgreSQL FTS.")
    parser.add_argument("--setup", action="store_true", help="Run the enhanced SQL setup script (setup_fts_enhanced.sql).")
    parser.add_argument("-q", "--query", type=str, help="Search query string.")
    parser.add_argument("-l", "--lang", type=str, default="english", choices=['english', 'russian', 'both', 'simple'], help="Search language ('english' and 'russian' use unaccent).")
    parser.add_argument("-n", "--limit", type=int, default=DEFAULT_SEARCH_LIMIT, help="Maximum number of results.")

    args = parser.parse_args()

    conn = get_db_connection()

    if not conn:
        exit(1)

    setup_success = True
    if args.setup:
        logging.info(f"Running database setup using '{SQL_SETUP_FILE}'...")
        if not execute_sql_setup(conn):
            logging.error("Database setup failed. Please check logs.")
            setup_success = False
            conn.close()
            exit(1)
        else:
            logging.info("Database setup completed (or verified).")

    if setup_success and args.query:
        logging.info(f"Searching for: '{args.query}' (Language: {args.lang}, Limit: {args.limit})")
        search_results = search_documents(conn, args.query, args.lang, args.limit)

        if search_results is not None:
            if search_results:
                print("\n--- Search Results ---")
                for i, result in enumerate(search_results):
                    rank_str = f"{result.get('rank', 0.0):.4f}" if isinstance(result.get('rank'), (float, int)) else "N/A"
                    headline_text = result.get('headline', 'N/A').replace('\n', ' ').replace('\r', '').replace('\t', ' ')
                    print(f"{i+1}. Path: {result.get('file_path', 'N/A')} (Rank: {rank_str})")
                    print(f"   Context: ...{headline_text}...")
                print("--------------------\n")
            else:
                print("\n--- No results found. ---\n")
        else:
            print("\n--- An error occurred during the search. Check logs. ---\n")

    elif setup_success and not args.query and not args.setup:
         print("\nEntering interactive search mode. Press Ctrl+C or Ctrl+D to exit.")
         print(f"Using configurations: EN='{FTS_CONFIG_EN}', RU='{FTS_CONFIG_RU}', Simple='{FTS_CONFIG_SIMPLE}'")
         try:
             while True:
                 try:
                    search_term = input("Enter search query: ")
                    if not search_term: continue

                    lang_options = ['english', 'russian', 'both', 'simple']
                    lang_prompt = f"Enter language ({'/'.join(lang_options)}) [english]: "
                    lang_choice_input = input(lang_prompt).lower().strip()

                    if not lang_choice_input:
                        lang_choice = 'english'
                    elif lang_choice_input in lang_options:
                        lang_choice = lang_choice_input
                    else:
                        print(f"Invalid language '{lang_choice_input}', defaulting to 'english'.")
                        lang_choice = 'english'

                    search_results = search_documents(conn, search_term, lang_choice, args.limit)

                    if search_results is not None:
                        if search_results:
                            print("\n--- Search Results ---")
                            for i, result in enumerate(search_results):
                                rank_str = f"{result.get('rank', 0.0):.4f}" if isinstance(result.get('rank'), (float, int)) else "N/A"
                                headline_text = result.get('headline', 'N/A').replace('\n', ' ').replace('\r', '').replace('\t', ' ')
                                print(f"{i+1}. Path: {result.get('file_path', 'N/A')} (Rank: {rank_str})")
                                print(f"   Context: ...{headline_text}...")
                            print("--------------------\n")
                        else:
                            print("\n--- No results found. ---\n")
                    else:
                        print("\n--- An error occurred during the search. Check logs. ---\n")

                 except EOFError:
                     print()
                     break
                 except KeyboardInterrupt:
                     print()
                     break
         finally:
            print("\nExiting interactive mode.")

    if conn:
        if not conn.closed:
            conn.close()
            logging.info("Database connection closed.")
        else:
            logging.info("Database connection was already closed.")
