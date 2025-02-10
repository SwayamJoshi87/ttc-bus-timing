#!/usr/bin/env python3
import csv
import os
import logging
import psycopg2
from psycopg2 import sql, OperationalError

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def connect_db(database_url):
    try:
        logging.debug("Attempting to connect to PostgreSQL database.")
        conn = psycopg2.connect(database_url)
        logging.info("Successfully connected to the database.")
        return conn
    except OperationalError as e:
        logging.error("Error connecting to the database: %s", e)
        raise

def create_table(cur):
    try:
        logging.debug("Creating 'stops' table if not exists.")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stops (
                stop_id TEXT PRIMARY KEY,
                stop_code TEXT,
                stop_name TEXT NOT NULL,
                stop_lat DOUBLE PRECISION NOT NULL,
                stop_lon DOUBLE PRECISION NOT NULL
            );
        """)
        logging.info("'stops' table created or already exists.")
    except Exception as e:
        logging.error("Error creating table: %s", e)
        raise

def insert_stop(cur, stop):
    try:
        cur.execute("""
            INSERT INTO stops (stop_id, stop_code, stop_name, stop_lat, stop_lon)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (stop_id) DO NOTHING;
        """, stop)
        logging.debug("Inserted stop with stop_id: %s", stop[0])
    except Exception as e:
        logging.error("Error inserting stop %s: %s", stop[0], e)
        raise

def create_indexes(cur):
    try:
        logging.debug("Creating index on stop_name.")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stop_name ON stops (stop_name);")
        logging.debug("Creating index on stop_code.")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stop_code ON stops (stop_code);")
        logging.info("Indexes created (if they did not already exist).")
    except Exception as e:
        logging.error("Error creating indexes: %s", e)
        raise

def main():
    DATABASE_URL = "postgresql://neondb_owner:npg_P1Kj2QvMgCiB@ep-bitter-flower-a87ggzzu-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
    if not DATABASE_URL:
        logging.error("DATABASE_URL environment variable is not set.")
        raise ValueError("DATABASE_URL environment variable is not set.")

    conn = connect_db(DATABASE_URL)
    cur = conn.cursor()

    # Create table
    create_table(cur)
    conn.commit()

    # Set to track unique stop_ids
    unique_stop_ids = set()
    records_inserted = 0

    # Read CSV file and insert rows
    try:
        with open("stops.txt", newline='', encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                stop_id = row["stop_id"].strip()
                if stop_id in unique_stop_ids:
                    logging.debug("Duplicate stop_id %s found; skipping.", stop_id)
                    continue
                unique_stop_ids.add(stop_id)
                stop_code = row["stop_code"].strip() if row.get("stop_code") else None
                stop_name = row["stop_name"].strip()
                try:
                    stop_lat = float(row["stop_lat"].strip())
                    stop_lon = float(row["stop_lon"].strip())
                except ValueError:
                    logging.warning("Invalid latitude/longitude for stop_id %s; skipping.", stop_id)
                    continue

                # Prepare the tuple of values
                stop_values = (stop_id, stop_code, stop_name, stop_lat, stop_lon)
                insert_stop(cur, stop_values)
                records_inserted += 1

        conn.commit()
        logging.info("Inserted %d unique stops.", records_inserted)
    except FileNotFoundError:
        logging.error("File 'stops.txt' not found.")
    except Exception as e:
        logging.error("Error processing CSV file: %s", e)
        conn.rollback()

    # Create indexes on stop_name and stop_code
    create_indexes(cur)
    conn.commit()

    # Clean up
    cur.close()
    conn.close()
    logging.info("Database connection closed. Script completed successfully.")

if __name__ == "__main__":
    main()
