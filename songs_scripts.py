import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load CSV file
songs_df = pd.read_csv("data/songs/songs.csv")

# Establish MySQL Connection
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("SONGS_DB_NAME")
        )
        if connection.is_connected():
            print("Connected to MySQL.")
            return connection
    except mysql.connector.Error as e:
        print("Error:", e)
        return None

# Optimized batch insertion function
def insert_songs_batch(connection, data):
    insert_query = """
        INSERT INTO songs (
            id, track_id, artists, album_name, track_name, popularity,
            duration_ms, explicit, danceability, energy, `key`, loudness,
            mode, speechiness, acousticness, instrumentalness, liveness,
            valence, tempo, time_signature, track_genre
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # 🔹 Convert DataFrame to a List of Tuples (handling NaNs)
    records = [tuple(x) for x in data.astype(object).where(pd.notnull(data), None).values]

    batch_size = 1000  # Insert 1000 rows at a time

    try:
        with connection.cursor() as cursor:
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                cursor.executemany(insert_query, batch)
                connection.commit()
                print(f"✅ Inserted {i + len(batch)} records so far...")
        print("🎉 All records inserted successfully!")
    except mysql.connector.Error as e:
        print("❌ Failed to insert batch:", e)
        connection.rollback()

# Main function
def main():
    connection = connect_to_database()
    if connection:
        try:
            insert_songs_batch(connection, songs_df)
        finally:
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
