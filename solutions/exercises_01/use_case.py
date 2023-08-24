import os
import csv
import re
import sqlite3
import subprocess as sp
import datetime

CURRENT_WORKING_DIRECTORY = os.getcwd()
DATA_FOLDER_PATH = os.path.join(CURRENT_WORKING_DIRECTORY, "data")
# check function, check and create, delete, rename either file or folder
RAW_DATA_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH, "raw")
PROCESSED_DATA_FOLDER_PATH = os.path.join(DATA_FOLDER_PATH, "processed")
DATABASE_PATH = os.path.join(CURRENT_WORKING_DIRECTORY, "database")

# # TODO: make utils function that check if the path is zip/ rar, ...., then unzip and move the its it row procssed
# # After rename the exercise path
EXERCISE_DATA_FOLDER_PATH = f"{RAW_DATA_FOLDER_PATH}/tennis_atp-master.zip"

print(f"[PROCESS] Unziping {EXERCISE_DATA_FOLDER_PATH}")
# Retrieving data
# TODO: more on the sp functions
# Unzip the data to the processed folder
# sp.run(
#     ["unzip", EXERCISE_DATA_FOLDER_PATH, "-d", PROCESSED_DATA_FOLDER_PATH], check=True
# )
EXERCISE_DATA_FOLDER_PATH = f"{PROCESSED_DATA_FOLDER_PATH}/tennis_atp-master/"
print("[PROCESS] Unziping [DONE]")


def get_files_list(folder_path, pattern):
    """This function allows you to get the list of files to ingest"""
    return [
        os.path.join(folder_path, file)
        for file in os.listdir(folder_path)
        if re.match(pattern, file)
    ]


pattern = r"^atp_matches_\d{4}\.csv$"
files_to_process = get_files_list(EXERCISE_DATA_FOLDER_PATH, pattern)
print()
print(f"[FILES TO PROCESS]: {files_to_process[:3]}")

interested_headers = [
    "tourney_id",
    "tourney_name",
    "surface",
    "draw_size",
    "tourney_level",
    "tourney_date",
    "match_num",
    "winner_id",
    "winner_name",
    "winner_hand",
    "winner_ht",
    "winner_ioc",
    "winner_age",
]

new_atp_matches = []
for index, file_path in enumerate(files_to_process):
    filename = os.path.basename(file_path)
    with open(file_path, "r", encoding="utf-8") as atp_file:
        csv_reader = csv.reader(atp_file)
        headers = next(csv_reader)
        header_indices = [headers.index(header) for header in interested_headers]

        interested_values = (
            [interested_headers + ["filename", "time_stamp"]] if index == 0 else []
        )
        # Process each row
        for row in list(csv_reader)[1:11]:
            values = [row[index] for index in header_indices]
            values += [filename, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            interested_values.append(values)

        new_atp_matches.extend(interested_values)

with open(
    f"{PROCESSED_DATA_FOLDER_PATH}/all_matches.csv", "w", newline="", encoding="utf-8"
) as file:
    writer = csv.writer(file)
    writer.writerows(new_atp_matches)

print(f"ALL MATCHES CREATED [DONE] | {PROCESSED_DATA_FOLDER_PATH}/all_matches.csv")
print()
import pandas as pd

df = pd.read_csv(f"{PROCESSED_DATA_FOLDER_PATH}/all_matches.csv")
print(df.columns)

print(df.shape)
print()

EXERCISE_DATABASE_PATH = os.path.join(DATABASE_PATH, "tennis_atp.sqlite")
# TODO LEARNIN PACKAGE
db_connection = sqlite3.connect(EXERCISE_DATABASE_PATH)
db_cursor = db_connection.cursor()
db_cursor.execute(
    """CREATE TABLE IF NOT EXISTS tennis_atp (tourney_id text, tourney_name text, surface text, draw_size integer,
    tourney_level text, tourney_date integer, match_num integer, winner_id text, winner_name text, winner_hand text, 
    winner_ht float, winner_ioc text, winner_age float, filename text, time_stamp text)"""
)

print("[TABLE CREATED SUCCESSFULLY]")

with open(
    f"{PROCESSED_DATA_FOLDER_PATH}/all_matches.csv", "r", encoding="utf-8"
) as file:
    reader = csv.DictReader(file)
    for row in reader:
        cols = list(row.keys())
        row_values = tuple(row.values())
        db_cursor.execute(
            f"INSERT INTO tennis_atp ({','.join(cols)}) VALUES ({','.join(['?' for _ in cols])})",
            row_values,
        )

db_connection.commit()

print()
print("[SAMPLING QUERIES]")
print()

#####
# Easy Queries

print("Find the name of the tournament with the most matches.")
db_cursor.execute(
    """
SELECT tourney_name
FROM tennis_atp
GROUP BY tourney_name
ORDER BY COUNT(*) DESC
LIMIT 1;
"""
)

print("The tournament with the most matches is:", db_cursor.fetchone()[0])
print()

print("Find the average age of the winners of matches played on hard courts.")
db_cursor.execute(
    """
SELECT AVG(winner_age)
FROM tennis_atp
WHERE surface = 'Hard';
"""
)

print(
    "The average age of the winners of matches played on hard courts is:",
    db_cursor.fetchone()[0],
)
print()

# Medium Queries

print("Find the number of matches played by each player.")
db_cursor.execute(
    """
SELECT winner_name, COUNT(*) AS num_matches
FROM tennis_atp
GROUP BY winner_name
LIMIT 10;
"""
)

for row in db_cursor:
    print(row[0], "has played", row[1], "matches.")
print()

print("Find the number of matches played on each surface.")
db_cursor.execute(
    """
SELECT surface, COUNT(*) AS num_matches
FROM tennis_atp
GROUP BY surface;
"""
)

for row in db_cursor:
    print(row[0], "has", row[1], "matches played on it.")
print()
# Hard Query

print(
    "Find the average age of the winners of matches played on hard courts, but only for matches that were played in the year 2018"
)
db_cursor.execute(
    """
SELECT AVG(winner_age)
FROM tennis_atp
WHERE surface = 'Hard'
AND tourney_date BETWEEN '2018-01-01' AND '2018-12-31';
"""
)

print(
    "The average age of the winners of matches played on hard courts in 2018 is:",
    db_cursor.fetchone()[0],
)

db_connection.close()
