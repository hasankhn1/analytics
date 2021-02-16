import sqlite3

connection = sqlite3.connect('analytics_sfcc.db');

cursor = connection.cursor()

create_table = "Create table analytics (uae_sfcc string, ksa_sfcc string, kuwait_sfcc string)"

cursor.execute(create_table);

connection.commit()
connection.close()