import sqlite3

connection = sqlite3.connect('analytics_sfcc.db');

cursor = connection.cursor()

date = '2021-02-01T15:50:02.000Z'

cursor.execute("insert into analytics values (?, ?, ?)",(date,date,date))
connection.commit()
connection.close()