# ITP-487: Enterprise Data Analytics
# Instructor: Mike Lee, Viterbi School of Engineering, University of Southern California
# Python helper functions.

"""
to_mysql - inserts dataframe into mysql table 
"""
def to_mysql(df, host, port, user, password, database, table, debug = False):
  import pymysql
  import pandas as pd
  import time

  conn = pymysql.connect(
      host=host,
      port=port,
      user=user,
      passwd=password,
      db=database,
      charset='utf8mb4',
      autocommit = True)

  cursor = conn.cursor()
  cursor.execute("TRUNCATE " + table + " ;")

  # CHECK IF ANY DATA EXISTS TO INSERT
  if len(df) < 1:
    print('NO ROWS - NOTHING WAS DONE')
    return
  columnlist = df.columns.tolist()
  if len(columnlist) < 1:
    print('NO COLUMNS - NOTHING WAS DONE')
    return

  # CHECK IF TABLE EXISTS
  query = 'SHOW TABLES LIKE "' + table + '";'
  result = pd.read_sql(query, conn)
  if len(result) < 1:
    print('TABLE DOES NOT EXIST, CREATE TABLE WITH THE FOLLOWING COLUMNS BEFORE LOADING:\n')
    for column in columnlist:
      print('\t' + column)

  # CREATE SQL FORMAT STRING
  sqlformat = 'INSERT INTO ' + table + ' ('
  for column in columnlist:
    sqlformat += column + ','
  # remove last ,
  sqlformat = sqlformat[:-1] + ') VALUES ('
  for index in range(len(columnlist)):
    sqlformat += '"%s",'
  # remove last ,
  sqlformat = sqlformat[:-1] + ')'
  
  if debug:
    print('SQLFORMAT: ' + sqlformat)

  start = time.time()
  count = 0
  for index, row in df.iterrows():
    formatlist = []
    for x in range(len(row)):
      formatlist.append(str(row[x]))
    insertsql = sqlformat % tuple(formatlist)
    if debug:
      print(insertsql)
    count += 1
    cursor.execute(insertsql)

  end = time.time()
  print("ROWS ADDED: " + str(count))
  print("ELAPSED TIME: " + str(end-start))
  cursor.close()
