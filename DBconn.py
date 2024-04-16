import pymssql

class FetchDB:
  def __init__(self):
    self.serverip = ["server_1","server_2"]
    self.user = ["username1","username2"]
    self.password = "password"
    self.database = ["DB1","DB2"]

  def sql_connect_1(self):
    conn = pymssql.connect(
      server = self.serverip[0], user = self.user[0], password = self.password, database = self.database[0]
    )
    cursor = conn.cursor()
    return conn, cursor

  def sql_connect_2(self):
    conn = pymssql.connect(
      server = self.serverip[1], user = self.user[1], password = self.password, database = self.database[1]
    )
    cursor = conn.cursor()
    return conn, cursor

  def insert_data(self, results):
    """
    예측 결과를 database에 입력
    동일한 CODE와 시간대가 db에 있으면 UPDATE, 없으면 INSERT
    return: 2 (insert 성공) / -1 ( insert 실패 )
    """
    hsum_df = hsum_df.iloc[1:-1]
    result_tuples = [tuple(row) for row in hsum_df.values]
    
    col_str = (str(hsum_df.columns.tolist())
      .replace("[","").replace("]","").replace("'","") + ', TXN_TIME')
    set_clause = ', '.join([f"{col}=%s" for col in hsum_df.columns]) + ' , TXN_TIME= CURRENT_TIMESTAMP'
    table = "TABLE_NAME"
    
    conn, cursor = self.sql_connect_2()

    try:
    	for record in result_tuples:
        code = record[0]
        time_stmp = record[1]

        query_check = f"""SELECT * FROM {table} 
                    WHERE CODE = %s AND TIME_STAMP = %s"""
        cursor.execute(query_check, (code, time_stmp))
        existing_record = cursor.fetchone()
      
        if existing_record:
          query_update = f"""UPDATE {table} SET {set_clause}
                WHERE CODE = %s AND TIME_STAMP=%s"""
          cursor.execute(query_update, record + (code, time_stmp))
          conn.commit()
        else:
          query = f"""INSERT INTO {table} ({col_str}) 
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,CURRENT_TIMESTAMP)"""
          cursor.execute(query, record)

      output_param = 2
    except Exception as e:
      output_param = -1
    
    conn.close()
    return output_param
