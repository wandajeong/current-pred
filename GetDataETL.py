import numpy as np
import pandas as pd

class getData:
  def __init__(self, db, code, inst_id):
    self.db = db 
    self.code = code
    self.inst_id = inst_id
    self.tag_names = ['전류', '전류시간당합계', 'GAS_VALVE'] 

  def tag_info(self):
    '''
    Extract tag_id meta data
    '''
    conn, cursor = db.sql_connect_1()
    query = "SELECT * FROM TAG"
    data = pd.read_sql(sql=query, con=conn)
    conn.close()
    tags_df = pd.DataFrame(data)
    return tags_df
    
  def get_arc_data(self):
    '''
    tag_id 기준 실시간 공정 데이터 불러오기 
    '''
    tags_df = self.tag_info()
    tag_dict ={}
    fst_times, lst_times =[], []
    
    for i, tag_name in enumerate(self.tag_names):
      ## tag_id 구하기 
      c1 = tags_df['CODE'] == self.code
      c2 = tags_df['DESC'] ==tag_name
      tag_id = tags_df.loc[c1&c2, 'TAG_ID'].values[0]
      
       ## tag_id에 해당하는 raw data 가져오기 
      if i==2: table = 'TABLE_DI'
      else: table = 'TABLE_AI'
        
      conn, cursor = self.db.sql_connect_1()
      query = f"SELECT * FROM {table} WHERE TAG_ID='{tag_id}'"
      data = pd.read_sql(sql=query, con=conn)
      conn.close()
      
      ## 데이터 전처리 
      data_ = (
        data[['TIME_STAMP', 'TAG_VAL']]
        .drop_duplicates(subset=['TIME_STAMP'], keep='last')
        .set_index('TIME_STAMP')
      )
      if i ==1: data.columns = ['AmpSumm']
      elif i==2: data.columns = ['Valve']
      else: data.columns = [tag_name]
        
      fst_times.append(data_.first_valid_index())
      lst_times.append(data_.last_valid_index())
      
      tag_dict[data_.columns.values[0]] = data_
      
    return tag_dict, fst_times, lst_times

  def data_organize(self, tag_dict, fst_times, lst_times):
    '''
    불러온 Raw data 후처리  
    '''
    isgo = 1 
    com_df = pd.DataFrame()
    
    fst_time = np.min(fst_times)
    lst_time = np.min(lst_times)
    
    for i, (col, data) in enumerate(tag_dict.items()):
      if i==2:
        if data.iloc[0].values[0] == 'OPEN':
          data.loc[fst_time] = 'OPEN'
          data = data.sort_index()
          
      sec_data = data.resample('1S').ffill()  # 1초단위 데이터 보간
      com_df = pd.concat([com_df, sec_data], axis=1)
    # 특정 열의 전체 데이터가 null
    com_df = com_df.iloc[:lst_time]
    for col in com_df.columns:
      if com_df[col].isnull().all():
        isgo =0

    return com_df, isgo


    
     

  
    













