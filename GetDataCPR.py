import sys
import pandas as pd
import datetime as dt
import warnings
warnings.filterwarnings('ignore')

class getData:
    def __init__(self, code, time_stamp, inst_id, db):
        self.code = code
        self.time_stamp = time_stamp
        self.inst_id = inst_id
        self.db = db 
        
    def extract_data(self):
        '''
        database에서 입력 CODE에 해당하는 TABLE 가져오기
        '''
        isquit = 0
        conn, cursor = db.sql_connect_2()
        query = f"""SELECT CODE, TIME_STAMP, PAMP_AVG FROM FROM HSUMM
            WHERE CODE = {self.code}"""
        data = pd.read_sql(sql=query, con=conn)
        conn.close()
    
        data = pd.DataFrame(data).sort_values(by='TIME_STAMP')
        cut_date = self.time_stamp - pd.Timedelta(weeks=40)
        data = data.loc[data['TIME_STAMP'] > cut_date]
        if data.shape[0] ==0:
            print('{',
                f'"inst_id" : "{self.inst_id}", "Result": "Fail, CODE does not exist in database.", "output": -1',
            '}')   
            isquit = 1
          
        return data, isquit  
      
    def preprocess(self, data):
        '''
        데이터 전처리
        - 데이터셋 1시간 단위로 선형 보간
        - 누적전류값 생성
        '''
        try:
            df = pd.DataFrame()
            fst_idx = data.first_valid_index()
            lst_idx = data.last_valid_index()
            new_idx = pd.date_range(start = data.loc[fst_idx, 'TIME_STAMP'], end = data.loc[lst_idx, 'TIME_STAMP'], freq = 'H')
            
            df = data.set_index('TIME_STAMP').reindex(new_idx)
            df.index.name = 'ds'
            df = df.interpliate().reset_index()

            ## 누적전류 구하는 로직 ## 
            starts_0, ends_0 = [], []
            fidx = df.first_valid_index()
            lidx = df.last_valid_index()
            
            df['flag'] = df['PAMP_AVG']==0  # 연속으로 0인 구간 찾기 
            df['group'] = (df['flag'] != df['flag'].shift()).cumsum()
            groups = df[df['flag'].groupby('group')
            
            for name, group in groups:
                if len(group) >= 3:
                    starts_0.append(group.index[0])
                    ends_0.append(group.index[-1])
                    
            ends_0 = [fidx] + ends_0
            starts_0 = starts_0 + [lidx]
            for ed, st in zip(ends_0, starts_0):
                df.loc[ed:st, 'y'] = df.loc[ed:st, 'PAMP_AVG'].cumsum()  # y: 누적전류 
            df['y'] = df['y'].ffill()
            
        except Exception as e:
            print('{', f'"inst_id" : "{self.inst_id}", "Result": "Fail, {e}", "output": -1','}')   

        return df[['ds','y']]

    def get_cut_data(self, data):
        '''
        time_stamp(입력값) 기준으로 과거 30일 데이터만 filtering
        '''
        df = pd.DataFrame()
        isquit = 0 
        try:
            lst_idx = data[data['ds']==self.time_stamp].index.values[0]
        except:
            lst_idx = data['ds'].sub(self.time_stamp).abs().idxmin()

        if lst_idx - 60*24 <0:
            print('{', f'"inst_id" : "{self.inst_id}", 
                "Result": "Fail, The operation period is at least 60 days.", "output": -1','}')
            isquit = 1 
        else: 
            df = data.loc[lst_idx - 60*24 + 1:lst_idx]
            
        return df, isquit
        

        



      
    
    
    
    
    
