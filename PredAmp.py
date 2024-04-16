import pandas as pd 
from prophet import Prophet

class PredInsert:
    def __init__(self, code, db):
        self.code = code
        self.db = db
        
    def train_pred(self, df):
        '''
        30일 과거 데이터를 입력받아, 미래 30일 누적 전류값을 예측 
        예측 결과 중 정오(00:00:00)에 해당되는 예측값만 출력
        '''
        try:
            pred_mid = pd.DataFrame()
            model = Prophet()
            model.fit(df)
            
            test_start_time = df['ds'].iloc[-1] + pd.TimeDelta(hours=1)
            future_range = pd.date_range(start = test_start_time, period=60*24, freq='H')
            future_range = pd.DataFrame({'ds': future_range})
            forecast = model.predict(future_range)
            
            pred_df = forecast[['ds', 'yhat']]
            pred_mid = (
                pred_df
                .loc[lambda x: x['ds'].dt.strftime('%H:%M:%S') =='00:00:00']
                .reset_index(drop=True)
                )
        except Exception as e:
            print("Output: -1")
            print(f"Result: Fail ({e})")
            
        return pred_mid
        
    def postprocess(self, pred_mid):
        '''
        예측값을 받아서 database에 입력할 수 있는 형태로 후처리 
        '''
        try:
            pred_mid.columns = ['TIME_STAMP', 'PRED_AMP_SUM']
            pred_mid['CODE'] = self.code
            pred_mid = pred_mid[['CODE', 'TIME_STAMP', 'PRED_AMP_SUM']]
        except Exception as e:
            print("Output: -1")
            print(f"Result: Fail ({e})")

        return pred_mid 

    def insert_data(self, results):
        '''
        예측값을 database에 INSERT OR UPDATE
        '''
        conn, cursor = self.sql_connect_2()
        table = "PRED_AMP"
        try:
    	    for record in result_tuples:
                code = record[0]
                time_stmp = record[1]
                pred_amp = record[2]
                
                cursor.execute(f"""SELECT * FROM {table} 
                    WHERE CODE = %s AND TIME_STAMP = %s""", (code, time_stmp))

                existing_record = cursor.fetchone()
                if existing_record:
                    query_update = f"""UPDATE {table} 
                    SET PRED_AMP_SUM = %s, TXN_TIME = CURRENT_TIMESTAMP
                    WHERE CODE = %s AND TIME_STAMP=%s"""
                    cursor.execute(query_update, (pred_amp, code, time_stmp))
                    conn.commit()
                else:
                    query = f"""INSERT INTO {table} (CODE, TIME_STAMP, PRED_AMP_SUM, TXN_TIME) 
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)"""
                    cursor.execute(query, record)
            output_param = 2
        except Exception as e:
            output_param = -1
            
        conn.close()
        return output_param
    
    def insert_oh_data(self, results):
        '''
        예측값을 database에 INSERT OR UPDATE
        '''
        conn, cursor = self.sql_connect_2()
        table = "OH_PRED"
        try:
    	    for record in result_tuples:
                query = f"""INSERT INTO {table} (CODE, PRED_DATE, WORK_TYPE, REMARK, TXN_TIME) 
                        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)"""
                cursor.execute(query, record)
            output_param = 2 
        except Exception as e:
            output_param = -1
            
        conn.close()
        return output_param






        
                
