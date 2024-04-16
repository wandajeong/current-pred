import argparse
import pandas as pd 
from DBconn import FetchDB
import datetime as dt 
import holidays 
import sys
import json 
from GetDataETL import getData
from GetDataCPR import GetData
import HSUMM as hs
from PredAmp import PredInsert 
import warnings
warnings.filterwarnings('ignore')

parser = argparse.ArgumentParser()
parser.add_argument("--input_file", requred=True, help="JSON input file")
args =parser.parse_args()

dbobj = FetchDB()

def run_etl(code, inst_id, com_df, db):
    '''
    ETL API 실행
    '''
    hsum_df = hs.gen_hsum(com_df, code, inst_id)
    output = db.insert_data(hsum_df)
    if output ==2:
        print('{',
          f'"inst_id" : "{inst_id}", "Result": "Sucess.", "output": 2',
          '}')
    else:
        print('{',
          f'"inst_id" : "{inst_id}", "Result": "Fail, DB Insertion Error.", "output": -1',
          '}')    

def run_cpr(code, inst_id, df, db):
    '''
    누적전류 예측 API 실행
    '''
    pi = PredInsert(code, db)
    pred_mid = pi.train_pred(df)
    pred_mid = pi.postprocess(pred_mid)
    result_tuples = [tuple(row) for row in pred_mid.values]
    output = pi.insert_data(result_tuples)
    if output ==2:
        print('{',
          f'"inst_id" : "{inst_id}", "Result": "Sucess.", "output": 2',
          '}')
    else:
        print('{',
          f'"inst_id" : "{inst_id}", "Result": "Fail, DB Insertion Error.", "output": -1',
          '}')    

    return pred_mid, pi 

def get_meta(db):
    # 공휴일 정보 가져오기 
    present_yr = dt.datetime.today().year
    holdays = list(holidays.KOR(years = present_yr))
    # meta data 가져오기 
    conn, _ = db.sql_connect_2()
    qr = "" # CAPA 정보 (계획일수, 공장, 장비코드, CAPA VALUE 를 가져온다) 
    meta_df = pd.read_sql(sql=qr, con=conn)
    conn.close()
    return holdays, meta_df 

def adjust_day(day, holdays):
    if day.weekday()==5: day -= dt.timedelta(days=1)
    elif day.weekday()==6: day += dt.timedelta(days=1)
    while day in holdays:
        if day.weekday() ==4: day -= dt.timedelta(days=1)
        else: day += pd.Timedelta(days=1)

    return day 

def run_oh(code, pred_mid, pi, db):
    '''
    work order API 실행
    '''
    holdays, meta_df = get_meta(db)
    design_op_days = meta_df.loc[lambda x: x['CODE']==str(code), 'DESIGN_OP_DAYS'].values[0]
    cp_amp = meta_df.loc[lambda x: x['CODE']==str(code), 'CAPA_VALUE'].values[0]
    th_oh = cp_amp * 24 * desing_op_days  # threshold value 
    
    c1 = pred_mid['PRED_AMP_SUM'] >= th_oh
    c2 = pred_mid['PRED_AMP_SUM'] < (th_oh = 1000000)
    oh_pred = pred_mid[c1&c2]
    if oh_pred.shape[0]==0: pass
    else:  # threshold와 매칭된 time_stamp가 있다면, db insert 
        oh_day = oh_pred['TIME_STAMP'].iloc[0]
        oh_day = adjust_day (oh_day, holdays)
        oh_res = pd.DataFrame({'CODE': code, 
                               'PRED_DATE': oh_day, 
                               'WORK_TYPE': '정기', 
                               'REMARK': ''}, 
                              index=[0])
        res_tuples = [tuple(row) for row in oh_res.values]
        output = pi.insert_oh_data(res_tuples)

def main(input_data, db):
    '''
    FUNCTION TYPE에 따른 API 분기
    '''
    if 'ETL' in list(input_data.keys()):
        com_df = pd.DataFrame()
        input_cond = input_data['ETL']
        for cond in input_cond: 
            inst_id = cond['inst_id']
            param = cond['parameter']
            code = str(param['code'])
            
            gd = getData(db, code, inst_id)
            try: 
                tag_dict, fst_times, lst_times = gd.get_arc_data()
            except Exception as e:
                print('{', 
                      f'"inst_id" : "{inst_id}", "Result": "Fail, {e}.", "output": -1',
                      '}')
                continue 
            try:
                com_df, isgo = gd.data_organize(tag_dict, fst_times, lst_times)
             except Exception as e:
                 print('{', 
                      f'"inst_id" : "{inst_id}", "Result": "Fail, {e}.", "output": -1',
                      '}')
                 continue 

            if isgo==0:
                print('{', 
                      f'"inst_id" : "{inst_id}", "Result": "Fail, Column Data is Null All.", "output": -1',
                      '}')
                continue
            else:
                run_etl(code, inst_id, com_df, db)
                
    if 'CPR' in list(input_data.keys()):
        input_cond = input_data['CPR']
        for cond in input_cond: 
            inst_id = cond['inst_id']
            param = cond['parameter']
            code = str(param['code'])
            if (param['time_stamp'] ==None):
                print('{', 
                      f'"inst_id" : "{inst_id}", "Result": "Fail, At least one TIMESTAMP is required.", "output": -1',
                    '}')    
            else: time_stamp = pd.Timestamp(param['time_stamp'])
                
            gd= GetData(code, time_stamp, inst_id, db)
            ex_data, isquit = gd.extract_data()
            if isquit ==1: continue
                
            pre_data = gd.preprocess(ex_data)
            df, isquit_ = gd.get_cut_data(pre_data)
            if isquit_ ==1: continue      
            pred_mid, pi = run_cpr(code, pred_mid, pi, db) 
            
            try: output = run_oh(code, pred_mid, pi, db)  # CPR 실행 후, OH 예측 실행
            except Exception as e: continue

if __name__ == '__main__':
    with open(args.input_file, 'r') as f:
        try:
            input_data = json.load(f)
        except json.JSONDecodeError:
            print('{',
                  f'"inst_id" : " ", "Result": "Fail, Not a valid JSON string", "output": -1',
                  '}')   
    main(input_data, dbobj)

            
                 
                
            
        
        
    









  

