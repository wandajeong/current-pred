import numpy as np 
import pandas as pd

def gen_hsum(com_df, code, inst_id):
  '''
  After Postprocessing, Data Hourly Summation
  '''
  hsum_df = pd.DataFrame()
  try:
    com_df['PVal'] = np.where(com_df['PValve'] =='OPEN', 0, 1)
    com_df['PAMP'] = np.where(com_df['PValve'] =='OPEN', com_df['전류'], 0)
    # 가장 첫 번째 정각인 시간
    sharp_time = com_df[(com_df.index.minute==0) & (com_df.index.second==0)].first_valid_index()
    hsum_df['AMP_MAX'] = com_df['전류'].resampe('1H', origin = sharp_time).max()
    hsum_df['AMP_MIN'] = com_df['전류'].resampe('1H', origin = sharp_time).min()
    hsum_df['AMP_AVG'] = com_df['전류'].resampe('1H', origin = sharp_time).mean()
    
    hsum_df['PAMP_MAX'] = com_df['PAMP'].resampe('1H', origin = sharp_time).max()
    hsum_df['PAMP_MIN'] = com_df['PAMP'].resampe('1H', origin = sharp_time).min()
    hsum_df['PAMP_AVG'] = com_df['PAMP'].resampe('1H', origin = sharp_time).mean()  
    
    hsum_df['AMP_SUM'] = com_df['AmpSumm'].resampe('1H', origin = sharp_time).max()
    hsum_df['NON_PROCESS'] = com_df['PVal'].resampe('1H', origin = sharp_time).sum()

    hsum_df = (
      hsum_df
      .dropna()
      .reset_index()
      .insert(0, column='CODE', value = code)
    )
    sum_zero_idx = hsum_df.iloc[:, 2:-2].sum(axis=1) ==0
    hsum_df.loc[sum_zero_idx, 'NON_PROCESS'] = 3600
  except Exception as e:
    print('{', f'"inst_id" : "{inst_id}", "Result": "Fail, {e}", "output": -1','}')   
    
  return hsum_df
    
