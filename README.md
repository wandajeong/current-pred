# 공정 데이터 가공 및 누적전류 예측 API 

실시간 공정 데이터를 가공하여 Hourly Summury 하고, 이를 기반으로 미래 30일의 누적전류 profile을 예측하여 결과를 database에 insert 하는 API 

### 누적전류 예측 모델 비교 
- Prophet
  - Facebook에서 개발한 시계열 예측 도구
  - 시계열을 세 가지 주요 구성 요소(트렌드, 계절성, 휴일 효과)로 분해하여 모델링
  - 큰 트렌드나 계절 패턴이 뚜렷한 데이터에 적합하고, 특별한 날짜(휴일 등)의 영향을 모델에 추가하기 쉽다 
- ETS(지수평활법)
  - 오류(Error), 추세(Trend), 계절성(Seasonal) 요소를 사용하여 시계열 데이터를 모델링
  - 단기적인 정확도에 초점을 맞추고, 작은 데이터셋에 강력함 

![](https://velog.velcdn.com/images/hajeongjj/post/a9ba2057-38b1-48af-ac21-714d5790ca87/image.png)

시계열 예측을 위해 위 두 모델을 대상으로 데이터셋에 적합해본 결과, 

전반적으로 Prophet이 ETS보다 더 예측 성능이 우수한 것으로 보인다. 

실제 예측 결과를 시각화해보았다.  
![](https://velog.velcdn.com/images/hajeongjj/post/7444aef2-1b1d-4b2d-ad2a-5a9a19612fab/image.png)
![](https://velog.velcdn.com/images/hajeongjj/post/925a5b3d-5900-4f16-a1af-3e321678a8b2/image.png)

안정적인 시계열 데이터에서는 두 모델의 예측이 큰 차이가 없지만, 

특정 변화에 대한 트렌드를 ETS가 더 민감하게 받아들이고 있는 것으로 보인다. 

파라미터 튜닝을 적용하면 결과가 조금 달라지겠지만, 현재 데이터에서는 전반적으로 Prophet이 적합해 보인다. 
