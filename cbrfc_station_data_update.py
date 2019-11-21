import requests
import pandas as pd

response = requests.get('https://www.cbrfc.noaa.gov/gmap/list/list.php?search=&point=forecast&plot=&sort=riverhis&psv=on&type=river&basin=0&subbasin=&espqpf=0&espdist=emperical')

response.request.url
'https://www.cbrfc.noaa.gov/gmap/list/list.php?search=&point=forecast&plot=&sort=riverhis&psv=on&type=river&basin=0&subbasin=&espqpf=0&espdist=emperical'

df = pd.read_csv(response.url)

column_list = ['NWS_ID', 'River', 'Location', 'Forecast_Condition', 'Point_Type', 'Observed_DayTime', 'Latest_Flow', 'Latest_Stage', 'Flood_Stage', 'Bankfull_Stage', 'HUC', 'State', 'HSA', 'Elevation', 'Forecast_Group', 'Segment', 'DeleteMe', 'DeleteMe_2']

df.columns = column_list

df = df.drop(['DeleteMe', 'DeleteMe_2'], axis=1)

# >>> print(df.loc[0])
# NWS_ID                                      tnra3
# River                                   Tonto Ck
# Location               Roosevelt  Nr  Gun Ck  Abv
# Forecast_Condition                              4
# Point_Type                                      1
# Observed_DayTime                            21.15
# Latest_Flow                                   429
# Latest_Stage                                 3.68
# Flood_Stage                                  16.0
# Bankfull_Stage                               16.0
# HUC                                   1.50601e+07
# State                                          AZ
# HSA                                           PSR
# Elevation                                    2523
# Forecast_Group                               SALT
# Segment                                        11
# Name: 0, dtype: object
# >>> print(df.loc[0]['NWS_ID'])
# tnra3
# >>> print(df.loc[0]['Latest_Flow'])
# 429
# >>>
