import os, requests
from datetime import datetime, timezone

API_KEY = '4ku7YB5AIpIL_IRvfIiI4xZV09EoLGD6'        #Prod API Key
#API_KEY = 'gNUdx8Rrob9OtDQSGK9EBX7K179qpNjQ'  


#url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/5/minute/2023-01-09/2023-02-10?adjusted=true&sort=asc&apiKey={API_KEY}"


def get_intra_day(symbol, day):
    #date/day format is YYYY-MM-DD
    thirty_one = ['01','03','05','07','08','10','12']
    thirty = ['04','06','09','11']
    def calculate_next_day(day):
        
        full_date = str(day).split("-")
        if int(full_date[2]) <= 31:
            if full_date[2] == '31':
                if full_date[1] in thirty_one:
                    next_day = full_date[0] +"-"+ str(int(full_date[1]) + 1) + "-01"
                    print(f"From 31: {next_day}")
                    return next_day
                else:
                    print("Invalid date: There aren't 31 days in that month. ")
                
            elif full_date[2] == '30':
                if full_date[1] in thirty:
                    next_day = full_date[0] +"-"+ str(int(full_date[1]) + 1) + "-01"
                    print(f"From 30: {next_day}")
                    return next_day
                else:
                    print("Invalid date: There aren't 30 days in that month. ")
                
            else:
                next_day = full_date[0] +"-"+ full_date[1] +"-"+ str(int(full_date[2]) + 1)
                print(f"From else: {next_day}")
                return next_day
        else:
            print("Enter a valid date. ")
                
        
        return
        
    next_day = calculate_next_day(day)   
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/5/minute/{day}/{next_day}?adjusted=true&sort=asc&apiKey={API_KEY}"
    #url = f"https://api.polygon.io//v2/aggs/ticker/MSFT/range/5/minute/2024-05-31/2024-6-01" # Test url
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json().get('results', [])
        return data
    
#if __name__ == __main__():
#day = input("Enter the date to view chart: ")
#get_intra_day(day) 