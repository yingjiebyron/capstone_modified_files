import pandas as pd
from yahoofinancials import YahooFinancials
import datetime
import json
from dateutil.relativedelta import relativedelta

def stock_price(companies,db, start=None,end=None,symbols=None):
    end = str(datetime.date.today())
    start = str(datetime.date.today() - relativedelta(day=1))
    db_activity = db.Activity
    stock_collection = db["Stock Prices"]
    
    if not symbols:
        symbols = [i["tickerID"] for i in companies if i["tickerID"] !=""]
    companies_financials = YahooFinancials(symbols)

    print("Loading stock data from Yahoo Finance...")
    companies_data = companies_financials.get_historical_price_data(start_date=start, 
                                                    end_date=end, 
                                                    time_interval='daily')
    print("successfully retrieved stock data for listed companies!")
    master_data = pd.DataFrame(columns= ['company','tickerID','high','low','open','close','volume','adjclose'])
    missing = []
    for symbol in symbols:
        #print("formatting data for tickerID ", symbol)
        if companies_data[symbol]: #not none type
            if "prices" in companies_data[symbol].keys():
                company_df = pd.DataFrame(companies_data[symbol]['prices'])
                if "date" in company_df.columns:
                    company_df = company_df.drop('date', axis=1).set_index('formatted_date')
                company_df = company_df.assign(tickerID=lambda x: symbol)
                company_df = company_df.assign(company=lambda x: db["Company"].find_one({"tickerID":symbol})["company"])
                #check if corresponding company price has intraday surge or plunge
                if len(company_df)>1: #if yesterday stock is available
                    today = datetime.datetime.today().strftime('%Y-%m-%d')
                    ytd_price = float(company_df.loc[company_df.index[-2],"close"])
                    today_price = float(company_df.loc[company_df.index[-1],"close"])
                    percentChange = (today_price - ytd_price)/ytd_price*100
                    if abs(percentChange)>=10:
                        gfcid = [i["gfcid"] for i in companies if i["tickerID"] ==symbol][0]
                        db_activity.insert_one({"gfcid":gfcid, "date":today,"type":"stock price","count":1})

                master_data = pd.concat([master_data,company_df.tail(1)],sort=False)
            else:
                print("did not find data for tickerID {}".format(symbol))
                missing.append(symbol)
        else:
            print("did not find data for tickerID {}".format(symbol))
            missing.append(symbol)
    master_data = master_data.rename(columns={"adjclose": "adjClose"})
    master_data["date"] = master_data.index
    master_data["date"] = pd.to_datetime(master_data['date'], format ="%Y-%m-%d")#convert to pd datetime type so that it will be recognized as unix in json
    master_df_document = master_data.reset_index(drop=1)
    document = json.loads(master_df_document.to_json(orient = "records"))
    print("successfully loaded stock prices data")
    try:
        stock_collection.insert_many(document)

    except Exception as e:
        print("MongoDB insertion failed with: ",e)
    return missing