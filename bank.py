import datetime
import pandas as pd
import requests
import json
import time
import warnings
warnings.filterwarnings('ignore')
import sqlalchemy
import math

BOT_TOKEN = ""
BOT_ID = ""
url_oc = "https://www.nseindia.com/option-chain"
url_indices = "https://www.nseindia.com/api/allIndices"
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s",
                    datefmt='%m-%d %H:%M',
                    filename=f'log_BANK_{datetime.datetime.now().date()}')



def telegram_bot_sendtext(bot_message, bot_id=BOT_ID, bot_token=BOT_TOKEN):
    bot_token = bot_token
    bot_chatID = bot_id
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    return response.json()

def session():
    sess = requests.Session()
    return sess

def get_bn_option_chain():
    sess = session()
    url_bnf = 'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
        'accept-language': 'en,gu;q=0.9,hi;q=0.8',
        'accept-encoding': 'gzip, deflate, br'}
    try:
        response =sess.get(url_bnf, headers=headers)
        data = json.loads(response.text)
        
        currExpiryDate = data["records"]["expiryDates"][0]
        l1 = data['records']['data']
        df =pd.DataFrame(l1)
        df = df[df.expiryDate == currExpiryDate]
        pe_list = list(df.PE)
        ce_list = list(df.CE)
        pe_list = list(filter(lambda x: type(x) == dict, pe_list))
        ce_list = list(filter(lambda x: type(x) == dict, ce_list))
        col = ['strikePrice', 'openInterest','changeinOpenInterest']
        pe = pd.DataFrame(pe_list,columns=col)
        ce = pd.DataFrame(ce_list,columns=col)
        opt_chain = pd.merge(ce,pe,on='strikePrice',suffixes=('_PE','_CE'))
        opt_chain = opt_chain.rename(columns={'openInterest_CE':'CE_OI','openInterest_PE':'PE_OI',
                                  'changeinOpenInterest_PE':'CHCE_OI','changeinOpenInterest_CE':'CHPE_OI'})
        opt_chain['date'] = datetime.datetime.now()
        return opt_chain
    except:
        time.sleep(5)
        print(datetime.datetime.now())
        get_bn_option_chain()


def transform_option_chain(engine,a,b):
    strike = get_strike()
    print(strike)
    bnf_strikes = list(range(strike-4*100,strike+4*100,100))
    try:
        df =get_bn_option_chain()
        df['CE_SUM'] = df.CE_OI.sum()
        df['PE_SUM'] = df.PE_OI.sum()
        strike_list=df.strikePrice.unique()
        for item in strike_list:
            df1 = df.query(f'strikePrice=={item}')
            symbol = f'{item}'

            df1.to_sql(symbol, engine, index=False, if_exists='append')
        min = datetime.datetime.now().minute
        hr = datetime.datetime.now().hour
        df.to_sql(f'BANK_OI_{hr}_{min}',engine,index=False,if_exists='replace')
        df2 = df[df.strikePrice.isin(bnf_strikes)]
        a1 = df2.CE_OI.sum()
        b1 = df2.PE_OI.sum()

        ce = (a1-a)/a1 *100
        pe = (b1-b)/b1 * 100
        if ce<0 and abs(ce)>18:
            telegram_bot_sendtext(f'B-SC in CALL {round(ce,2)} %')
        if pe < 0 and abs(pe) > 18:
            telegram_bot_sendtext(f'B-SC in PUT {round(pe,2)} %')
        logging.info(f'CE -- {a1}, PE --{b1} '
              f'%CE -- {ce}, %PE--{pe} {datetime.datetime.now()}')
        print(f'CE -- {a1}, PE --{b1} '
              f'%CE -- {ce}, %PE--{pe} {datetime.datetime.now()}')
        a = a1
        b = b1
        return a,b
    except BaseException as e:
        logging.info(e.args)
        transform_option_chain(engine,a,b)

def crate_db_engine(name):
    engine = sqlalchemy.create_engine('sqlite:///'+ name + '.db' )
    return engine

def get_strike():
    try:
        from nsepython import nse_quote_ltp
        ltp = nse_quote_ltp('BANKNIFTY')
        strike = int(math.ceil(float(ltp)/100)*100)
        return strike
    except BaseException as e:
        logging.info(f'{e.args}')
        get_strike()
        

def main():
    engine = crate_db_engine(f'BANKNIFTY_{datetime.datetime.today().date()}')
    a,b =0,0
    count = 1
    while datetime.time(9, 10) >datetime.datetime.now().time():
        pass
    while datetime.time(9, 10) < datetime.datetime.now().time() < datetime.time(15, 31):
        try:
            if datetime.datetime.now().minute % 3 == 0:
                if count ==1:
                    a,b = transform_option_chain(engine,a=0,b=0)
                    time.sleep(60)
                    count +=1
                else:
                    a,b = transform_option_chain(engine,a,b)
                    time.sleep(60)
                    count +=1
        except BaseException as e:
            logging.info(f'{e.args}')
            pass

if __name__ == '__main__':
    main()


