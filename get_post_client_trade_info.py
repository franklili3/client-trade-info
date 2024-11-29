# -*- coding: utf-8 -*-
import ccxt
import logging
import requests, json
import pandas as pd
#import datetime
from empyrical import max_drawdown, cum_returns, annual_return, annual_volatility, sharpe_ratio
import os
import time
#import datetime
from requests.exceptions import Timeout

def main():


    logging.basicConfig(level=logging.INFO)#DEBUG

    logger = logging.getLogger(__name__)
    username = os.environ.get('admin_username')
    #Log('username: ', username)
    password = os.environ.get('admin_password')
    client_credential = os.environ.get('client_credential')
    #logger.debug("client_credential: ", client_credential)
    client_credential_dict = json.loads(client_credential)
    #logger.debug("client_credential_dict: ", client_credential_dict)
    for item in client_credential_dict['data']:
        client_id = item['client_id']
        #test_apiKey = os.environ.get('test_apiKey')
        #logger.debug("test_apiKey: ", test_apiKey)
        #test_secret = os.environ.get('test_secret')
        
        apiKey = item['apiKey']
        logger.debug("apiKey: {}".format(apiKey))
        secret = item['secret']
        
        exchange = ccxt.binance({
        'enableRateLimit': True,
        #'timeout': 30000,
        #'proxies': {
        #    'http': 'http://127.0.0.1:10818',
        #    'https': 'http://127.0.0.1:10818',
        #},
        })#config
        #exchange.set_sandbox_mode(True)  # enable sandbox mode
        #exchange.apiKey = test_apiKey
        #exchange.secret = test_secret
        exchange.apiKey = apiKey
        exchange.secret = secret
        markets = retry_request(lambda: exchange.load_markets(), logger)
        #BTCUSDT = exchange.markets['BTC/USDT']
        symbol = 'BTC/USDT'
        if exchange.has['fetchOHLCV']:
            time.sleep (exchange.rateLimit / 1000) # time.sleep wants seconds
            ohlcv = retry_request(lambda: exchange.fetch_ohlcv (symbol, '1d'), logger)
            close = ohlcv[-1][4]
            timestamp = ohlcv[-1][0]
            date_time = pd.to_datetime(timestamp, unit='ms')
            date_time_str = date_time.strftime('%Y-%m-%d %H:%M:%S')

        balance = retry_request(exchange.fetchBalance, logger)
        total_cash_balance = balance['USDT']['total']
        total_asset_amount = balance['BTC']['total']
        if total_asset_amount > 0:
            symbol = "BTC_USDT"
        else:
            symbol = ''
        position_value = total_asset_amount * close
        net_asset_value = total_cash_balance + position_value
        home_url = 'https://pocketbase-5umc.onrender.com'#'http://127.0.0.1:8090/' 
        auth_path = '/api/admins/auth-with-password'
        auth_url = home_url + auth_path

        # json.dumps 将python数据结构转换为JSON
        data1 = json.dumps({"identity": username, "password": password})
        # Content-Type 请求的HTTP内容类型 application/json 将数据已json形式发给服务器
        header1 = {"Content-Type": "application/json"}
        response1 = retry_request(lambda: requests.post(auth_url, data=data1, headers=header1), logger)
        response1_json = response1.json()
        #print('response1_json: ', response1_json)

        # html.json JSON 响应内容，提取token值
        if response1_json['token']:
            token = response1_json['token']

            # 使用已经登录获取到的token 发送一个post请求
            post_path = '/api/collections/clients_trade_account/records'

            post_url = home_url + post_path
            header2 = {
                "Content-Type": "application/json",
                "Authorization": token
            }
            # 使用已经登录获取到的token 发送一个get请求
            get_path = '/api/collections/clients_trade_account/records'
            query_daily_return = "?filter(client_id=" + client_id + ")&&sort=date&&fields=date,net_asset_value,draw_down,daily_return"#&&page=50&&perPage=100&&sort=date&&skipTotal=1response1_json
            get_url = home_url + get_path + query_daily_return
            
            response2 = retry_request(lambda: requests.get(get_url, headers=header2), logger)
            response2_json = response2.json()
            response2_str = str(response2_json)
            #app.logger.debug('response2_str: {}'.format(response2_str))
            total_pages = response2_json['totalPages']
            total_items = response2_json['totalItems']
            getted_datas = {'date': [], 'net_asset_value': [], 'daily_return': []} 
            if total_items == 0:
                data2 = {'client_id': client_id, 'date': date_time_str, 'net_asset_value': net_asset_value, 'position_amount': total_asset_amount,
                    'symbol': symbol, 'position_value': position_value, 'cash_balance': total_cash_balance}
                data2_json = json.dumps(data2)
            elif total_items > 0:
                if total_pages == 1:
                    for item in response2_json['items']:
                        getted_datas['date'].append(item['date'])
                        getted_datas['net_asset_value'].append(item['net_asset_value'])
                        getted_datas['daily_return'].append(item['daily_return'])
                elif total_pages > 1:
                    for i in range(1, total_pages + 1):
                        query_daily_return1 = "?filter(client_id=" + client_id + ")&&sort=date&&fields=date,daily_return,net_asset_value&&page=" + str(i)#50&&perPage=100&&sort=date&&skipTotal=1response1_json
                        get_url1 = home_url + get_path + query_daily_return1
                        response3 = retry_request(lambda: requests.get(get_url1, headers=header2), logger)
                        response3_json = response3.json()
                        response3_str = str(response3_json)
                        print('response3_str[0:100]: ', response3_str[0:100])
                        for item in response3_json['items']:
                            getted_datas['date'].append(item['date'])
                            getted_datas['net_asset_value'].append(item['net_asset_value'])
                            getted_datas['daily_return'].append(item['daily_return']) 
                getted_datas_df = pd.DataFrame(getted_datas)
                if len(getted_datas_df['net_asset_value']) == 0:
                    last_daily_return1 = net_asset_value / getted_datas_df['net_asset_value'] - 1
                else:
                    last_daily_return1 = net_asset_value / getted_datas_df['net_asset_value'].iloc[-1] - 1
                total_profit = net_asset_value - getted_datas_df['net_asset_value'][0]
                getted_datas_df.loc[len(getted_datas_df)] = [date_time_str, net_asset_value, last_daily_return1]
                returns = getted_datas_df['daily_return']
                max_drawdown1 = max_drawdown(returns)
                cum_returns1 = cum_returns(returns)
                last_cum_return1 = cum_returns1.iloc[-1]
                annual_return1 = annual_return(returns, period='daily', annualization=365)
                annual_volatility1 = annual_volatility(returns, period='daily', annualization=365)
                sharpe_ratio1 = sharpe_ratio(returns, period='daily', annualization=365, risk_free=0.02)
                data2 = {'client_id': client_id, 'date': date_time_str, 'net_asset_value': net_asset_value, 'position_amount': total_asset_amount,
                        'symbol': symbol, 'position_value': position_value, 'cash_balance': total_cash_balance, 'daily_return': last_daily_return1,
                        'max_drawdown': max_drawdown1, 'total_return': last_cum_return1, 'annualized_return': annual_return1, 'annualized_volatility': annual_volatility1,
                        'annualized_sharpe': sharpe_ratio1}
                data2_json = json.dumps(data2)
                #print('data: ', data)
            response4 = retry_request(lambda: requests.post(post_url, headers=header2, data=data2_json), logger)
            response4_json = response4.json()
            response4_str = str(response4_json)
            if response4.status_code == 200:
                logger.info('client_id : {}, post data success.'.format(client_id))
            else:
                logger.info('response4_str: ', response4_str)

    return

def retry_request(request_func, logger, retries=2, delay=1):
    for attempt in range(retries):
        try:
            return request_func()
        except Timeout as e:
            logger.warning(f"Request timed out: {e}. Retrying {attempt + 1}/{retries}...")
            time.sleep(delay)
    raise Exception("Request failed after retries")

if __name__ == '__main__':
    main()