# KOSPI Version
import pandas as pd
import numpy as np
import requests
import re
from bs4 import BeautifulSoup

req = requests.get('https://finance.naver.com/sise/sise_market_sum.nhn?sosok=0')
html = req.text
soup = BeautifulSoup(html, features="html.parser")

# finding the last page
last_page = soup.find('td', {'class': 'pgRR'})
last_number = re.findall(r'\d+', last_page.a.get('href'))[1]

db = pd.DataFrame()
# page iteration
for i in range(1, int(last_number) + 1):
    # KOSPI
    req = requests.get('https://finance.naver.com/sise/sise_market_sum.nhn?&page=' + str(i))
    html = req.text
    soup = BeautifulSoup(html, features="html.parser")

    # find company list for each page
    company_list = soup.find_all('a', {'class': 'tltle'})

    # company iteration for each page
    for company in company_list:
        company_code = re.search('\d+', company.get('href')).group()
        company_name = company.text

        # extracting fianacial statement
        company_url = 'https://finance.naver.com/item/main.nhn?code=' + str(company_code)
        df = pd.read_html(company_url, encoding='euc-kr')[3]

        # ETF exception
        if df.columns[0] == '구성종목(구성자산)' or df.columns[0] == 0 or df.iloc[:, 1:].isnull().all(axis=None):
            continue

        # dataframe arrange
        df.columns = df.columns.droplevel([0, 2])
        df.index = df['주요재무정보']
        df_arrange = df.iloc[:, 1:]

        data = pd.DataFrame(df_arrange.stack(dropna=False))
        #if company_name == company_list[0]:
        index = data.index.to_flat_index()
        data.index = index

        df2 = pd.DataFrame([company_code, company_name], index=['code', 'name'])
        df2 = df2.append(data).T

        print(company_name)
        if company_name == 'SK아이이테크놀로지':
            continue
        if company_name == '하이브':
            df2.to_csv("hive.csv", index=False, encoding='utf-8-sig')
            continue
        if company_name == '우리금융지주':
            df2.to_csv("woori.csv", index=False, encoding='utf-8-sig')
            continue
        if company_name == '두산퓨얼셀':
            df2.to_csv("doosan.csv", index=False, encoding='utf-8-sig')
            continue
        if company_name == 'DL이앤씨':
            df2.to_csv("DL이앤씨.csv", index=False, encoding='utf-8-sig')
            continue
        if company_name == '프레스티지바이오파마':
            df2.to_csv("프레스티지바이오파마.csv", index=False, encoding='utf-8-sig')
            continue
        if company_name == '솔루스첨단소재':
            df2.to_csv("솔루스첨단소재.csv", index=False, encoding='utf-8-sig')
            continue
        if company_name == '솔루엠':
            df2.to_csv("솔루엠.csv", index=False, encoding='utf-8-sig')
            continue
        db = db.append(df2, ignore_index=True)

# save to csv file
db.to_csv("KOSPI.csv", index=False, encoding='utf-8-sig')