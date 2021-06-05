import pandas as pd
import requests
import time

# 재무제표 데이터를 가져와 데이터프레임으로 만드는 함수
def make_fs_dataframe(firm_code):
    fs_url = 'https://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701&gicode=' + firm_code
    fs_page = requests.get(fs_url)
    fs_tables = pd.read_html(fs_page.text)

    temp_df = fs_tables[0]
    temp_df = temp_df.set_index(temp_df.columns[0])
    temp_df = temp_df[temp_df.columns[:4]]
    temp_df = temp_df.loc[['매출액', '영업이익', '당기순이익']]

    temp_df2 = fs_tables[2]
    temp_df2 = temp_df2.set_index(temp_df2.columns[0])
    temp_df2 = temp_df2.loc[['자산', '부채', '자본']]

    temp_df3 = fs_tables[4]
    temp_df3 = temp_df3.set_index(temp_df3.columns[0])
    temp_df3 = temp_df3.loc[['영업활동으로인한현금흐름']]

    fs_df = pd.concat([temp_df, temp_df2, temp_df3])

    return fs_df

# 데이터프레임 형태 바꾸기 코드 함수화
def change_df(firm_code, dataframe):
    for num, col in enumerate(dataframe.columns):
        temp_df = pd.DataFrame({firm_code: dataframe[col]})
        temp_df = temp_df.T
        temp_df.columns = [[col] * len(dataframe), temp_df.columns]
        if num == 0:
            total_df = temp_df
        else:
            total_df = pd.merge(total_df, temp_df, how='outer', left_index=True, right_index=True)

    return total_df

# 재무 비율 데이터프레임을 만드는 함수
def make_fr_dataframe(firm_code):
    fr_url = 'https://comp.fnguide.com/SVO2/asp/SVD_FinanceRatio.asp?pGB=1&cID=&MenuYn=Y&ReportGB=D&NewMenuID=104&stkGb=701&gicode=' + firm_code
    fr_page = requests.get(fr_url)
    fr_tables = pd.read_html(fr_page.text)

    temp_df = fr_tables[0]
    temp_df = temp_df.set_index(temp_df.columns[0])
    temp_df = temp_df.loc[['유동비율(유동자산 / 유동부채) * 100 유동비율계산에 참여한 계정 펼치기',
                           '부채비율(총부채 / 총자본) * 100 부채비율계산에 참여한 계정 펼치기',
                           '영업이익률(영업이익 / 영업수익) * 100 영업이익률계산에 참여한 계정 펼치기',
                           'ROA(당기순이익(연율화) / 총자산(평균)) * 100 ROA계산에 참여한 계정 펼치기',
                           'ROIC(세후영업이익(연율화)/영업투하자본(평균))*100 ROIC계산에 참여한 계정 펼치기']]
    temp_df.index = ['유동비율', '부채비율', '영업이익률', 'ROA', 'ROIC']
    return temp_df

# 투자지표 데이터프레임을 만드는 함수
def make_invest_dataframe(firm_code):
    invest_url = 'https://comp.fnguide.com/SVO2/asp/SVD_Invest.asp?pGB=1&cID=&MenuYn=Y&ReportGB=D&NewMenuID=105&stkGb=701&gicode=' + firm_code
    invest_page = requests.get(invest_url)
    invest_tables = pd.read_html(invest_page.text)
    temp_df = invest_tables[1]

    temp_df = temp_df.set_index(temp_df.columns[0])
    temp_df = temp_df.loc[['PER수정주가(보통주) / 수정EPS PER계산에 참여한 계정 펼치기',
                           'PCR수정주가(보통주) / 수정CFPS PCR계산에 참여한 계정 펼치기',
                           'PSR수정주가(보통주) / 수정SPS PSR계산에 참여한 계정 펼치기',
                           'PBR수정주가(보통주) / 수정BPS PBR계산에 참여한 계정 펼치기',
                           '총현금흐름세후영업이익 + 유무형자산상각비 총현금흐름']]
    temp_df.index = ['PER', 'PCR', 'PSR', 'PBR', '총현금흐름']
    return temp_df

# [코드 3.27] 모든 종목에 대해서 재무제표 데이터 가져오기 (CH3. 데이터 수집하기.ipynb)
path = r'C:\Users\Yoon\Documents\stock_data\data.xls'
code_data = pd.read_excel(path)
code_data = code_data[['종목코드', '기업명']]

def make_code(x):
    x = str(x)
    return 'A' + '0' * (6-len(x)) + x

code_data['종목코드'] = code_data['종목코드'].apply(make_code)
# 모든 종목에 대해서 재무제표 데이터 가져오기
for num, code in enumerate(code_data['종목코드']):
    try:
        print(num, code)
        time.sleep(1)
        try:
            fs_df = make_fs_dataframe(code)
        except requests.exceptions.Timeout:
            time.sleep(60)
            fs_df = make_fs_dataframe(code)
        fs_df_changed = change_df(code, fs_df)
        if num == 0 :
            total_fs = fs_df_changed
        else:
            total_fs = pd.concat([total_fs, fs_df_changed])
    except ValueError:
        continue
    except KeyError:
        continue

# [코드 3.28] 재무제표 데이터 엑셀로 저장 (CH3. 데이터 수집하기.ipynb)
total_fs.to_excel(r'C:\Users\Yoon\Documents\stock_data\재무제표데이터.xlsx')

# 모든 종목에 대해서 재무비율 데이터 가져오기
for num, code in enumerate(code_data['종목코드']):
    try:
        print(num, code)
        time.sleep(1)
        try:
            fr_df = make_fr_dataframe(code)
        except requests.exceptions.Timeout:
            time.sleep(60)
            fr_df = make_fr_dataframe(code)
        fr_df_changed = change_df(code, fr_df)
        if num == 0 :
            total_fr = fr_df_changed
        else:
            total_fr = pd.concat([total_fr, fr_df_changed])
    except ValueError:
        continue
    except KeyError:
        continue

# 재무비율 데이터 엑셀로 저장
total_fr.to_excel(r'C:\Users\Yoon\Documents\stock_data\재무비율데이터.xlsx')

# 모든 종목에 대해서 투자지표 데이터 가져오기
for num, code in enumerate(code_data['종목코드'][313:]):
    try:
        print(num, code)
        time.sleep(1)
        try:
            invest_df = make_invest_dataframe(code)
        except requests.exceptions.Timeout:
            time.sleep(60)
            invest_df = make_invest_dataframe(code)
        invest_df_changed = change_df(code, invest_df)
        if num == 0 :
            total_invest = invest_df_changed
        else:
            total_invest = pd.concat([total_invest, invest_df_changed])
    except ValueError:
        continue
    except KeyError:
        continue

# 투자지표 데이터 엑셀로 저장
total_invest.to_excel(r'C:\Users\Yoon\Documents\stock_data\투자지표데이터.xlsx')