# data manipulation libs
import numpy as np
import pandas as pd
# date and time libs
import datetime as dt
from datetime import timedelta
# using a soup lib to scrapp the page
import requests
from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
# lib to pass cookies
import http.cookiejar
from lxml.html import fragment_fromstring
import re
# libs to clean data exported data
from collections import OrderedDict
from decimal import Decimal
from functools import reduce


# classe contendo todas as funções personalizadas
class functions(object):

    def __init__(self) -> None:
        pass

    def format_currency(x):
        return "R${:,.2f}".format(x)

    def format_perc( x):
        return "{}%".format(x)

    def today():
        return dt.date.today()

    def replace_nan(df,column,to_replace,repl):
        df[column] = df[column].replace(to_replace,repl)
    
    def replace_nan_str(df,column:str,to_replace:str,repl:str):
        df[column] = df[column].str.replace(to_replace,repl)

    def change_type(df,column,type):
        df[column] = df[column].astype(type, errors='ignore')

    def options():
        global pd_options
        pd_options = pd.options.mode.chained_assignment = None

    def column_index(df, query_cols):
        cols = df.columns.values
        sidx = np.argsort(cols)
        return sidx[np.searchsorted(cols,query_cols,sorter=sidx)]

    def inicio_mes():
        hoje = dt.datetime.today() 
        inicio_mes_data = hoje - timedelta(hoje.day)+ timedelta(days=1)
        return inicio_mes_data
        
    def round_data(df,columns_to_round):
        df[columns_to_round] = np.round(df[columns_to_round],2)
    
    def centralizar_valor(valor):
        return f'{valor:^10}'

    def merge_all_dfs(dfs,name:str,type_of_merge:str):
        df = reduce(lambda left, right: pd.merge(left,right, on=name, how=type_of_merge), dfs)
        return df
    
    def decimal_point_thousand(df, column):
        df[column] = df[column].apply(lambda x: str(x).replace('.', '', 1))

    
class extract:
    def __init__(self) -> None:
        pass

    def extract_fundamentus_baseline():
        # url de extracao
        base_url = r"https://www.fundamentus.com.br/resultado.php"
        cookie_jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
        opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201'),
                                                      ('Accept', 'text/html, text/plain, text/css, text/sgml, */*;q=0.01')]
        html = opener.open(base_url)
        # decodificar em ISO8859
        html_content = html.read().decode('ISO-8859-1')
        # Fazer o soup
        soup = BeautifulSoup(html_content,'html.parser')
        table = soup.find_all(
        'table'
        )
        tables = table[0]
        thead = tables.find('thead')
        headers_cells = thead.find_all('th')
        headers = []
        for cell in headers_cells:
        headers.append(cell.get_text(strip=True))
        acoes_data = []
        rows = tables.find_all('tr')
        # criar loop e localizar os dados dentro da tag td
        for row in rows[1:]:
        cells = row.find_all('td')
        nome_acao = cells[0].a.get_text(strip=True)  # Obter o texto da tag <a>
        cotacao = cells[1].get_text(strip=True)  # Obter o texto da tag <td>
        p_l = cells[2].get_text(strip=True) 
        p_vp = cells[3].get_text(strip=True)  # Obter o texto da tag <td>
        psr = cells[4].get_text(strip=True) 
        dividend_yield = cells[5].get_text(strip=True) 
        p_ativo = cells[6].get_text(strip=True)  # Obter o texto da tag <td>
        p_cap_giro= cells[7].get_text(strip=True)
        p_ebit = cells[8].get_text(strip=True)
        p_ativ_circ_liq = cells[9].get_text(strip=True)
        ev_ebit = cells[10].get_text(strip=True)
        ev_ebitda = cells[11].get_text(strip=True)
        mrg_ebit = cells[12].get_text(strip=True)
        mrg_liq = cells[13].get_text(strip=True)
        liq_corr = cells[14].get_text(strip=True)
        roic = cells[15].get_text(strip=True)
        roe = cells[16].get_text(strip=True)
        liq_2meses = cells[17].get_text(strip=True)
        patrim_liq = cells[18].get_text(strip=True)
        div_brut_patrimv = cells[19].get_text(strip=True)
        cresc_rec_5av= cells[20].get_text(strip=True)
        # criar um dicionario com os valores encontrados
        acoes_data.append({'papel':nome_acao, 
                            'cotacao':cotacao, 
                            'p_l':p_l, 
                            'p_vp':p_vp, 
                            'psr':psr, 
                            'div_yield':dividend_yield, 
                            'p_ativo':p_ativo,
                                'p_cap_giro':p_cap_giro, 
                                'p_ebit':p_ebit, 
                                'p_ativ_circ_liq':p_ativ_circ_liq,
                                'ev_ebit':ev_ebit,
                                'ev_ebitda':ev_ebitda,
                                'mrg_ebit':mrg_ebit,
                                'mrg_liq':mrg_liq,
                                'liq_corr':liq_corr,
                                'roic':roic,
                                'roe':roe, 
                                'liq_2meses':liq_2meses,
                                'patrim_liq':patrim_liq, 
                                'div_brut_patrim':div_brut_patrimv, 
                                'cresc_rec_5a':cresc_rec_5av
        })

        # %%
        for row in rows[1:]:
            cells = row.find_all('td')
            nome_acao = cells[0].a.get_text(strip=True)  # Obter o texto da tag <a>
            cotacao = cells[1].get_text(strip=True)  # Obter o texto da tag <td>
            p_l = cells[2].get_text(strip=True) 
            p_vp = cells[3].get_text(strip=True)  # Obter o texto da tag <td>
            psr = cells[4].get_text(strip=True) 
            dividend_yield = cells[5].get_text(strip=True) 
            p_ativo = cells[6].get_text(strip=True)  # Obter o texto da tag <td>
            p_cap_giro= cells[7].get_text(strip=True)
            p_ebit = cells[8].get_text(strip=True)
            p_ativ_circ_liq = cells[9].get_text(strip=True)
            ev_ebit = cells[10].get_text(strip=True)
            ev_ebitda = cells[11].get_text(strip=True)
            mrg_ebit = cells[12].get_text(strip=True)
            mrg_liq = cells[13].get_text(strip=True)
            liq_corr = cells[14].get_text(strip=True)
            roic = cells[15].get_text(strip=True)
            roe = cells[16].get_text(strip=True)
            liq_2meses = cells[17].get_text(strip=True)
            patrim_liq = cells[18].get_text(strip=True)
            div_brut_patrimv = cells[19].get_text(strip=True)
            cresc_rec_5av= cells[20].get_text(strip=True)

        # criar um dicionario com os valores encontrados

            acoes_data.append({'papel':nome_acao, 
                            'cotacao':cotacao, 
                            'p_l':p_l, 
                            'p_vp':p_vp, 
                            'psr':psr, 
                            'div_yield':dividend_yield, 
                            'p_ativo':p_ativo,
                                'p_cap_giro':p_cap_giro, 
                                'p_ebit':p_ebit, 
                                'p_ativ_circ_liq':p_ativ_circ_liq,
                                'ev_ebit':ev_ebit,
                                'ev_ebitda':ev_ebitda,
                                'mrg_ebit':mrg_ebit,
                                'mrg_liq':mrg_liq,
                                'liq_corr':liq_corr,
                                'roic':roic,
                                'roe':roe, 
                                'liq_2meses':liq_2meses,
                                'patrim_liq':patrim_liq, 
                                'div_brut_patrim':div_brut_patrimv, 
                                'cresc_rec_5a':cresc_rec_5av})
        # #### 2.3. Renderizar Dataframe

        # %%
        # criar dataframe a partir do dicionario
        stocks_df = pd.DataFrame.from_dict(
            acoes_data
            )
        return stocks_df
    
