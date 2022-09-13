from operator import index
import string
from time import strftime
from tokenize import String
from turtle import clear
from unicodedata import name
from urllib import response
from numpy import append
from pytz import HOUR
import requests
import pandas as pd
import json
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv, find_dotenv
import os
import pathlib as Path
import glob
from datetime import datetime, timezone, timedelta as dt

load_dotenv()

session = requests.Session()
url = 'http://api.olhovivo.sptrans.com.br/v2.1/'

# TODO - TRANSLATE ALL THE CODE TO ENGLISH
# TODO - INSERT DOCSTRING IN ALL FUNCTIONS
# TODO - INSERT TYPED VARIABLES IN FUNCTION PARAMETERS

def auth() -> str:

     '''
     Esta função é responsavel por autenticar nossa aplicação junto a SPTRANS utilizando um token de usuário
     A função deve retornar um [bool], sendo TRUE para autenticado ou FALSE para não autenticado
     '''

     TOKEN_API_OLHOVIVO = os.getenv("TOKEN_API_OLHOVIVO")

     endpoint = 'Login/Autenticar?token={}'.format(TOKEN_API_OLHOVIVO)
     response = session.post(url + endpoint)

     return print(response.text)


def _get(endpoint : str) -> json:

     '''
     Essa função é responsavel por trazer os dados da API Olho Vivo passando a URL da API mais a rota (endpoint)
     A função deve retornar um objeto [Json] com o conteúdo do endpoint solicitado.
     '''

     response = session.get(url + endpoint)
     data = response.json()
     return data


def _remove_duplicates(list_df : list) -> list:
          return list(set(list_df))


def write_json_files(endpoint : str, df : pd.DataFrame, path : str) -> None:

     for item in df:

          data = _get(endpoint.format(item))
     
          with open(path.format(item), 'w') as f:
               
               json.dump(data, f)


def read_json_files(path : str) -> pd.DataFrame:

     paths = glob.glob(path)
     df = pd.DataFrame()

     for path in paths:

          df_aux = pd.read_json(path)
          df = pd.concat(([df, df_aux]), ignore_index = False)
     
     return df


def get_lines() -> pd.DataFrame:
     
     '''
     [int]cl Código identificador da linha. Este é um código identificador único de cada linha do sistema (por sentido de operação)
     [bool]lc Indica se uma linha opera no modo circular (sem um terminal secundário)
     [string]lt Informa a primeira parte do letreiro numérico da linha
     [int]tl Informa a segunda parte do letreiro numérico da linha, que indica se a linha opera nos modos:
     BASE (10), ATENDIMENTO (21, 23, 32, 41)
     [int]sl Informa o sentido ao qual a linha atende, onde 1 significa Terminal Principal para Terminal Secundário e 2 para Terminal Secundário para Terminal Principal
     [str]tp Informa o letreiro descritivo da linha no sentido Terminal Principal para Terminal Secundário
     [str]ts Informa o letreiro descritivo da linha no sentido Terminal Secundário para Terminal Principal
     '''

     df_bus_position = get_bus_position('')
     lines_list_code = df_bus_position['l.c'].to_list()
     lines_list_code = _remove_duplicates(lines_list_code)
     
     write_json_files('/Linha/Buscar?termosBusca={}', lines_list_code, 'C:\\repos\\olhovivo_sptrans\\data\\tmp\\lines\\lines{}.json')

     df_lines = read_json_files('C:\\repos\\olhovivo_sptrans\\data\\tmp\\lines\\*.json')

     return df_lines


def get_stops(stop_id : str) -> pd.DataFrame:

     '''
     [int]cp Código identificador da parada
     [string]np Nome da parada
     [string]ed Endereço de localização da parada
     [double]py Informação de latitude da localização da parada
     [double]px Informação de longitude da localização da parada
     '''

     stops = _get('/Parada/Buscar?termosBusca={}'.format(stop_id))
     df_stops = pd.DataFrame(stops)
     df_stops = pd.json_normalize(json.loads(df_stops.to_json(orient='records')))
     
     return df_stops


def get_bus_runner_stops() -> pd.DataFrame:

     '''
     [int]cp Código identificador da parada
     [string]np Nome da parada
     [string]ed Endereço de localização da parada
     [double]py Informação de latitude da localização da parada
     [double]px Informação de longitude da localização da parada
     '''
     path = 'C:\\repos\\olhovivo_sptrans\\data\\tmp\\bus_runners_stops\\bus_runner_stop{}.json'
     endpoint = '/Parada/BuscarParadasPorCorredor?codigoCorredor={}'
     runner_stops = get_bus_runner()
     runner_stops = runner_stops['cc']

     write_json_files(endpoint, runner_stops, path)
     df_runners_stops = read_json_files('C:\\repos\\olhovivo_sptrans\\data\\tmp\\bus_runners_stops\\*.json')

     return df_runners_stops


def get_bus_runner() -> pd.DataFrame:

     '''
     [int]cc Código identificador da corredor. Este é um código identificador único de cada corredor inteligente do sistema
     [string]nc Nome do corredor
     '''
     bus_runner = _get('/Corredor')
     df_bus_runner = pd.DataFrame(bus_runner)

     return df_bus_runner


def get_company() -> pd.DataFrame:

     '''
     [string]hr Horário de referência da geração das informações
     [{}]e Relação de empresas por área de operação
          [int]a Código da área de operação 
     [{}]e Relação de empresas
          [int]a Código da área de operação
          [int]c Código de referência da empresa
          [string]n Nome da empresa
     '''
     company = _get('/Empresa')

     df_company = pd.DataFrame(company)
     df_company = pd.json_normalize(json.loads(df_company.to_json(orient='records'))).explode('e.e')
     df_company = pd.json_normalize(json.loads(df_company.to_json(orient='records')))

     return df_company


def get_bus_position(line_id : int) -> pd.DataFrame:

     '''
     [string]hr Horário de referência da geração das informações
     [{}]l Relação de linhas localizadas onde:
          [string]c Letreiro completo
          [int]cl Código identificador da linha
          [int]sl Sentido de operação onde 1 significa de Terminal Principal para Terminal Secundário e 2 de Terminal Secundário para Terminal Principal
          [string]lt0 Letreiro de destino da linha
          [string]lt1 Letreiro de origem da linha
          [int]qv Quantidade de veículos localizados
     [{}]vs Relação de veículos localizados, onde: 
          [int]p Prefixo do veículo
          [bool]a Indica se o veículo é (true) ou não (false) acessível para pessoas com deficiência
          [string]ta Indica o horário universal (UTC) em que a localização foi capturada. Essa informação está no padrão ISO 8601
          [double]py Informação de latitude da localização do veículo
          [double]px Informação de longitude da localização do veículo
     '''

     bus_position = _get('Posicao?codigoLinha={}'.format(line_id))

     df_bus_position = pd.DataFrame(bus_position)
     df_bus_position = pd.json_normalize(json.loads(df_bus_position.to_json(orient='records'))).explode('l.vs')
     df_bus_position = pd.json_normalize(json.loads(df_bus_position.to_json(orient='records')))
     df_bus_position['ano_part'] = pd.to_datetime("today").strftime("%Y")
     df_bus_position['mes_part'] = pd.to_datetime("today").strftime("%m")
     df_bus_position['dia_part'] = pd.to_datetime("today").strftime("%d")
 
     df_bus_position.to_parquet('C:\\repos\\olhovivo_sptrans\\data/bus_position', partition_cols=['ano_part', 'mes_part', 'dia_part'])
     #df_bus_position.to_parquet('/home/ubuntu/repos/olhovivo_sptrans/data/bus_position', partition_cols=['ano_part', 'mes_part', 'dia_part'])

     return df_bus_position


def get_garage() -> pd.DataFrame:

     '''
     [string]hr Horário de referência da geração das informações 
     [{}]l Relação de linhas localizadas onde: 
          [string]c Letreiro completo 
          [int]cl Código identificador da linha 
          [int]sl Sentido de operação onde 1 significa de Terminal Principal para Terminal Secundário e 2 de Terminal Secundário para Terminal Principal
          [string]lt0 Letreiro de destino da linha 
          [string]lt1 Letreiro de origem da linha 
          [int]qv Quantidade de veículos localizados 
     [{}]vs Relação de veículos localizados, onde: 
          [int]p Prefixo do veículo [bool]a Indica se o veículo é (true) ou não (false) acessível para pessoas com deficiência 
          [string]ta Indica o horário universal (UTC) em que a localização foi capturada. Essa informação está no padrão ISO 8601 
          [double]py Informação de latitude da localização do veículo
          [double]px Informação de longitude da localização do veículo
     '''
# TODO - ANALYZE WHY WE CAN'T USE THE READ_JSON_FILES FUNCTION AND IMPLEMENT A SOLUTION
     lista = get_company()
     list_cod_company = lista['e.e.c'].to_list()
     list_cod_company = _remove_duplicates(list_cod_company)

     write_json_files('/Posicao/Garagem?codigoEmpresa={}&codigoLinha=0', list_cod_company, 'C:\\repos\\olhovivo_sptrans\\data\\tmp\\garage\\garage{}.json')

     paths = glob.glob('C:\\repos\\olhovivo_sptrans\\data\\tmp\\garage\\*.json')

     df_garage = pd.DataFrame([pd.read_json(p, typ = "Series") for p in paths])

     df_garage = pd.json_normalize(json.loads(df_garage.to_json(orient='records')))
     df_garage = pd.json_normalize(json.loads(df_garage.to_json(orient='records'))).explode('l')
     df_garage = pd.json_normalize(json.loads(df_garage.to_json(orient='records')))
     df_garage = pd.json_normalize(json.loads(df_garage.to_json(orient='records'))).explode('l.vs')
     df_garage = pd.json_normalize(json.loads(df_garage.to_json(orient='records')))

     return df_garage


def get_predict() -> pd.DataFrame:

     '''
          [string]hr Horário de referência da geração das informações
          {}p Representa um ponto de parada onde: 
               [int]cp código identificador da parada 
               [string]np Nome da parada 
               [double]py Informação de latitude da localização do veículo 
               [double]px Informação de longitude da localização do veículo 
          [{}]l Relação de linhas localizadas onde: 
               [string]c Letreiro completo 
               [int]cl Código identificador da linha 
               [int]sl Sentido de operação onde 1 significa de Terminal Principal para Terminal Secundário e 2 de Terminal Secundário para Terminal Principal 
               [string]lt0 Letreiro de destino da linha 
               [string]lt1 Letreiro de origem da linha [int]qv Quantidade de veículos localizados 
          [{}]vs Relação de veículos localizados onde: 
               [int]p Prefixo do veículo 
               [string]t Horário previsto para chegada do veículo no ponto de parada relacionado
               [bool]a Indica se o veículo é (true) ou não (false) acessível para pessoas com deficiência
               [string]ta Indica o horário universal (UTC) em que a localização foi capturada. Essa informação está no padrão ISO 8601 
               [double]py Informação de latitude da localização do veículo
               [double]px Informação de longitude da localização do veículo
     '''

     df_stops = get_stops('')
     df_stops = df_stops['cp'].to_list()

     write_json_files('/Previsao?codigoParada={}&codigoLinha=0', df_stops, 'C:\\repos\\olhovivo_sptrans\\data\\tmp\\predict\\predict_{}.json')

     paths = glob.glob('C:\\repos\\olhovivo_sptrans\\data\\tmp\\predict\\*.json')

     df_predict = pd.DataFrame([pd.read_json(p, typ = "Series") for p in paths])

     df_predict = pd.json_normalize(json.loads(df_predict.to_json(orient='records'))).explode('p.l')
     df_predict = pd.json_normalize(json.loads(df_predict.to_json(orient='records')))
     df_predict = pd.json_normalize(json.loads(df_predict.to_json(orient='records'))).explode('p.l.vs')
     df_predict = pd.json_normalize(json.loads(df_predict.to_json(orient='records')))

     return df_predict
          

auth()
print('\n\n*********************************************** PRINT GET BUS POSITION ******************************************')
print(get_bus_position(''))
print('\n\n**************************************************** PRINT GET COMPANY ******************************************')
print(get_company())
print('\n\n****************************************************** PRINT GET STOPS ******************************************')
print(get_stops(''))
print('\n\n************************************************* PRINT GET BUS RUNNER ******************************************')
print(get_bus_runner())
print('\n\n******************************************* PRINT GET BUS RUNNER STOPS ******************************************')
print(get_bus_runner_stops())
print('\n\n***************************************************** PRINT GET GARAGE ******************************************')
print(get_garage())
print('\n\n****************************************************** PRINT GET LINES ******************************************')
print(get_lines())
print('\n\n**************************************************** PRINT GET PREDICT ******************************************')
print(get_predict())