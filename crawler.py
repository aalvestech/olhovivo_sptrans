from operator import index
from time import strftime
from turtle import clear
from unicodedata import name
from urllib import response
import requests
import pandas as pd
import json
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv, find_dotenv
import os
import pathlib as Path
import glob

load_dotenv()

session = requests.Session()
url = 'http://api.olhovivo.sptrans.com.br/v2.1/'

def auth() -> str:

     TOKEN_API_OLHOVIVO = os.getenv("TOKEN_API_OLHOVIVO")

     endpoint = 'Login/Autenticar?token={}'.format(TOKEN_API_OLHOVIVO)
     response = session.post(url + endpoint)

     return print(response.text)

def _get(endpoint):

    response = session.get(url + endpoint)
    data = response.json()

    return data

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
     [{}]vs Relação de veículos localizados, onde: [int]p Prefixo do veículo
     [bool]a Indica se o veículo é (true) ou não (false) acessível para pessoas com deficiência
     [string]ta Indica o horário universal (UTC) em que a localização foi capturada. Essa informação está no padrão ISO 8601
     [double]py Informação de latitude da localização do veículo
     [double]px Informação de longitude da localização do veículo
     '''

     bus_position = _get('Posicao?codigoLinha={}'.format(line_id))

     df_bus_position = pd.DataFrame(bus_position)
     df_bus_position = pd.json_normalize(json.loads(df_bus_position.to_json(orient='records'))).explode('l.vs')
     df_bus_position = pd.json_normalize(json.loads(df_bus_position.to_json(orient='records')))
     df_bus_position.columns = (
          ['hr_ref', 'letreiro_completo', 'id_linha',
           'sentido_operacao', 'detino_linha', 'origem_linha',
           'qtd_veiculos', 'prefixo_veiculo', 'flag_acessibilidade',
           'data_ref_api', 'geo_loc_y', 'geo_loc_x'])
     # TODO - Verificar bug da hora, esta dando 3 horas a mais.
     df_bus_position['ano_part'] = pd.to_datetime("today").strftime("%Y")
     df_bus_position['mes_part'] = pd.to_datetime("today").strftime("%m")
     df_bus_position['dia_part'] = pd.to_datetime("today").strftime("%d")
     df_bus_position['hora_part'] = pd.to_datetime("now").strftime("%H:%M:%S")

     #df_bus_position.to_parquet('C:\\repos\\sptrans_olhovivo\\data/bus_postion.parque', partition_cols=['ano_part', 'mes_part', 'dia_part'])
     #df_bus_position.to_parquet('/home/ubuntu/repos/sptrans_olhovivo/data', partition_cols=['ano_part', 'mes_part', 'dia_part'])

     return print(df_bus_position)

def get_garage(company_id, line_id):

     bus_in_garage = _get('/Posicao/Garagem?codigoEmpresa={}&codigoLinha={}'.format(company_id, line_id))

     return print(bus_in_garage)

def get_company():

     company = _get('/Empresa')

     df_company = pd.DataFrame(company)
     df_company = pd.json_normalize(json.loads(df_company.to_json(orient='records'))).explode('e.e')
     df_company = pd.json_normalize(json.loads(df_company.to_json(orient='records')))
     df_company.columns = ('hr_ref', 'codigo_empresa_area', 'codigo_area', 'codigo_ref_empresa', 'nome_empresa') 

     # print(df_company)

     return df_company

def get_stops(stop_id):

     stops = _get('/Parada/Buscar?termosBusca={}'.format(stop_id))
     df_stops = pd.DataFrame(stops)
     df_stops = pd.json_normalize(json.loads(df_stops.to_json(orient='records')))
     df_stops.columns = ('codigo_parada', 'nome_parada', 'endereco_parada', 'geo_loc_y', 'geo_loc_x')
     
     return print(df_stops) 

def get_bus_runner():

     bus_runner = _get('/Corredor')
     bus_runner = pd.DataFrame(bus_runner)
     bus_runner = pd.json_normalize(json.loads(bus_runner.to_json(orient='records')))
     bus_runner.columns = ('codigo_corredor', 'nome_corredor')

     return print(bus_runner)

def get_bus_runner_stops(runner_id):

     bus_runner_stops = _get('/Parada/BuscarParadasPorCorredor?codigoCorredor={}'.format(runner_id))
     df_bus_runner_stops = pd.DataFrame(bus_runner_stops)
     df_bus_runner_stops.columns = ('codigo_corredor_parada', 'nome_corredor_parada', 'endereco_corredor_parada', 'geo_loc_y', 'geo_loc_x')

     return print(df_bus_runner_stops)

def get_garage():

     # garage = _get('/Posicao/Garagem?codigoEmpresa=37&codigoLinha=0')
     # df_garage = pd.DataFrame(garage)
     lista = get_company()
     lista_cod_empresa = lista['codigo_ref_empresa'].to_list()

     def remove_duplicates(lista_cod_empresa):
          return list(set(lista_cod_empresa))

     lista_cod_empresa = remove_duplicates(lista_cod_empresa)

     for empresa in lista_cod_empresa:
          
          garage = _get('/Posicao/Garagem?codigoEmpresa={}&codigoLinha=0'.format(empresa))
          
          with open('C:\\repos\\olhovivo_sptrans\\data\\tmp\\company{}_.json'.format(empresa), 'w') as f:
               json.dump(garage, f)

     paths = glob.glob("C:\\repos\\sptrans_olhovivo\\data\\tmp\\*.json")
     df_garage = pd.DataFrame([pd.read_json(p, typ="series") for p in paths])
     df_garage = pd.json_normalize(json.loads(df_garage.to_json(orient='records'))).explode('l')
     df_garage = pd.json_normalize(json.loads(df_garage.to_json(orient='records')))
     df_garage = pd.json_normalize(json.loads(df_garage.to_json(orient='records'))).explode('l.vs')
     df_garage = pd.json_normalize(json.loads(df_garage.to_json(orient='records')))
     df_garage.columns = ('hr_ref', 'letreiro_completo', 'identificador_linha', 'sentid_linha',
                             'destino_linha', 'origem_linha', 'quantidade_veiculos', 'prefixo_veiculo',
                             'flag_acessibilidade', 'data_ref_api', 'geo_loc_y', 'geo_loc_x')

     return print(df_garage)


auth()
get_bus_position('')
get_company()
get_stops('')
get_bus_runner()
get_bus_runner_stops('9')
get_garage()