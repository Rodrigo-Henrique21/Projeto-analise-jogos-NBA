#Import das bibliotecas

import requests 
import json
import pandas as pd
import datetime as dt
import time
import os 
import azure.storage.blob
from datetime import datetime
from azure.storage.blob import PublicAccess, ContainerClient, BlobServiceClient


global blob_texts
blob_texts = {}


# Definindo URL base

url_base = 'http://data.nba.net/prod/v1/today.json'
url_incio = 'http://data.nba.net'


# Realizando a requisição na API 

requisicao = requests.get(url_base,timeout = 15)

# Verificando retorno
print(requisicao)

# Transformando para json 
resposta = requisicao.json()

# Verificando as chaves do retorno em json
chaves = resposta.keys()

# Verificando dados
print(resposta)

# Capturando os links
def captura_link():
    for i in resposta:
        links = resposta['links']['currentScoreboard']
        return links
captura_link()


# Captura link times

def captura_team():
    for i in resposta:
        team = resposta['links']['teams']
        return team 
        
team = captura_team()
print(team)


teste = requests.get(url_incio+team).json()
print(teste['league']['standard'])

# Chamando link capturado

nova_requisicao = requests.get(url_incio+captura_link())

# Verificando status
print(nova_requisicao)

# Transformando para json 

retorno_link = nova_requisicao.json()

# Capturando games

games = retorno_link['games']
print(games)


# Verifica se há jogos 

url_incio = 'http://data.nba.net'

# Tratativa na data 
data = dt.date.today()
data = str(data)
data = data.replace('-','')

# Passa a data tratada para uma variavel 
data_list = data

# Tira o link do formato de lista
link = []
link.append(captura_link())
link = ','.join(link)


# Verifica se hoje há jogos se não busca a data que tenha 
def verfica_Jogos():
    status = 1
    contador = 0 
    dias = 0 
    while (status == 1):
        contador += 1 
        dias += 1 
        dia = str(link[15]) + str(link[16])
        mes = str(link[13]) + str(link[14])
        ano = str(link[9]) + str(link[10]) + str(link[11]) + str(link[12])
        data = ano+'/'+mes+'/'+dia
        data = datetime.strptime(data,'%Y/%m/%d').date()
        data_nova = data + dt.timedelta(days=dias)
        data_nova = str(data_nova)
        data_nova = data_nova.replace('-','')
        data_antiga = ano+mes+dia
        url = (link.replace(data_antiga,data_nova))
        requisicao = requests.get(url_incio+url,timeout = 10).json()
        if (requisicao['games'] == []):
            status = 1 
        else:
            status = 0 
            requisicao_team = requests.get(url_incio+team).json()
    return requisicao['games'],requisicao_team['league']['standard']

# Chama a funcao para uma variavel
requisicao,team = verfica_Jogos()
print(requisicao)
print('')
print('')
print(team)

# Print retorno funcao

# Verifica range de date 
def verifica_range_date():
    contador = 0
    range_list = []
    for i in link:
        contador += 1
        if(i in data_list):
            if(contador > 7):
                range_list.append(contador)
    max_list = max(range_list)
    min_list = min(range_list)
    range_list = str(min_list)+','+str(max_list)
    return range_list

verifica_range_date()

#for i in requisicao:
 #   print(i)

 # captura itens 

home_list = []
rival_list = []
cidade_list = []
local_list = []
dataEhora_list = []

def imprimi_jogos():
    for i in requisicao:
        home =  i['vTeam']['triCode']
        rival = i['hTeam']['triCode']
        local = i['arena']['name']
        cidade = i['arena']['city']
        data = i['startDateEastern']
        data = list(data)
        data = data[0]+data[1]+data[2]+data[3]+'-'+data[4]+data[5]+'-'+data[6]+data[7]
        data = datetime.strptime(data, '%Y-%m-%d').date()
        hora = i['startTimeEastern']
        data = str(data)
        dataEhora = data+' '+hora
        home_list.append(home)
        rival_list.append(rival)
        cidade_list.append(cidade)
        local_list.append(local)
        dataEhora_list.append(dataEhora)
    return print('itens adicionados com sucesso')


imprimi_jogos()        

# captura variaveis team 

team_full_name_home = []
team_tricode_home = []
team_city_home = []

team_full_name_rival = []
team_tricode_rival = []
team_city_rival = []

for i in team:
    if (i['tricode'] in home_list):
        team_full_name_home.append(i['fullName'])
        team_tricode_home.append(i['tricode'])
        team_city_home.append(i['city'])
    if (i['tricode'] in rival_list):
        team_full_name_rival.append(i['fullName'])
        team_tricode_rival.append(i['tricode'])
        team_city_rival.append(i['city'])
    
print(team_full_name_home)
print(team_tricode_home)
print(team_city_home)

print(team_full_name_rival)
print(team_tricode_rival)
print(team_city_rival)

# transformando em um dataframe

df = pd.DataFrame(team_full_name_home)
df['home_city'] = team_city_home
df['tricode_home'] = team_tricode_home
df['rival_name'] = team_full_name_rival
df['rival_city'] = team_city_rival
df['tricode_rival'] = team_tricode_rival

df_team = df.rename(columns={0:'home_nome'})

print(df_team)


# Salva itens em um dataframe

df_game = pd.DataFrame(home_list, columns = ['tricode_home'])

df_game['tricode_rival'] = rival_list
df_game['cidade'] = cidade_list
df_game['local'] = local_list
df_game['dataEhora'] = dataEhora_list


print(df_game)
   

# Mergeando dataframe

df = df_team.merge(df_game , left_on = 'tricode_home', right_on = 'tricode_home',how="inner")

print(df)


# Passando dataframe para arquivo em excel

df.to_excel(r'C:\Users\Rodrigo Correa\Desktop\Codigos\jogoNBA.xlsx',index=False)

leitura = pd.read_excel(r'C:\Users\Rodrigo Correa\Desktop\Codigos\jogoNBA.xlsx')

leitura.head()



# Faz o upload do dataframe para azure

def upload_file():
    connection_string = "DefaultEndpointsProtocol=https;AccountName=staengdados;AccountKey=KtfGJ/u3NWxqsFBksx2gR8hRVAcpV0lsVr9liYwsXJoTx68DIa2KtFVobhO6Ob3bmo8PcobxzNYk+AStltMUjA==;EndpointSuffix=core.windows.net"
    container_name = 'teste1'
    nome_file = "dadosNBA"
    data = df
    filename = '{}.json'.format(nome_file)  
    
    container_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = container_client.get_blob_client(container=container_name, blob = filename)

    output = pd.DataFrame(df).to_json()
    print('upload sendo realizando...')
    time.sleep(4)
    blob_client.upload_blob(output, blob_type="BlockBlob", overwrite = True)
    print('upload concluido com sucesso')

upload_file()


# cria funcoes de normalizacao 

normalize_keys = {}

custom_normalize_keys = {}


# DEFINIÇÃO DAS FUNÇÕES


def create_obj(keys, value = ''):
    return { i: copy.copy(value) for i in keys }        

def normalize_level(data, principal_key, default_keys):
    if principal_key in data:
        if not isinstance(data[principal_key]['0'], (dict, list)):
            return None
        temp_data = create_obj(default_keys, {})
        for key, value in data[principal_key].items():
            if not isinstance(value, (dict, list)) or value == []:
                continue
            if value == {}:
                value = create_obj(default_keys)
            if isinstance(value, list):
                value = value[0]
            for dkey, dvalue in value.items():
                if dkey in default_keys:
                    temp_data[dkey][key] = dvalue
        del data[principal_key]    
        data.update(temp_data)
    return data

def normalize_keys_data(data, pre_normalize_arr = {}):
    data = json.loads(data.content_as_text())
    data = pd.json_normalize(data, max_level = 0).iloc[0].to_dict()
    
    for key, default_value in pre_normalize_arr.items():
        normalize_level(data, key, default_value) 
    
    for key, default_value in normalize_keys.items():
        normalize_level(data, key, default_value) 
    
    if data == {} or data == []:
        return None
    
    return pd.DataFrame(pd.json_normalize(data, max_level = 0).iloc[0].to_dict()).to_csv(index = False)

def normalize_blob_data(data, max_level = -1):
    return pd.json_normalize(json.loads(data.content_as_text()), max_level = max_level).iloc[0].to_json()

def download_and_contain_blob(blob_client, blob_name):
    print('[{}]:[INFO] : Blob name: {}'.format(dt.datetime.utcnow(), blob_name))
    print("[{}]:[INFO] : Downloading {} ...".format(dt.datetime.utcnow(), blob_name))
    blob_name = blob_name.split('.')[0] + '.csv'
    blob_data = blob_client.download_blob()
    
    pre_normalize = {}
    for normalize_name, normalize_data in custom_normalize_keys.items():
        if normalize_name in blob_name:
            pre_normalize.update(normalize_data)
    
    blob_text = normalize_keys_data(blob_data, pre_normalize)
    blob_texts.update({blob_name: blob_text})
    print("[{}]:[INFO] : download finished".format(dt.datetime.utcnow()))

def download_blob(blob_client, blob_name):
    print('[{}]:[INFO] : Blob name: {}'.format(dt.datetime.utcnow(), blob_name))
    print("[{}]:[INFO] : Downloading {} ...".format(dt.datetime.utcnow(), blob_name))
    blob_data = blob_client.download_blob()
    print("[{}]:[INFO] : download finished".format(dt.datetime.utcnow()))
    return blob_data
  
connection_string = "DefaultEndpointsProtocol=https;AccountName=staengdados;AccountKey=KtfGJ/u3NWxqsFBksx2gR8hRVAcpV0lsVr9liYwsXJoTx68DIa2KtFVobhO6Ob3bmo8PcobxzNYk+AStltMUjA==;EndpointSuffix=core.windows.net"
    
def upload_all_data(data, connection_string = connection_string, container_name = 'teste1'):
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    print(container_client)
    print('Fazendo Upload arquivos...')

    for blob_name, blob_data in data.items():
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(blob_data, overwrite = True)
        print('Upload finalizado: {}'.format(blob_name))


# faz dowload dos referente á NBA 

# cria as conexoes 

connection_string = "DefaultEndpointsProtocol=https;AccountName=staengdados;AccountKey=KtfGJ/u3NWxqsFBksx2gR8hRVAcpV0lsVr9liYwsXJoTx68DIa2KtFVobhO6Ob3bmo8PcobxzNYk+AStltMUjA==;EndpointSuffix=core.windows.net"
container_name = "teste1"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)


# lista os arquivos do container

blob_list = container_client.list_blobs()
for blob in blob_list:
    # So baixa se a extensao for json 
    if blob.name.split('.')[1].lower() != 'json':
        continue
    # captura os arquivos 
    blob_client = container_client.get_blob_client(blob.name)
    # faz o dowload e normaliza 
    download_and_contain_blob(blob_client, blob.name)


blob_texts = { k: v for k, v in blob_texts.items() if v } 

print(blob_texts)

data_names = pd.DataFrame(list(blob_texts)).values


# VERIFICA SE A VARIAVEL POSSUI DADOS 
if blob_texts == {}:
    print('Não há nenhum dado para ser utilizado!')
else:
    print('[{}]:[INFO] : Nome da Coleção de Dados: '.format(dt.datetime.utcnow()) + data_names)

upload_all_data(blob_texts)
