Projeto baseado na API http://data.nba.net/prod/v1/today.json 



Objetivo:

Analisar a API 
Coletar dados dos proximos jogos 
Coletar dados dos time que iram jogar,local,data e hora
Coletar dados dos time individualmente 

Processo:

Foi realizada a exploração da API 
Os dados coletados foram juntados em um DataFrame (Team e Game)
Posteriormente mergeados 
Dados foram salvos na camada prata em um container do Azure em json
Baixados, normalizado e transformados em csv 
Foi dado upload no arquivo tratado para camada ouro no azure
