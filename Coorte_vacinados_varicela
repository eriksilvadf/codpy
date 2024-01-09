import pandas as pd
import numpy as np 
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle

# Lê os DataFrames
Doses_2016 = pd.read_excel("Varicela_2016.xlsx")
Doses_2017 = pd.read_excel("Varicela_2017.xlsx")
Doses_2018 = pd.read_excel("Varicela_2018.xlsx")
Doses_2019 = pd.read_excel("Varicela_2019.xlsx")
Populacao = pd.read_excel('pop_sinasc_2021.xlsx')




# Especifica as colunas de doses aplicadas a serem  somadas
coluna_4anos_2016, coluna_3anos_2016, coluna_2anos_2016, coluna_1ano_2016 = '4anos', '3anos', '2anos', '1ano'
coluna_4anos_2017, coluna_3anos_2017, coluna_2anos_2017, coluna_1ano_2017 = '4anos', '3anos', '2anos', '1ano'
coluna_4anos_2018, coluna_3anos_2018, coluna_2anos_2018, coluna_1ano_2018 = '4anos', '3anos', '2anos', '1ano'
coluna_4anos_2019, coluna_3anos_2019, coluna_2anos_2019, coluna_1ano_2019 = '4anos', '3anos', '2anos', '1ano'

# especfica as colunas de populacao a serem calculadas para %
pop_1ano, pop_2anos, pop_3anos, pop_4anos = 'pop_1ano', 'pop_2anos', 'pop_3anos', 'pop_4anos'



# Cria uma nova tabela com a soma das colunas
doses_aplicadas = pd.DataFrame({
    'Cod_municipio': Doses_2016['cod_municipio'],
    'nome_municipio': Doses_2016['nome_municipio'],

    'D_acumuladas_4anos': Doses_2019[coluna_4anos_2019] + Doses_2018[coluna_3anos_2018] + Doses_2017[coluna_2anos_2017] + Doses_2016[coluna_1ano_2016],
    'D_acumuladas_3anos': Doses_2019[coluna_3anos_2019] + Doses_2018[coluna_2anos_2018] + Doses_2017[coluna_1ano_2017], 
    'D_acumuladas_2anos': Doses_2019[coluna_2anos_2019] + Doses_2018[coluna_1ano_2018], 
    'D_acumuladas_1ano': Doses_2019[coluna_1ano_2019], 
       }).sort_values(by='Cod_municipio')

# Cria uma nova tabela com a soma das colunas
dfPopulacao = pd.DataFrame({
    'Cod_municipio': Populacao['cod_municipio'],
    'nome_municipio': Populacao['nome_municipio'],

    'populacao_1ano': Populacao[pop_1ano],
    'populacao_2anos': Populacao[pop_2anos],
    'populacao_3anos': Populacao[pop_3anos],
    'populacao_4anos': Populacao[pop_4anos],
}).sort_values(by='Cod_municipio').merge(doses_aplicadas,how='inner', on=['Cod_municipio','nome_municipio'])

# Cria uma nova tabela com a soma das colunas
resultado_final = pd.DataFrame({
    'Cod_municipio': dfPopulacao['Cod_municipio'],
    'nome_municipio': dfPopulacao['nome_municipio'],

    #coorte de vacinados da vacina varicela em crianças de 1 ano de idade
    'populacao_1ano': dfPopulacao['populacao_1ano'],
    'doses_aplicadas_1ano': dfPopulacao['D_acumuladas_1ano'],
    'percent_%_1ano': dfPopulacao['D_acumuladas_1ano']/ dfPopulacao['populacao_1ano'] * 100,
    'estima_1ano': np.maximum(dfPopulacao['populacao_1ano'] - dfPopulacao['D_acumuladas_1ano'], 0),

     #coorte de vacinados da vacina varicela em crianças de 2 anos de idade
    'populacao_2anos': dfPopulacao['populacao_2anos'],
    'doses_aplicadas_2anos': dfPopulacao['D_acumuladas_2anos'],
    'percent_%_2anos': dfPopulacao['D_acumuladas_2anos']/ dfPopulacao['populacao_2anos'] * 100,
    'estima_2anos': np.maximum(dfPopulacao['populacao_2anos'] - dfPopulacao['D_acumuladas_2anos'], 0),

     #coorte de vacinados da vacina varicela em crianças de 3 anos de idade
    'populacao_3anos': dfPopulacao['populacao_3anos'],
    'doses_aplicadas_3anos': dfPopulacao['D_acumuladas_3anos'],
    'percent_%_3anos': dfPopulacao['D_acumuladas_3anos']/ dfPopulacao['populacao_3anos'] * 100,
    'estima_3anos': np.maximum(dfPopulacao['populacao_3anos'] - dfPopulacao['D_acumuladas_3anos'], 0),

    #coorte de vacinados da vacina varicela em crianças de 4 anos de idade
    'populacao_4anos': dfPopulacao['populacao_4anos'],
    'doses_aplicadas_4anos': dfPopulacao['D_acumuladas_4anos'],
    'percent_%_4anos': dfPopulacao['D_acumuladas_4anos']/ dfPopulacao['populacao_4anos'] * 100,
    'estima_4anos': np.maximum(dfPopulacao['populacao_4anos'] - dfPopulacao['D_acumuladas_4anos'], 0)
})
excel_filename = "resultado_final.xlsx"

#resultado_final.round(2).to_excel(excel_filename, index=False)
#print(f'Arquivo Excel "{excel_filename}" criado com sucesso.')
print(resultado_final.round(2))
