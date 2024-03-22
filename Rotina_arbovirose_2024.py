import os
import pandas as pd
import json
from dbfread import DBF
import subprocess
from tqdm import tqdm
import csv
import xml.etree.ElementTree as ET

# Definição dos diretórios de entrada e saída
diretorioEntradaDBC = "C:\\Projeto_Tabnet_SIHSUS\\Rotina_abovirose\\entrada_arbovirose"
diretorioSaida = "C:\\Projeto_Tabnet_SIHSUS\\Rotina_abovirose\\saida_arbovirose"
diretorioDBF = os.path.join(diretorioSaida, "dbf")
diretorioCSV = os.path.join(diretorioSaida, "csv")
diretorioXML = os.path.join(diretorioSaida, "xml")
diretorioJSON = os.path.join(diretorioSaida, "json")
diretorioParquet = os.path.join(diretorioSaida, "parquet")

# Criação dos diretórios de saída se eles não existirem
for diretorio in [diretorioDBF, diretorioCSV, diretorioXML, diretorioJSON, diretorioParquet]:
    os.makedirs(diretorio, exist_ok=True)

arquivoTABWIN = "C:\\Projeto_Tabnet_SIHSUS\\Tabwin\\dbf2dbc.exe"

# Função para converter CSV para XML
def csv_para_xml(file_csv, file_xml):
    root = ET.Element('Tabela')
    with open(file_csv, 'r', encoding='ISO-8859-1') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader)
        for row in csv_reader:
            linha = ET.SubElement(root, 'Linha')
            for campo, valor in zip(header, row):
                tag = ET.SubElement(linha, campo)
                tag.text = valor
    tree = ET.ElementTree(root)
    tree.write(file_xml, encoding='UTF-8', xml_declaration=True)
    print(f'Arquivo XML gerado com sucesso: {file_xml}')

# Função para converter CSV para JSON
def csv_para_json(file_csv, file_json):
    data = []
    with open(file_csv, 'r', encoding='ISO-8859-1') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            data.append(row)
    with open(file_json, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
    print(f'Arquivo JSON gerado com sucesso: {file_json}')

# Função para converter DBF para Parquet
def dbf_para_parquet(file_dbf, file_parquet):
    try:
        dbf_file = DBF(file_dbf, encoding="latin1")
        df = pd.DataFrame(iter(dbf_file))
        df.to_parquet(file_parquet)
        print(f'Arquivo Parquet gerado com sucesso: {file_parquet}')
    except Exception as e:
        print(f"Erro ao processar o arquivo {file_dbf}: {e}")

# Converte de DBC para DBF
arquivos_dbc = [os.path.join(diretorioEntradaDBC, arquivo) for arquivo in os.listdir(diretorioEntradaDBC) if arquivo.endswith(".dbc")]

# Barra de progresso para conversão de DBC para DBF
with tqdm(total=len(arquivos_dbc), desc="Conversão DBC para DBF") as pbar_dbc:
    for arquivo_dbc in arquivos_dbc:
        subprocess.run([arquivoTABWIN, arquivo_dbc, diretorioDBF], shell=True)
        pbar_dbc.update(1)

# Converte de DBF para CSV, XML, JSON e Parquet
with tqdm(total=len(os.listdir(diretorioDBF)), desc="Conversão DBF para CSV, XML, JSON e Parquet") as pbar_csv:
    for arquivo in os.listdir(diretorioDBF):
        if arquivo.endswith(".dbf"):
            caminho_dbf = os.path.join(diretorioDBF, arquivo)
            try:
                nome_base = os.path.splitext(arquivo)[0]
                caminho_csv = os.path.join(diretorioCSV, f"{nome_base}.csv")
                caminho_parquet = os.path.join(diretorioParquet, f"{nome_base}.parquet")
                df = pd.DataFrame(iter(DBF(caminho_dbf, encoding="latin1")))
                df.to_csv(caminho_csv, index=False)
                csv_para_xml(caminho_csv, os.path.join(diretorioXML, f"{nome_base}.xml"))
                csv_para_json(caminho_csv, os.path.join(diretorioJSON, f"{nome_base}.json"))
                dbf_para_parquet(caminho_dbf, caminho_parquet)
            except Exception as e:
                print(f"Erro ao processar o arquivo {arquivo}: {e}")
            finally:
                pbar_csv.update(1)

print("Conversão concluída com sucesso!")