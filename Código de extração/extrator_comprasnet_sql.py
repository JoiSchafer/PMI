# -*- coding: utf-8 -*-
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyodbc
from time import sleep

# Gera a data atual no formato dd-mm-yyyy
data_str = datetime.now().strftime('%d-%m-%Y')

# Caminho base desejado
base_dir = str(Path.home() / "Downloads")
download_dir = os.path.join(base_dir, f"extract_comprasnet_{data_str}")
os.makedirs(download_dir, exist_ok=True)

"""
Aqui poderia estar a lógica de extração via Selenium (comentada
no arquivo original). Este script assume que já existem arquivos
.xlsx na pasta download_dir.
"""

# Simula um pequeno delay, se necessário
sleep(1)

# Lista arquivos .xlsx baixados
xlsx_files = [f for f in os.listdir(download_dir) if f.endswith(".xlsx")]
xlsx_file_paths = [os.path.join(download_dir, f) for f in xlsx_files]

# ---------------------- SQL SERVER ----------------------

# Conexão com o SQL Server
server = r"LAPTOP-3ANH0P70\SQLEXPRESS"
database = "PMI"
username = "sa"
password = "Senha123"

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Nome fixo da tabela de destino
tabela_destino = "Contratos_Comprasnet"

# Colunas que identificam de forma única um contrato
KEY_COLS = ["Número do Contrato", "Data de Assinatura"]
total_inseridos = 0

for file_path in xlsx_file_paths:
    print(f" Lendo arquivo {file_path}")
    try:
        # Usa a primeira linha do Excel como cabeçalho
        df = pd.read_excel(file_path, engine="openpyxl", header=0)

        if df.empty:
            print("Arquivo vazio, pulando.")
            continue

        # Cria a tabela se não existir
        colunas_sql = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
        create_table_sql = (
            f"IF OBJECT_ID('{tabela_destino}', 'U') IS NULL "
            f"CREATE TABLE {tabela_destino} ({colunas_sql})"
        )
        cursor.execute(create_table_sql)
        conn.commit()

        # Verifica se as colunas de chave existem no DataFrame
        missing_keys = [col for col in KEY_COLS if col not in df.columns]
        if missing_keys:
            raise ValueError(
                f"As colunas de chave {missing_keys} não foram encontradas "
                f"no arquivo {os.path.basename(file_path)}."
            )

        # Inserção dos dados apenas para contratos ainda não existentes
        placeholders = ", ".join(["?"] * len(df.columns))
        insert_sql = (
            f"INSERT INTO {tabela_destino} "
            f"({', '.join(f'[{col}]' for col in df.columns)}) "
            f"VALUES ({placeholders})"
        )

        registros_inseridos_arquivo = 0

        for _, row in df.iterrows():
            # Monta a consulta de verificação usando a chave (Número do Contrato + Data de Assinatura)
            key_values = [str(row[col]) for col in KEY_COLS]
            where_clause = " AND ".join(f"[{col}] = ?" for col in KEY_COLS)
            check_sql = f"SELECT 1 FROM {tabela_destino} WHERE {where_clause}"

            cursor.execute(check_sql, key_values)
            exists = cursor.fetchone()

            if exists:
                # Já existe contrato com a mesma chave, não insere novamente
                continue

            # Insere o registro novo
            all_values = list(row.fillna("").astype(str))
            cursor.execute(insert_sql, all_values)
            registros_inseridos_arquivo += 1

        conn.commit()
        total_inseridos += registros_inseridos_arquivo
        print(
            f" {registros_inseridos_arquivo} registros inseridos do arquivo "
            f"{os.path.basename(file_path)}"
        )

    except Exception as e:
        print(f"Erro ao processar {file_path}: {e}")

cursor.close()
conn.close()
print(f"\n Total de registros inseridos no SQL Server: {total_inseridos}")