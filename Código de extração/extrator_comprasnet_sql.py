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

# Lista arquivos .xlsx já baixados
xlsx_files = [f for f in os.listdir(download_dir) if f.endswith(".xlsx")]
xlsx_file_paths = [os.path.join(download_dir, f) for f in xlsx_files]

for file_path in xlsx_file_paths:
    print(f"📂 Arquivo encontrado: {file_path}")

print(f"\n✅ Total de arquivos encontrados: {len(xlsx_file_paths)}")

# ------------------ PARTE DE EXTRAÇÃO COMENTADA ------------------

"""
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# def create_driver_instance(url):
#     chrome_options = Options()
#     prefs = {
#         "download.default_directory": download_dir,
#         "download.prompt_for_download": False,
#         "directory_upgrade": True,
#         "profile.default_content_setting_values.automatic_downloads": 1
#     }
#     chrome_options.add_experimental_option("prefs", prefs)
#     chrome_options.add_argument("--no-sandbox")
#     chrome_options.add_argument("--window-size=1920x1080")
#     chrome_options.add_argument("--start-maximized")
#     service = Service(ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=service, options=chrome_options)
#     driver.get(url)
#     return driver

# url = 'https://contratos.comprasnet.gov.br/transparencia/contratos?...'
# driver = create_driver_instance(url)
# ... (toda a lógica de extração com scroll, filtros, cliques e download foi comentada)
"""

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

# Cria tabela com base na data
tabela_destino = f"Contratos_{data_str.replace('-', '_')}"
total_inseridos = 0

for file_path in xlsx_file_paths:
    print(f"📥 Lendo arquivo {file_path}")
    try:
        # ✅ Usa a primeira linha do Excel como cabeçalho
        df = pd.read_excel(file_path, engine="openpyxl", header=0)

        if df.empty:
            print("⚠️ Arquivo vazio, pulando.")
            continue

        # Cria a tabela se não existir
        colunas_sql = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
        create_table_sql = f"IF OBJECT_ID('{tabela_destino}', 'U') IS NULL CREATE TABLE {tabela_destino} ({colunas_sql})"
        cursor.execute(create_table_sql)
        conn.commit()

        # Inserção dos dados
        placeholders = ", ".join(["?"] * len(df.columns))
        insert_sql = f"INSERT INTO {tabela_destino} ({', '.join(f'[{col}]' for col in df.columns)}) VALUES ({placeholders})"

        for _, row in df.iterrows():
            cursor.execute(insert_sql, tuple(row.fillna("").astype(str)))

        conn.commit()
        total_inseridos += len(df)
        print(f"✅ {len(df)} registros inseridos do arquivo {os.path.basename(file_path)}")

    except Exception as e:
        print(f"❌ Erro ao processar {file_path}: {e}")

cursor.close()
conn.close()
print(f"\n📦 Total de registros inseridos no SQL Server: {total_inseridos}")
