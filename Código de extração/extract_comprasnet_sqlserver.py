# -*- coding: utf-8 -*-
import os
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import pyodbc

# Gera a data atual no formato dd-mm-yyyy
data_str = datetime.now().strftime('%d-%m-%Y')

# Caminho base desejado
from pathlib import Path
base_dir = str(Path.home() / "Downloads")
download_dir = os.path.join(base_dir, f"extract_comprasnet_{data_str}")
os.makedirs(download_dir, exist_ok=True)

# Função que inicia o webdriver com configurações de download
def create_driver_instance(url):
    chrome_options = Options()
    download_dir_win = download_dir.replace("/", "\\").replace("\\\\", "\\")
    prefs = {
        "download.default_directory": download_dir_win,
        "download.prompt_for_download": False,
        "directory_upgrade": True,
        "profile.default_content_setting_values.automatic_downloads": 1
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--start-maximized")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)
    driver.implicitly_wait(8)
    return driver

# Scrolls
def scroll_to_bottom(driver):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    sleep(1)

def scroll_to_top(driver):
    driver.execute_script("window.scrollTo(0, 0);")
    sleep(1)

# Abre o navegador e acessa o ComprasNet
driver = create_driver_instance('https://contratos.comprasnet.gov.br/transparencia/contratos?orgao=%5B%2236000%22%5D')
driver.implicitly_wait(8) 
scroll_to_bottom(driver)
sleep(5)

# Exibe 100 registros por página
driver.find_element(By.XPATH, '//*[@id="crudTable_length"]/label/select/option[4]').click()
sleep(60)
scroll_to_top(driver)
sleep(20)

# Clica no botão 'visibilidade da coluna'
driver.find_element(By.XPATH, '//*[@id="datatable_button_stack"]/div/button[1]').click()

# Seleciona colunas
skip_indices = [5, 14, 18, 19, 20, 21, 22]
for i in range(3, 29):
    if i in skip_indices:
        continue
    try:
        xpath = f'//*[@id="datatable_button_stack"]/div/ul/li[{i}]/a'
        element = driver.find_element(By.XPATH, xpath)
        sleep(5)
        element.click()
        sleep(5)
    except Exception as e:
        print(f"Erro ao clicar no elemento li[{i}]: {e}")

scroll_to_top(driver)
sleep(2)

WebDriverWait(driver, 30).until(
    EC.element_to_be_clickable((By.XPATH, '//*[@id="datatable_button_stack"]/div/button[3]'))
) 

try:
    while True:
        scroll_to_top(driver)
        sleep(5)
        download_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel'))
        )
        download_button.click()
        print("Baixando o arquivo da página atual")
        sleep(10) 

        next_button_disabled = driver.find_elements(By.CSS_SELECTOR, 'li.paginate_button.next.disabled')
        if next_button_disabled:
            print("Última página. Parando o script.")
            break

        next_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="crudTable_next"]/a'))
        )
        next_button.click()
        print("Indo para a próxima página")
        sleep(30)

except Exception as e:
    print("Ocorreu um erro ou não há mais páginas para navegar:", e)

sleep(5)

# Lista arquivos .xlsx baixados
xlsx_files = [f for f in os.listdir(download_dir) if f.endswith(".xlsx")]
xlsx_file_paths = [os.path.join(download_dir, f) for f in xlsx_files]

for file_path in xlsx_file_paths:
    print(f" Arquivo encontrado: {file_path}")

print(f"\n Total de arquivos encontrados: {len(xlsx_file_paths)}")

sleep(5)

# ---------------------- SQL SERVER (NOVO DESTINO) ----------------------

# Dados de conexão ao SQL Server
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

# Nome da tabela com base na data
tabela_destino = f"Contratos_{data_str.replace('-', '_')}"
total_inseridos = 0

for file_path in xlsx_file_paths:
    print(f" Lendo arquivo {file_path}")
    
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        if df.empty:
            print(" Arquivo vazio, pulando.")
            continue

        # Cria a tabela se ainda não existir
        colunas_sql = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
        create_table_sql = f"IF OBJECT_ID('{tabela_destino}', 'U') IS NULL CREATE TABLE {tabela_destino} ({colunas_sql})"
        cursor.execute(create_table_sql)
        conn.commit()

        # Insere os dados
        placeholders = ", ".join(["?"] * len(df.columns))
        insert_sql = f"INSERT INTO {tabela_destino} ({', '.join(f'[{col}]' for col in df.columns)}) VALUES ({placeholders})"

        for _, row in df.iterrows():
            cursor.execute(insert_sql, tuple(row.fillna("").astype(str)))

        conn.commit()
        total_inseridos += len(df)
        print(f"{len(df)} registros inseridos do arquivo {os.path.basename(file_path)}")

    except Exception as e:
        print(f"Erro ao processar {file_path}: {e}")

cursor.close()
conn.close()
print(f"\n Total de registros inseridos no SQL Server: {total_inseridos}")

driver.quit()
