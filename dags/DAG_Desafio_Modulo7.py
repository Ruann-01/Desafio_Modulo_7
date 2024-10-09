# Desenvolvedor: Ruann Campos de Castro Farrapo

from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
import sqlite3
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 1: Leitura da tabela 'orders' e exportação para CSV
def extract_orders():
    # Caminho do diretório onde o CSV será salvo
    output_dir = '/opt/airflow/outputs'

    # Verificando se o diretório existe, e se não, criar
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Diretório {output_dir} criado!")

    # Usando o caminho dentro do container do Airflow
    conn = sqlite3.connect('/opt/airflow/data/Northwind_small.sqlite')
    # Extraindo dados da tabela 'Order'
    orders = pd.read_sql_query('SELECT * FROM "Order"', conn)
    # Salvando os dados em CSV
    orders.to_csv('/opt/airflow/outputs/output_orders.csv', index=False)
    conn.close()

# Task 2: Join da tabela 'order_details' com 'orders' e cálculo da soma de quantidade
def calculate_quantity_rio():
    conn = sqlite3.connect('/opt/airflow/data/Northwind_small.sqlite')
    order_details = pd.read_sql_query("SELECT * FROM 'OrderDetail'", conn)
    orders = pd.read_csv('/opt/airflow/outputs/output_orders.csv')

    # Convertendo as colunas 'OrderID' para int, ignorando valores inválidos
    order_details['OrderId'] = pd.to_numeric(order_details['OrderId'], errors='coerce')
    orders['Id'] = pd.to_numeric(orders['Id'], errors='coerce')
    
    # Removendo possíveis linhas com valores NaN, que não possam ser convertidas
    order_details = order_details.dropna(subset=['OrderId'])
    orders = orders.dropna(subset=['Id'])
    
    # Convertendo de volta para inteiros (agora que possíveis valores inválidos foram removidos)
    order_details['OrderId'] = order_details['OrderId'].astype(int)
    orders['Id'] = orders['Id'].astype(int)
    
    # Fazendo o merge entre order_details e orders usando o Id dos pedidos
    merged_data = pd.merge(order_details, orders, left_on='OrderId', right_on='Id')

    # Filtrando para os dados com ShipCity como 'Rio de Janeiro'
    rio_data = merged_data[merged_data['ShipCity'] == 'Rio de Janeiro']

    # Calculando a soma das quantidades
    total_quantity = rio_data['Quantity'].sum()

    # Escrevendo o resultado no arquivo count.txt
    with open('/opt/airflow/outputs/count.txt', 'w') as f:
        f.write(str(total_quantity))
    
    conn.close()

# Task 3: Exportando o resultado final para final_output.txt
def export_final_answer():
    import base64
    from airflow.models import Variable

    with open('/opt/airflow/outputs/count.txt') as f:
        count = f.read().strip()

    my_email = Variable.get("my_email")
    message = my_email + count
    base64_message = base64.b64encode(message.encode('ascii')).decode('ascii')

    with open("/opt/airflow/outputs/final_output.txt", "w") as f:
        f.write(base64_message)

# Definindo o DAG
with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Extraindo os dados da tabela 'Order'
    extract_orders_task = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
    )

    # Task 2: Calculando a soma da quantidade vendida para o Rio de Janeiro
    calculate_quantity_rio_task = PythonOperator(
        task_id='calculate_quantity_rio',
        python_callable=calculate_quantity_rio,
    )

    # Task 3: Exportando o resultado final
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
    )

    # Definindo a ordem das tasks
    extract_orders_task >> calculate_quantity_rio_task >> export_final_output

