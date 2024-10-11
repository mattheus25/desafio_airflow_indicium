from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models import Variable
import base64
import sqlite3
import pandas as pd

# Configurações padrão (default_args)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mattheus.nascimento@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Função para ler dados da tabela 'Order' e salvar como CSV
def extract_orders():
    conn = sqlite3.connect('/mnt/c/Users/Matheus Martins/Desktop/airflow/airflow_tooltorial/data/Northwind_small.sqlite')
    query = "SELECT * FROM 'Order'"
    df = pd.read_sql_query(query, conn)
    df.to_csv('/mnt/c/Users/Matheus Martins/Desktop/airflow/airflow_tooltorial/data/output_orders.csv', index=False)
    conn.close()

# Função para ler dados da tabela 'OrderDetail', fazer o JOIN e calcular a soma
def process_order_details():
    conn = sqlite3.connect('/mnt/c/Users/Matheus Martins/Desktop/airflow/airflow_tooltorial/data/Northwind_small.sqlite')
    query = "SELECT * FROM 'OrderDetail'"
    df_order_detail = pd.read_sql_query(query, conn)

    # Carregar os dados do arquivo CSV gerado anteriormente
    df_orders = pd.read_csv('/mnt/c/Users/Matheus Martins/Desktop/airflow/airflow_tooltorial/data/output_orders.csv')

    # Fazer o JOIN entre as tabelas
    df_join = pd.merge(df_orders, df_order_detail, how='inner', left_on='Id', right_on='OrderId')

    # Filtrar para o destino 'Rio de Janeiro' e somar as quantidades
    total_quantity = df_join[df_join['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()

    # Escrever a soma em count.txt
    with open('/mnt/c/Users/Matheus Martins/Desktop/airflow/airflow_tooltorial/data/count.txt', 'w') as f:
        f.write(str(total_quantity))

    conn.close()

# Função final export_final_output
def export_final_output():
    # Ler o valor do count.txt
    with open('/mnt/c/Users/Matheus Martins/Desktop/airflow/airflow_tooltorial/data/count.txt', 'r') as f:
        count = f.read().strip()
    
    # Obter o email da variável do Airflow
    my_email = Variable.get("my_email")
    
    # Criar a mensagem em base64
    message = my_email + count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    
    # Escrever a mensagem codificada no arquivo final_output.txt
    with open('/mnt/c/Users/Matheus Martins/Desktop/airflow/airflow_tooltorial/data/final_output.txt', 'w') as f:
        f.write(base64_message)

# Definir o DAG
with DAG(
    'northwind_dag',
    default_args=default_args,
    description='DAG para processar dados do banco Northwind',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False
) as dag:

    # Task 1: Extrair dados da tabela 'Order'
    task1 = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
    )

    # Task 2: Fazer JOIN com 'OrderDetail' e calcular a soma
    task2 = PythonOperator(
        task_id='process_order_details',
        python_callable=process_order_details,
    )

    # Task final: exportar resultado final com valor base64
    task3 = PythonOperator(
        task_id='export_final_output_task',
        python_callable=export_final_output,
        provide_context=True
    )

    # Definir a ordem das tasks
    task1 >> task2 >> task3