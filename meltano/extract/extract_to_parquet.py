import pandas as pd
import os
import shutil
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine

def get_db_connection():
    return create_engine('postgresql://northwind_user:thewindisblowing@db:5432/northwind')

def copy_csv_from_host():
    host_csv = '/data/order_details.csv'
    input_dir = 'data/input'
    

    os.makedirs(input_dir, exist_ok=True)
    
    destination = os.path.join(input_dir, 'order_details.csv')
    
    try:
        shutil.copy2(host_csv, destination)
        print(f'Arquivo CSV copiado com sucesso!: {host_csv} -> {destination}')
        return True
    except Exception as e:
        print(f'Erro ao realizar a  copia arquivo CSV: {str(e)}')
        return False

def extract_to_parquet(execution_date=None):
    current_date = execution_date or datetime.now().strftime('%Y-%m-%d')
    print(f'Processando data: {current_date}')
    
    input_dir = '/data'
    base_output_dir = '/data/output'
    
    order_details_file = os.path.join(input_dir, 'order_details.csv')
    if os.path.exists(order_details_file):
        try:
            df = pd.read_csv(order_details_file)
            output_dir = os.path.join(base_output_dir, 'csv', current_date)
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, 'order_details.parquet')
            df.to_parquet(output_file, index=False)
            print(f'Convertido CSV: order_details -> {output_file}')
        except Exception as e:
            print(f'Erro ao processar order_details.csv: {str(e)}')
    else:
        print(f'Arquivo order_details.csv não encontrado em: {order_details_file}')
    
    db_tables = [
        'categories',
        'customers',
        'employees',
        'orders',
        'products',
        'shippers',
        'suppliers'
    ]
    
    engine = get_db_connection()
    for table in db_tables:
        try:
            df = pd.read_sql_table(table, engine)
            output_dir = os.path.join(base_output_dir, 'postgres', table, current_date)
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f'{table}.parquet')
            df.to_parquet(output_file, index=False)
            print(f'Convertido PostgreSQL: {table} -> {output_file}')
        except Exception as e:
            print(f'Erro ao processar {table}: {str(e)}')

if __name__ == "__main__":
    import sys
    execution_date = sys.argv[1] if len(sys.argv) > 1 else None
    extract_to_parquet(execution_date) 