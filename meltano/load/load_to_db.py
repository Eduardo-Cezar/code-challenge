import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine

def get_transformed_db_connection():
    return create_engine('postgresql://northwind_user:thewindisblowing@db_transformed:5432/northwind_transformed')

def load_to_db(execution_date=None):
    current_date = execution_date or datetime.now().strftime('%Y-%m-%d')
    print(f'Processando data: {current_date}')
    
    # Alterando o diretório base para usar /data do host
    base_output_dir = '/data/output'
    
    postgres_tables = [
        'categories',
        'customers',
        'employees',
        'orders',
        'products',
        'shippers',
        'suppliers'
    ]
    
    result = {
        'success': True,
        'files_found': False,
        'tables_loaded': [],
        'tables_failed': []
    }
    
    engine = get_transformed_db_connection()
    
    for table in postgres_tables:
        try:
            parquet_path = os.path.join(base_output_dir, 'postgres', table, current_date, f'{table}.parquet')
            
            if os.path.exists(parquet_path):
                result['files_found'] = True
                df = pd.read_parquet(parquet_path)
                df.to_sql(table, engine, if_exists='replace', index=False)
                result['tables_loaded'].append(table)
                print(f'Carregado no banco (PostgreSQL): {table} -> northwind_transformed')
            else:
                result['tables_failed'].append({
                    'table': table,
                    'error': 'Arquivo não encontrado'
                })
                print(f'Arquivo não encontrado: {parquet_path}')
        except Exception as e:
            result['success'] = False
            result['tables_failed'].append({
                'table': table,
                'error': str(e)
            })
            print(f'Erro ao processar {table}: {str(e)}')
    
    try:
        csv_parquet_path = os.path.join(base_output_dir, 'csv', current_date, 'order_details.parquet')
        
        if os.path.exists(csv_parquet_path):
            result['files_found'] = True
            df = pd.read_parquet(csv_parquet_path)
            df.to_sql('order_details', engine, if_exists='replace', index=False)
            result['tables_loaded'].append('order_details')
            print(f'Carregado no banco (CSV): order_details -> northwind_transformed')
        else:
            result['tables_failed'].append({
                'table': 'order_details',
                'error': 'Arquivo não encontrado'
            })
            print(f'Arquivo não encontrado: {csv_parquet_path}')
    except Exception as e:
        result['success'] = False
        result['tables_failed'].append({
            'table': 'order_details',
            'error': str(e)
        })
        print(f'Erro ao processar order_details: {str(e)}')
    
    return result

if __name__ == "__main__":
    import sys
    execution_date = sys.argv[1] if len(sys.argv) > 1 else None
    load_to_db(execution_date) 