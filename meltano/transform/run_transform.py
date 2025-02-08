import sys
from datetime import datetime
sys.path.append('/project')
from extract.extract_to_parquet import extract_to_parquet
from load.load_to_db import load_to_db

def run_transform(date_str=None):

    try:
        if date_str:
            datetime.strptime(date_str, '%Y-%m-%d')
        
        print(f"Iniciando extração para data: {date_str or 'hoje'}")
        extract_to_parquet(date_str)
        
        print(f"Iniciando carga para data: {date_str or 'hoje'}")
        load_result = load_to_db(date_str)
        
        if load_result['success']:
            print("Transformação e carga concluídas com sucesso!")
            print(f"Tabelas carregadas: {', '.join(load_result['tables_loaded'])}")
        else:
            print("Transformação concluída, mas houve erros na carga!")
            for failure in load_result['tables_failed']:
                print(f"Erro na tabela {failure['table']}: {failure['error']}")
        
    except ValueError:
        print("Erro: Por favor, forneça a data no formato YYYY-MM-DD")
    except Exception as e:
        print(f"Erro durante a transformação: {str(e)}")

if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    run_transform(date_str) 