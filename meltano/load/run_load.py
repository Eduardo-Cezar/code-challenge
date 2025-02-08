import sys
import json
from datetime import datetime
from load_to_db import load_to_db

def run_load(date_str=None):
    result = {
        "success": False,
        "date": date_str or datetime.now().strftime('%Y-%m-%d'),
        "message": "",
        "details": {
            "tables_loaded": [],
            "tables_failed": []
        },
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    try:
        if date_str:
            datetime.strptime(date_str, '%Y-%m-%d')
        
        load_status = load_to_db(date_str)
        
        if not load_status['files_found']:
            result['message'] = f"Nenhum arquivo encontrado para a data {result['date']}"
            print(json.dumps(result, indent=2, ensure_ascii=False))
            return result
        
        result['success'] = load_status['success']
        result['details']['tables_loaded'] = load_status['tables_loaded']
        result['details']['tables_failed'] = load_status['tables_failed']
        
        if result['success']:
            result['message'] = "Carregamento concluído com sucesso"
        else:
            result['message'] = "Carregamento concluído com erros"
        
    except ValueError:
        result['message'] = "Erro: Data fornecida em formato inválido. Use YYYY-MM-DD"
    except Exception as e:
        result['message'] = f"Erro durante o carregamento: {str(e)}"
    
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result

if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    run_load(date_str) 