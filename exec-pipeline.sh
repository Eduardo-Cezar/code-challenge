#!/bin/bash
CONTAINER_NAME="code-challenge-meltano-1"

# Função de ajuda
show_help() {
    echo "Uso: ./exec-pipeline.sh <comando> [data]"
    echo ""
    echo "Comandos disponíveis:"
    echo "  transform         Executa o pipeline completo para a data atual"
    echo "  transform-date    Executa o pipeline completo para uma data específica (Disponivel apenas em debug)"
    echo "  load              Executa apenas o carregamento para a data atual"
    echo "  load-date         Executa apenas o carregamento para uma data específica"
    echo ""
    echo "Exemplos:"
    echo "  ./exec-pipeline.sh transform"
    echo "  ./exec-pipeline.sh transform-date 2024-05-23"
    echo "  ./exec-pipeline.sh load"
    echo "  ./exec-pipeline.sh load-date 2024-02-08"
}

# valida a data
validate_date() {
    if [[ ! $1 =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        echo "Erro: Formato de data inválido. Use YYYY-MM-DD"
        exit 1
    fi
}

# checa se o container esta rodando
check_container() {
    if ! docker ps | grep -q $CONTAINER_NAME; then
        echo "Erro: Container $CONTAINER_NAME não está em execução"
        exit 1
    fi
}

case "$1" in
    "transform")
        check_container
        docker exec -it $CONTAINER_NAME python /project/transform/run_transform.py
        ;;
    
    "transform-date")
        if [ -z "$2" ]; then
            echo "Erro: Data não especificada"
            show_help
            exit 1
        fi
        validate_date "$2"
        check_container
        docker exec -it $CONTAINER_NAME python /project/transform/run_transform.py "$2"
        ;;
    
    "load")
        check_container
        docker exec -it $CONTAINER_NAME python /project/load/run_load.py
        ;;
    
    "load-date")
        if [ -z "$2" ]; then
            echo "Erro: Data não especificada"
            show_help
            exit 1
        fi
        validate_date "$2"
        check_container
        docker exec -it $CONTAINER_NAME python /project/load/run_load.py "$2"
        ;;
    
    *)
        show_help
        exit 1
        ;;
esac 