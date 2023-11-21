import psycopg2
import pandas as pd
import json

def read_log(entradaLog):
    # Lê o arquivo de log e remove o caractere '>' de cada linha
    df = pd.read_csv(entradaLog, sep='<', names=['LOG'], engine='python') 
    dados = [x for x in df.LOG.str.strip('>')]
    
    # Inicializa listas para armazenar diferentes tipos de informações durante o processamento do log
    lista_ckpt, lista_commit, operations_list, starts_list = [], [], [], []
    
    # Inicializa a variável para rastrear a posição do último checkpoint no log
    index_ckpt = -1  
    
    # Inicializa uma lista para armazenar as operações que precisam ser desfeitas durante o processo de leitura do log
    undo_operations = []

    for n, x in enumerate(dados):
        if x.startswith('START CKPT'):
            # Atualiza a posição do último checkpoint
            index_ckpt = n
        elif x.startswith('END CKPT'):
            # Verifica operações a serem desfeitas entre o último checkpoint e o fim do log
            undo_operations = check_ckpts(dados, lista_ckpt, lista_commit, index_ckpt, n)

        if x.startswith('start'):
            if n > index_ckpt:
                # Adiciona transações iniciadas após o último checkpoint
                starts_list.append(x.lstrip('start '))
        elif x.startswith('commit'):
            if n > index_ckpt:
                # Adiciona transações confirmadas após o último checkpoint
                lista_commit.append(x.lstrip('commit '))
            else:
                # Adiciona transações confirmadas antes do último checkpoint
                lista_commit.append(x.lstrip('commit '))

    # Cria uma lista de transações válidas para verificação posterior
    transaction_list = check_transactions((lista_ckpt + starts_list), lista_commit)

    # Cria uma lista de operações para processamento posterior
    for x in dados:
        if x.startswith('T'):
            operation = list(map(str, x.replace('(', '').replace(')', '').split(',')))
            if operation[0] in transaction_list:
                operations_list.append(operation)

    return operations_list, undo_operations

def check_ckpts(data, ckpt, commit, start, end):
    # Inicializa uma lista para armazenar operações a serem desfeitas
    undo_operations = []
    for x in data[start+1:end]:
        if x.startswith('T'):
            operation = list(map(str, x.replace('(', '').replace(')', '').split(',')))
            if operation[0] in commit and operation[0] not in ckpt:
                # Adiciona operações a serem desfeitas
                undo_operations.append(operation)
    return undo_operations

def check_transactions(check, commit):
    # Inicializa uma lista para armazenar transações a serem verificadas
    verificar_transactions = []
    for x in check:
        if x not in commit:
            # Imprime transações que realizaram UNDO
            print(f'Transação {x} realizou UNDO.')
            verificar_transactions.append(x)
    return verificar_transactions

# ----------------------------------------------- Código inicial -------------------------------------

entradaLog = './teste'
metadata_path = './metadado.json'
conn = None

try:
    # Conexão ao banco de dados
    conn = psycopg2.connect(host='localhost', port='5432', database='postgres', user='postgres', password='daniel')
    cursor = conn.cursor()

    # Carrega o banco de dados com a tabela
    cursor.execute('drop table if exists vintage_log')
    cursor.execute('create table vintage_log (id integer, A integer, B integer)')

    # Insere dados iniciais na tabela
    df = pd.read_json(metadata_path)['INITIAL']
    for x in range(len(df['A'])):
        cursor.execute('insert into vintage_log values (%s, %s, %s)', (x + 1, df['A'][x], df['B'][x]))

    conn.commit()

    # Lê o log, verifica transações e operações a serem desfeitas
    undo_operations = read_log(entradaLog)

    # Verifica valores e atualiza o banco de dados
    for op in undo_operations:
        op = op[0]
        if len(op) >= 4:  
            cursor.execute(f'select {op[2]} from vintage_log where id = {op[1]}')
            result = cursor.fetchone()
            if result is not None and int(result[0]) != int(op[3]):  
                cursor.execute(f'update vintage_log set {op[2]} = {op[3]} where id = {op[1]}')

    conn.commit()

    # Imprime os metadados
    cursor.execute('select * from vintage_log order by id')
    row = cursor.fetchall()
    json = {"TABLE": {}}
    json["TABLE"]["A"] = [x[1] for x in row]
    json["TABLE"]["B"] = [x[2] for x in row]
    print('\nDados após UNDO:\n', json)
    cursor.close()

except psycopg2.DatabaseError as error:
    print()
    print("Error while connecting to PostgreSQL\n", error)

finally:
    if conn is not None:
        conn.close()
        print()
        print("Conexão com o PostgreSQL fechada\n")
