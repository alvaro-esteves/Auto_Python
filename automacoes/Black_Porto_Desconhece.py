import pymysql
from datetime import date, timedelta, datetime

# 🔹 Configurações iniciais
caminho = r'\\192.168.158.19\exclusions'
modo = 'A'      # A = ADICIONAR / D = DELETAR
tipo = 'V'      # A = ACCOUNT / V = TELEFONE
dias = 0        # QUANTIDADE DE DIAS
data_inicio = date.today().strftime("%b %d %Y")
data_fim = (date.today() + timedelta(dias)).strftime("%b %d %Y")
hora_inicio = '04:00AM'
hora_fim = '11:59PM'
bloqueio = 'ALL'
hora_atual = datetime.now().strftime("%H%M")

nome_arquivo = rf"{caminho}\Blacklist_Porto_Desconhece.act"

# 🔹 Conexão com o banco de dados
con = pymysql.connect(
    host='192.168.200.35',
    port=33306,
    user='usr_alvaroesteves',
    password='AE@esteves0902',
    database='cslog_prod',
    charset='utf8mb4'
)

query = """
SELECT DISTINCT
    CONCAT(d.DDD, d.NUMERO) AS TELEFONE
FROM HISTORICO a
LEFT JOIN CODIGO b 
    ON b.COD = a.CODIGO
LEFT JOIN CONTRATO c 
    ON c.ID_CONTR = a.ID_CONTR
LEFT JOIN TELEFONE d 
    ON d.ID_TEL = a.ID_TEL  
WHERE 
    DATE(a.`DATA`) BETWEEN DATE_SUB(CURDATE(), INTERVAL 2 DAY) AND CURDATE()
    AND b.HISTORICO IN (251, 4179)
    AND c.ID_CARTEIRA IN (110, 111, 113, 114, 115)
    AND c.STATUS NOT IN (8);
"""

# 🔹 Execução da consulta
with con.cursor() as cursor:
    cursor.execute(query)
    resultados = cursor.fetchall()

# 🔹 Geração do arquivo com base nos dados SQL
with open(nome_arquivo, 'w', encoding='utf-8') as arquivo_saida:
    for row in resultados:
        telefone = row[0]  # ✅ pega só o valor da tupla
        linha_formatada = f"{modo}|{tipo}|{data_inicio} {hora_inicio}|{data_fim} {hora_fim}|{telefone}|{bloqueio}"
        arquivo_saida.write(linha_formatada + '\n')

con.close()
print(f"✅ Arquivo gerado: {nome_arquivo}")
