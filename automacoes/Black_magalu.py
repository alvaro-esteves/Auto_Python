import pymysql
from datetime import date, timedelta, datetime

# 🔹 Configurações iniciais
caminho = r'\\192.168.158.84\\exclusions'
modo = 'A'      # A = ADICIONAR / D = DELETAR
tipo = 'A'      # A = ACCOUNT / V = TELEFONE
dias = 0        # QUANTIDADE DE DIAS
data_inicio = date.today().strftime("%b %d %Y")
data_fim = (date.today() + timedelta(dias)).strftime("%b %d %Y")
hora_inicio = '04:00AM'
hora_fim = '11:59PM'
bloqueio = 'ALL'
hora_atual = datetime.now().strftime("%H%M")

nome_arquivo = rf"{caminho}\BLACK_MAGALU_MANUAL_0.act"

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
WITH historico_hoje AS (
    SELECT 
        a.ID_CONTR,
        a.CODIGO
    FROM HISTORICO a
    INNER JOIN CODIGO b ON b.COD = a.CODIGO
    WHERE 
        b.HISTORICO IN (9, 18, 546, 2644)
        AND DATE(a.DATA) = CURDATE()
)
SELECT DISTINCT
    c.ID_CONTR,
    d.DDD,
    d.NUMERO,
    tc.DATA_ULT_CPC
FROM historico_hoje hh
INNER JOIN CONTRATO c ON c.ID_CONTR = hh.ID_CONTR
INNER JOIN TELEFONE d ON d.CPF = c.CPF
INNER JOIN TELEFONE_COMPL tc ON tc.ID_TEL = d.ID_TEL
WHERE
    DATE(tc.DATA_ULT_CPC) = CURDATE()
    AND c.ID_CARTEIRA IN (140)
    AND c.STATUS NOT IN (8);
"""

# 🔹 Execução da consulta
with con.cursor() as cursor:
    cursor.execute(query)
    resultados = cursor.fetchall()

# 🔹 Geração do arquivo com base nos dados SQL
with open(nome_arquivo, 'w', encoding='utf-8') as arquivo_saida:
    for row in resultados:
        id_contr, ddd, numero, data_ult_cpc = row
        linha_formatada = f"{modo}|{tipo}|{data_inicio} {hora_inicio}|{data_fim} {hora_fim}|{id_contr}|{bloqueio}"
        arquivo_saida.write(linha_formatada + '\n')

con.close()
print(f"✅ Arquivo gerado: {nome_arquivo}")
