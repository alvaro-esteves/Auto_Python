import pymysql
import pandas as pd

# CSLOG
con = pymysql.connect(
    host='192.168.200.35',
    port=33306,
    user='usr_alvaroesteves',
    password='AE@esteves0902',
    database='cslog_prod',
    charset='utf8mb4'
)

# DW
'''con = pymysql.connect(
    host='192.168.200.186',
    port=33306,
    user='usr_alvaroesteves',
    password='AE@esteves0902',
    database='dwbell',
    charset='utf8mb4'
)'''

#BPCOB
'''con = pymysql.connect(
    host='192.168.200.74',
    port=33306,
    user='usr_alvaroesteves',
    password='AE@esteves0902',
    database='bpcob',
    charset='utf8mb4'
)'''

#BPMESSAGE
'''con = pymysql.connect(
    host='192.168.200.34',
    port=33306,
    user='usr_alvaroesteves',
    password='AE@esteves0902',
    database='bpmessage',
    charset='utf8mb4'
)'''

# Query
query = """
SELECT
 CASE
    WHEN c.ID_CARTEIRA = 110 THEN 'AMIGAVEL'
    WHEN c.ID_CARTEIRA = 111 THEN 'CONTENCIOSO'
    WHEN c.ID_CARTEIRA = 112 THEN 'CARRINHO ABANDONADO'
    WHEN c.ID_CARTEIRA = 113 THEN 'PJ AMIGAVEL'
    WHEN c.ID_CARTEIRA = 114 THEN 'PJ CONTENCIOSO'
    WHEN c.ID_CARTEIRA = 115 THEN 'MVBANK'
    ELSE NULL
  END AS SEGMENTO,
	t.CPF,
	CONCAT(t.DDD, t.NUMERO) AS TELEFONE,
	CASE
		WHEN LENGTH(CONCAT(t.DDD, t.NUMERO)) = 10 THEN 'FIXO'
        WHEN LENGTH(CONCAT(t.DDD, t.NUMERO)) = 11 THEN 'CELULAR'
	END AS TIPO_TELEFONE,
	-- t.SCORE,
	tb.OBSERVACAO AS RAZAO_BLOQUEIO,
	tb.INCLUSAO AS DATA_BLOQUEIO,
	t.ORIGEM AS CODIGO_ORIGEM,
    t.NOME_CONTATO AS ORIGEM_TELEFONE,
    o.NOME AS ORIGEM_TELEFONE_CADASTRO
FROM 
	TELEFONE t
LEFT JOIN MARCACAO_TEL mt ON
	t.ID_TEL = mt.ID_TEL
LEFT JOIN MARCACAO_TEL_TIPO mtt ON 
	mt.ID_MARCACAO_TEL = mtt.ID_MARCACAO_TEL
LEFT JOIN TELEFONE_BLOQUEADO tb ON
t.DDD = tb.DDD AND t.NUMERO = tb.NUMERO
LEFT JOIN ORIGEM_CADASTRO o ON
	o.ID_ORIGEM_CADASTRO = t.ORIGEM
LEFT JOIN CONTRATO c ON 
	c.CPF = t.CPF
WHERE
	c.ID_CARTEIRA IN (110, 111, 113, 114, 115)
	AND tb.INCLUSAO IS NOT NULL
	AND tb.INCLUSAO BETWEEN '2025-09-01' AND '2025-11-25';
"""

# Carregar em DataFrame
df = pd.read_sql(query, con)

# Fechar conexão
con.close()

# Caminho do arquivo de saída
output_file = r"\\192.168.200.81\PlanejamentoPorto\1.Álvaro Esteves\Block_Porto(nov).csv"

# Salvar Excel (forçando engine)
# df.to_excel(output_file, index=False, engine="openpyxl")

# Salvar em CSV (UTF-8 para acentos)
df.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"✅ Arquivo salvo em: {output_file}")
