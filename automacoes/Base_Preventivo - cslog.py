import pymysql
import pandas as pd


#CSLOG
con = pymysql.connect(
    host='192.168.200.35',
    port=33306,
    user='usr_alvaroesteves',
    password='AE@esteves0902',
    database='cslog_prod',
    charset='utf8mb4'
)


# Query
query = """
SELECT DISTINCT
    a.ID_CONTR,
    a.NUM_CONTRATO,
    a.CPF,
    a.NOME,
    (
        SELECT CONCAT(t2.DDD, t2.NUMERO)
        FROM TELEFONE t2
        LEFT JOIN TELEFONE_COMPL tc2 ON t2.ID_TEL = tc2.ID_TEL
        WHERE t2.CPF = a.CPF
        ORDER BY tc2.DATA_ULT_CPC DESC
        LIMIT 1
    ) AS TELEFONE,
    CASE
        WHEN a.ID_CARTEIRA = 110 THEN 'AMIGAVEL'
        WHEN a.ID_CARTEIRA = 111 THEN 'CONTENCIOSO'
        WHEN a.ID_CARTEIRA = 112 THEN 'CARRINHO ABANDONADO'
        WHEN a.ID_CARTEIRA = 113 THEN 'PJ AMIGAVEL'
        WHEN a.ID_CARTEIRA = 114 THEN 'PJ CONTENCIOSO'
        WHEN a.ID_CARTEIRA = 115 THEN 'ULTRA'
        ELSE NULL
        END AS SEGMENTO,
    b.ID_ACORDO,
    b.NUM_PARCELAS,
    CASE
        WHEN b.STATUS = 0 THEN 'Valido'
        WHEN b.STATUS = 1 THEN 'Pre-Acordo'
        WHEN b.STATUS = 2 THEN 'Refeito'
        WHEN b.STATUS = 3 THEN 'Proposta de Acordo'
        WHEN b.STATUS = 4 THEN 'Cancelado'
        WHEN b.STATUS = 5 THEN 'Invalido'
        WHEN b.STATUS = 6 THEN 'Pre-acordo Invalido'
        WHEN b.STATUS = 7 THEN 'Liquidado'
        WHEN b.STATUS = 8 THEN 'Refeito Liquidado'
        ELSE 'INDEFINIDO'
    END AS STATUS_ACORDO,
    b.VENC_PARC_ABERTO
FROM CONTRATO a
LEFT JOIN ACORDO b ON a.ID_CONTR = b.ID_CONTR
WHERE 
    a.ID_CARTEIRA IN (110, 111, 112, 113, 114, 115)
    AND a.STATUS IN (4, 5)
    AND DATE(b.VENC_PARC_ABERTO) BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 2 DAY)
    AND b.STATUS IN (0, 1, 2, 3, 4, 5, 6, 7, 8); 
"""

# Carregar em DataFrame
df = pd.read_sql(query, con)

# Fechar conexão
con.close()

# Caminho do arquivo de saída
output_file = r"\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto\Preventivo D0, D-1, D-2 (CSLOG).xlsx"

# Salvar Excel (forçando engine)
df.to_excel(output_file, index=False, engine="openpyxl")

# Salvar em CSV (UTF-8 para acentos)
#df.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"✅ Arquivo salvo em: {output_file}")
