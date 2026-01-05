import pymysql
import pandas as pd


#BPCOB
con = pymysql.connect(
    host='192.168.200.74',
    port=33306,
    user='usr_alvaroesteves',
    password='AE@esteves0902',
    database='bpcob',
    charset='utf8mb4'
)


# Query
query = """
SELECT DISTINCT
    a.ContratoUnico AS CONTRATO_UNICO,
    a.cdContrato,
    a.CPF,
    a.Nome,
    (
        SELECT CONCAT(f.DDD, f.NUMERO)
        FROM cadfone f
        WHERE f.cdContrato = a.cdContrato
          AND f.NUMERO IS NOT NULL
          AND LENGTH(f.NUMERO) >= 8
        ORDER BY f.DtUltCPC DESC
        LIMIT 1
    ) AS TELEFONE,
    CASE
        WHEN a.cdBanco = 73 THEN 'E&F AMIGAVEL'
        WHEN a.cdBanco = 74 THEN 'E&F CONTENCIOSO SG'
        WHEN a.cdBanco = 75 THEN 'E&F CONTENCIOSO CG'
        ELSE NULL
    END AS SEGMENTO,
    b.diasAtraso AS ATRASO,
    a.VlContrato AS PRINCIPAL,
    a.UFOperacao AS UF,
    ca.cdAcordo as ID_ACORDO,
    ca.idAcordoApi as ID_ACORDO_API,
    ca.quantidadeParcelasApi as Quantidade_Parcelas,
    ca.dtVencimentoApi as Data_Vencimento
FROM cadcontrato a 
LEFT JOIN caddadosporto b ON b.cdContrato = a.cdContrato
LEFT JOIN cadeventoacionamento c ON c.cdEvento = a.cdUltEventoAcionamento
left join cadacordoporto ca on ca.cdContrato = a.cdContrato
WHERE a.cdBanco IN (73, 74, 75)
  AND a.Ativo = 1
  and ca.status IN (0, 2)
  AND DATE(ca.dtVencimentoApi) BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 2 DAY)
"""

# Carregar em DataFrame
df = pd.read_sql(query, con)

# Fechar conexão
con.close()

# Caminho do arquivo de saída
output_file = r"\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto\Preventivo D0, D-1, D-2 (BPCOB).xlsx"

# Salvar Excel (forçando engine)
df.to_excel(output_file, index=False, engine="openpyxl")

# Salvar em CSV (UTF-8 para acentos)
#df.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"✅ Arquivo salvo em: {output_file}")
