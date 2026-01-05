import os
from datetime import datetime
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine

# ==========================
# CONFIGURAÇÃO
# ==========================
DB_HOST = "192.168.200.35"          # ex.: "192.168.x.y"
DB_PORT = 33306                  # porta MySQL
DB_NAME = "cslog_prod"           # ex.: "cslog_prod"
DB_USER = "usr_alvaroesteves"
DB_PASS_RAW = "AE@esteves0902"
DB_PASS = quote_plus(DB_PASS_RAW)

# Pasta de rede (UNC). Ex.: r"\\servidor\compartilhamento\magalu"
NETWORK_FOLDER = r"\\192.168.200.81\PlanejamentoPorto\1.Carteiras\1.Magalu\Preventivo-Quebras"

# CSV com separador; troque para ";" se preferir abrir direto no Excel PT-BR
CSV_SEP = ";"
CSV_ENCODING = "utf-8-sig"

# Nome de arquivos (AAAA-MM-DD)
DATA_TAG = datetime.now().strftime("%Y-%m-%d")
ARQ_PREVENTIVO = os.path.join(NETWORK_FOLDER, f"PREVENTIVO_MAGALU-{DATA_TAG}.csv")
ARQ_QUEBRAS   = os.path.join(NETWORK_FOLDER, f"QUEBRAS_MAGALU-{DATA_TAG}.csv")

# ==========================
# SQLs
# ==========================
SQL_PREVENTIVO = """
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
        WHEN a.ID_CARTEIRA = 140 THEN 'MAGALU - TELECOBRANCA'
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
    a.PRINCIPAL,
    b.VENC_PARC_ABERTO
FROM CONTRATO a
LEFT JOIN ACORDO b ON a.ID_CONTR = b.ID_CONTR
WHERE 
    a.ID_CARTEIRA IN (140)
    -- a.STATUS IN (4, 5) -- descomente se necessário
    AND b.VENC_PARC_ABERTO >= CURDATE()         -- hoje 00:00:00
    AND b.VENC_PARC_ABERTO <  CURDATE() + INTERVAL 1 DAY
    AND b.STATUS IN (0, 1, 2, 3);
"""

SQL_QUEBRAS = """
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
        WHEN a.ID_CARTEIRA = 140 THEN 'MAGALU - TELECOBRANCA'
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
    a.PRINCIPAL,
    b.VENC_PARC_ABERTO
FROM CONTRATO a
LEFT JOIN ACORDO b ON a.ID_CONTR = b.ID_CONTR
WHERE 
    a.ID_CARTEIRA IN (140)
    AND a.STATUS <> 8
    AND b.VENC_PARC_ABERTO >= CURDATE() - INTERVAL 1 DAY  -- ontem 00:00:00
    AND b.VENC_PARC_ABERTO <  CURDATE()                   -- ontem 23:59:59
    AND b.STATUS IN (0, 1, 2, 3, 4, 5, 6, 7, 8);
"""

# ==========================
# Execução
# ==========================
def main():
    # Garante que a pasta de rede exista (se permissões permitirem)
    os.makedirs(NETWORK_FOLDER, exist_ok=True)

    print("🔌 Conectando ao MySQL...")
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    print("🏁 Executando PREVENTIVO MAGALU (hoje)...")
    df_prev = pd.read_sql(SQL_PREVENTIVO, engine)
    print(f"   → Linhas: {len(df_prev)}")

    print("🏁 Executando QUEBRAS MAGALU (ontem)...")
    df_qbr = pd.read_sql(SQL_QUEBRAS, engine)
    print(f"   → Linhas: {len(df_qbr)}")

    # Salva CSVs (sempre com cabeçalho; cria arquivo mesmo vazio)
    print("💾 Salvando CSVs na pasta de rede...")
    df_prev.to_csv(ARQ_PREVENTIVO, index=False, encoding=CSV_ENCODING, sep=CSV_SEP)
    df_qbr.to_csv(ARQ_QUEBRAS,   index=False, encoding=CSV_ENCODING, sep=CSV_SEP)

    print("✅ Arquivos gerados:")
    print(f"   - {ARQ_PREVENTIVO}")
    print(f"   - {ARQ_QUEBRAS}")

    # Encerra conexão
    engine.dispose()
    print("🔒 Conexão encerrada.")

if __name__ == "__main__":
    main()
