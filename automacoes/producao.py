import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime
import os

# ================================
# CONFIGURAÇÕES
# ================================
USUARIO = "usr_alvaroesteves"
SENHA = quote_plus("AE@esteves0902")
HOST = "192.168.200.35"
PORTA = "33306"
BANCO = "cslog_prod"

# Caminho da pasta de rede onde o arquivo será salvo
PASTA_REDE = r"\\192.168.200.81\PortoSeguro-Gestao\Contencioso\Relatorio - Producao diaria"

# Cria o nome do arquivo com a data atual
DATA_HOJE = datetime.now().strftime("%Y%m%d")
NOME_ARQUIVO = f"PRODUCAO-CONTENCIOSO_{DATA_HOJE}.csv"
CAMINHO_ARQUIVO = os.path.join(PASTA_REDE, NOME_ARQUIVO)

# ================================
# CONSULTA SQL
# ================================
QUERY = """
SELECT DISTINCT
 CASE
        WHEN a.ID_CARTEIRA = 110 THEN 'AMIGAVEL'
        WHEN a.ID_CARTEIRA = 111 THEN 'CONTENCIOSO'
        WHEN a.ID_CARTEIRA = 112 THEN 'CARRINHO ABANDONADO'
        WHEN a.ID_CARTEIRA = 113 THEN 'PJ AMIGAVEL'
        WHEN a.ID_CARTEIRA = 114 THEN 'PJ CONTENCIOSO'
        WHEN a.ID_CARTEIRA = 115 THEN 'ULTRA'
        ELSE NULL
        END AS SEGMENTO,
    a.ID_CONTR,
    a.NUM_CONTRATO,
    a.CPF,
    a.NOME,
    b.`DATA` as DATA_ACORDO,
    DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) AS ATRASO,
    b.VENC_PARC_ABERTO,
    b.ID_ACORDO,
    b.NUM_PARCELAS,
    b.TOTAL,
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
    END AS STATUS_ACORDO
FROM CONTRATO a
LEFT JOIN ACORDO b ON a.ID_CONTR = b.ID_CONTR
WHERE 
    a.ID_CARTEIRA IN (111)
    AND a.STATUS IN (4, 5)
    AND DATE(b.DATA) = CURDATE()
    AND b.STATUS IN (0, 3, 4, 7);
"""

# ================================
# EXECUÇÃO
# ================================
def main():
    # Cria conexão com o banco
    print("🔌 Conectando ao banco de dados...")
    engine = create_engine(f"mysql+pymysql://{USUARIO}:{SENHA}@{HOST}:{PORTA}/{BANCO}?connect_timeout=180")

    # Executa a query e carrega no DataFrame
    print("📊 Executando consulta SQL...")
    df = pd.read_sql(QUERY, con=engine)

    # Cria pasta se não existir
    os.makedirs(PASTA_REDE, exist_ok=True)

    # Salva o resultado em CSV (ou XLSX se preferir)
    print(f"💾 Salvando arquivo em: {CAMINHO_ARQUIVO}")
    df.to_csv(CAMINHO_ARQUIVO, sep=';', index=False, encoding='utf-8-sig')

    print("✅ Arquivo gerado com sucesso!")

if __name__ == "__main__":
    main()
