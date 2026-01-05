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
HOST = "192.168.200.74"
PORTA = "33306"
BANCO = "bpcob"

# Caminho da pasta de rede onde o arquivo será salvo
PASTA_REDE = r"\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto"

# Cria o nome do arquivo com a data atual
DATA_HOJE = datetime.now().strftime("%Y%m%d")
NOME_ARQUIVO = f"BASE_WHATS_E&F_{DATA_HOJE}.csv"
CAMINHO_ARQUIVO = os.path.join(PASTA_REDE, NOME_ARQUIVO)

# ================================
# CONSULTA SQL
# ================================
QUERY = """
SELECT
    a.cdContrato AS CD_CONTRATO,
    a.Contrato,
    a.CPF,
    a.Nome,
    a.cdBanco,
    CASE
        WHEN a.cdBanco = 73 THEN 'Porto E&F Amigavel'
        WHEN a.cdBanco = 74 THEN 'Porto E&F Contencioso SG' 
        WHEN a.cdBanco = 75 THEN 'Porto E&F Contencioso CG'
    END AS Carteira,
    CASE
        WHEN a.cdBanco = 73 AND atraso.DIAS_ATRASO BETWEEN 7 AND 15 THEN '1. 7 A 15'
        WHEN a.cdBanco = 73 AND atraso.DIAS_ATRASO BETWEEN 16 AND 30 THEN '2. 16 A 30'
        WHEN a.cdBanco = 73 AND atraso.DIAS_ATRASO BETWEEN 31 AND 45 THEN '3. 31 A 45'
        WHEN a.cdBanco = 73 AND atraso.DIAS_ATRASO BETWEEN 46 AND 65 THEN '4. 46 A 65'
        WHEN a.cdBanco = 73 AND (atraso.DIAS_ATRASO < 7 OR atraso.DIAS_ATRASO > 65) THEN '5. SEM FAIXA'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO <= 66 THEN '1. ATE 66'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO BETWEEN 67 AND 90 THEN '2. 66 A 90'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO BETWEEN 91 AND 120 THEN '3. 91 A 120'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO BETWEEN 121 AND 180 THEN '4. 121 A 180'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO BETWEEN 181 AND 360 THEN '5. 181 A 360'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO BETWEEN 361 AND 540 THEN '6. 361 A 540'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO BETWEEN 541 AND 720 THEN '7. 541 A 720'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO BETWEEN 721 AND 1080 THEN '8. 721 A 1080'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO BETWEEN 1081 AND 2190 THEN '9. 1081 A 2190'
        WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO > 2190 THEN '10. MAIOR 2190'
        WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO BETWEEN 91 AND 120 THEN '1. 91 A 120'
        WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO BETWEEN 121 AND 180 THEN '2. 121 A 180'
        WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO BETWEEN 181 AND 360 THEN '3. 181 A 360'
        WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO BETWEEN 361 AND 540 THEN '4. 361 A 540'
        WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO < 91 THEN '5. SEM FAIXA'
        WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO BETWEEN 541 AND 720 THEN '5. 541 A 720'
        WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO BETWEEN 721 AND 1080 THEN '6. 721 A 1080'
        WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO BETWEEN 1081 AND 2190 THEN '7. 1081 A 2190'
        WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO > 2190 THEN '8. 2190'
        ELSE NULL
    END AS FAIXA_ATRASO,
    a.VlContrato as Principal,
    CONCAT(cd.DDD, cd.NUMERO) AS TELEFONE,
    case 
    	when cd.Confirmado = 0 then 'NÃO CONFIRMADO'
    	when cd.Confirmado = 1 then 'CONFIRMADO'
    end as TIPO_TELEFONE
    from
    cadcontrato a
	left join cadfone cd ON 
	cd.cdContrato = a.cdContrato
	LEFT JOIN (
    SELECT 
        cdContrato,
        COALESCE(DATEDIFF(CURDATE(), MIN(dataVencimento)),0) AS DIAS_ATRASO
    FROM cadprestacaoporto
    WHERE dataPagamento = '0001-01-01'
    GROUP BY cdContrato
     ) atraso ON atraso.cdContrato = a.cdContrato
	where
	a.Ativo = 1
	and cd.Ativo = 1 
	and cd.cdTipoFone = 3
	and a.cdBanco in (73, 74, 75)
	and cd.Bom in (1)
	and cd.Confirmado in (0, 1)
ORDER BY
                IF(cd.DtUltCPC IS NOT null, 1, 2) asc,
                cd.Confirmado desc,
                cd.pontuacao DESC;
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
