import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os

# Configurações de conexão com o banco de dados
usuario = 'usr_alvaroesteves'
senha = quote_plus('AE@esteves0902')
host = '192.168.200.35'
porta = '33306'
banco = 'cslog_prod'

# Caminho para salvar os arquivos Excel
caminho_saida = r"\\192.168.200.81\\PlanejamentoPorto\\1.Carteiras\\1.Magalu\\Vend"

# Consulta SQL
consulta_sql = """
SELECT 
    a.NOME,
    a.CPF,
    CONCAT(b.DDD, b.NUMERO) AS NUMERO,
    a.STATUS,
    CASE
        WHEN a.STATUS = 0 THEN 'EM COBRANCA'
        WHEN a.STATUS = 4 THEN 'EM ACORDO'
        WHEN a.STATUS = 5 THEN 'ACORDO EM ATRASO'
        WHEN a.STATUS = 8 THEN 'DEVOLVIDO'
    END AS STATUS_CONTRATO,
    b.STATUS AS STATUS_TELEFONE,
    CASE
        WHEN b.STATUS = 0 THEN '1.CONFIRMADO'
        WHEN b.STATUS = 1 THEN '2.NÃO CONFIRMADO'
        WHEN b.STATUS = 2 THEN '3.INVÁLIDO'
        WHEN b.STATUS IS NULL THEN '4.SEM TELEFONE'
    END AS CLASSIFICACAO_STATUS,
    DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) AS ATRASO,
    CASE
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 6 AND 30 THEN '1. 6 a 30 Dias'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 31 THEN 'OVER 30'
        ELSE 'VERIFICAR'
    END AS CLASSIFICACAO_ATRASO
FROM CONTRATO a
JOIN TELEFONE b 
  ON b.ID_TEL = (
      SELECT FA.ID_TEL
      FROM TELEFONE FA
      INNER JOIN TELEFONE_COMPL CO ON CO.ID_TEL = FA.ID_TEL
      WHERE FA.CPF = a.CPF
        AND FA.STATUS IN (0) -- só confirmado 
        AND LENGTH(CONCAT(FA.DDD, FA.NUMERO)) = 11
        AND LEFT(FA.NUMERO, 1) >= 6 -- apenas celulares
        AND CO.DATA_ULT_CPC IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM TELEFONE_BLOQUEADO tb
            WHERE tb.CPF = FA.CPF
              AND tb.DDD = FA.DDD
              AND tb.NUMERO = FA.NUMERO
        )
      ORDER BY CO.DATA_ULT_CPC DESC
      LIMIT 1
  )
LEFT JOIN CARTEIRA c ON c.ID_CARTEIRA = a.ID_CARTEIRA
WHERE
    a.ID_CARTEIRA = 140
    AND a.STATUS IN (0)
    AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 6 AND 30;
"""

# Criação da engine de conexão
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}:{porta}/{banco}")

# Executar a consulta e salvar em um DataFrame
df = pd.read_sql(consulta_sql, con=engine)

# Caminho do arquivo Excel completo
arquivo_excel = os.path.join(caminho_saida, "magalu.xlsx")

# Salvar o DataFrame em um arquivo Excel
df.to_excel(arquivo_excel, index=False)

# Função para dividir o Excel em múltiplos arquivos com até 100 linhas e apenas 3 colunas
def dividir_excel(Base_vend, pasta_saida):
    df = pd.read_excel(Base_vend, engine='openpyxl')
    df = df[['NOME', 'NUMERO']]  # Seleciona apenas as colunas desejadas
    df['EMAIL'] = ''  # Adiciona a coluna EMAIL vazia

     # Renomear colunas para minúsculas
    df.columns = ['nome', 'numero', 'email']


    max_linhas = 100
    num_arquivos = (len(df) // max_linhas) + (1 if len(df) % max_linhas != 0 else 0)

    for i in range(num_arquivos):
        df_dividido = df.iloc[i * max_linhas : (i + 1) * max_linhas]
        nome_arquivo = f"Base_Vend_{i+1}.xlsx"
        caminho_completo = os.path.join(pasta_saida, nome_arquivo)
        df_dividido.to_excel(caminho_completo, index=False)

    print(f"Divisão concluída! {num_arquivos} arquivos foram gerados na pasta: {pasta_saida}")

# Executar a divisão
dividir_excel(arquivo_excel, caminho_saida)

#chave do banco salva na variavel
#db_url = "mysql://usr_alvaroesteves:AE%40esteves0902@192.168.200.35:33306/cslog_prod?connect_timeout=180"