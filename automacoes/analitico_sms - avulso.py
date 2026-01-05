import pandas as pd
import csv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime, timedelta

# --- Configurações e Credenciais ---
senha_bancos = quote_plus('AE@esteves0902')  # <--- COLOQUE SUA SENHA REAL AQUI!
output_path = r'\\192.168.200.81\PlanejamentoPorto\1.Álvaro Esteves\Cartoraria_OUT.csv'  # <--- COLOQUE SEU CAMINHO DE SAÍDA REAL AQUI!

usuario_db = 'usr_alvaroesteves'
host_cslog = '192.168.200.35'
porta_cslog = '33306'
banco_cslog = 'cslog_prod'

host_bpmessage = '192.168.200.34'
porta_bpmessage = '33306'
banco_bpmessage = 'bpmessage'

host_bpcob = '192.168.200.74'
porta_bpcob = '33306'
banco_bpcob = 'bpcob'

# --- Conexões com os bancos de dados ---
try:
    cslog_engine = create_engine(f'mysql+pymysql://{usuario_db}:{senha_bancos}@{host_cslog}:{porta_cslog}/{banco_cslog}')
    bpmessage_engine = create_engine(f'mysql+pymysql://{usuario_db}:{senha_bancos}@{host_bpmessage}:{porta_bpmessage}/{banco_bpmessage}')
    bpcob_engine = create_engine(f'mysql+pymysql://{usuario_db}:{senha_bancos}@{host_bpcob}:{porta_bpcob}/{banco_bpcob}')
    print("✅ Conexões com os bancos de dados configuradas.")
except Exception as e:
    print(f"❌ Erro ao configurar as conexões com o banco de dados: {e}")
    exit()

# --- Datas fixas para a query_message ---
start_date_fixed = '2025-10-01 00:00:00'
end_date_fixed = '2025-10-30 23:59:59'

# --- Consultas SQL ---
query_message = text("""
SELECT
  l.idLot, 
  s.name AS Fornecedor,
  CONVERT(AES_DECRYPT(
    FROM_BASE64(m.cpfCnpjReceiver),
    FROM_BASE64('Y7uIKQIPAASNT4ddJNqHRiyFBFqHuOsSaADXZPd59D8='),
    FROM_BASE64('pT8Ik00gAHCayRXxscLBAw==')
  ) USING UTF8) AS CPF,
  CONCAT(
    CONVERT(AES_DECRYPT(
      FROM_BASE64(m.areaCode),
      FROM_BASE64('Y7uIKQIPAASNT4ddJNqHRiyFBFqHuOsSaADXZPd59D8='),
      FROM_BASE64('pT8Ik00gAHCayRXxscLBAw==')
    ) USING UTF8),
    CONVERT(AES_DECRYPT(
      FROM_BASE64(m.phone),
      FROM_BASE64('Y7uIKQIPAASNT4ddJNqHRiyFBFqHuOsSaADXZPd59D8='),
      FROM_BASE64('pT8Ik00gAHCayRXxscLBAw==')
    ) USING UTF8)
  ) AS Telefone_enviado,
  CONVERT(AES_DECRYPT(
    FROM_BASE64(m.text),
    FROM_BASE64('Y7uIKQIPAASNT4ddJNqHRiyFBFqHuOsSaADXZPd59D8='),
    FROM_BASE64('pT8Ik00gAHCayRXxscLBAw==')
  ) USING UTF8) AS Template_fraseologia,
  m.sentDate AS Data_Disparo,
  st.name AS Status_Envio,
  s.price as Valor_disparo,
  IFNULL(CONCAT(bk.friendlyName, ' ', sf.Name), bk.friendlyName) AS 'Sistema'
FROM message m
LEFT JOIN lot l ON l.idLot = m.idLot
LEFT JOIN servicesettings ss ON ss.idServiceSettings = l.idServiceSettings
LEFT JOIN bookbusiness bk ON bk.idbookbusiness = ss.idbookbusiness
LEFT JOIN service s ON ss.idService = s.idService
LEFT JOIN quotasettings q ON q.idQuotaSettings = l.idQuotaSettings
LEFT JOIN step sf ON q.idStep = sf.idStep 
LEFT JOIN statusmessage st ON m.idStatusMessage = st.idStatusMessage
WHERE l.startDate BETWEEN '2025-10-01 00:00:00' AND '2025-10-30 23:59:59'
  AND s.idServiceType = 1
  AND (
    (sf.idStep IN (103, 104, 105) AND bk.idBookBusiness IN (12, 174, 190, 193, 195, 196, 197))
    OR
    (sf.idStep IS NULL AND bk.idBookBusiness IN (12, 174, 190, 193, 195, 196, 197))
  );
""")
params_message = {
    'start_date': start_date_fixed,
    'end_date': end_date_fixed
}

query_cslog = """
SELECT
    a.CPF,
    CASE
        WHEN a.ID_CARTEIRA = 110 THEN 'AMIGAVEL'
        WHEN a.ID_CARTEIRA = 111 THEN 'CONTENCIOSO'
        WHEN a.ID_CARTEIRA = 112 THEN 'CARRINHO ABANDONADO'
        WHEN a.ID_CARTEIRA = 113 THEN 'PJ AMIGAVEL'
        WHEN a.ID_CARTEIRA = 114 THEN 'PJ CONTENCIOSO'
        WHEN a.ID_CARTEIRA = 115 THEN 'ULTRA'
        ELSE NULL
    END AS SEGMENTO,
    a.NUM_CONTRATO AS CONTRATO,
    a.ID_CONTR,
    a.NOME,
    DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) AS ATRASO,
    CASE
        WHEN a.ID_CARTEIRA = 110 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 61 THEN 'BK-3'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 07 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 30 THEN 'BK-1'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 31 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 60 THEN 'BK-2'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 61 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 90 THEN 'BK-3'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 91 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 120 THEN 'A2'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 121 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 180 THEN 'A3'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 181 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 360 THEN 'A4'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 361 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 540 THEN 'A5'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 541 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 720 THEN 'W1'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 721 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 1080 THEN 'W2'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 1081 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 1800 THEN 'W3'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 1801 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) <= 2190 THEN 'W4'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 2191 THEN 'W5'
        ELSE 'INDEFINIDO'
    END AS BUCKET,
    a.PRINCIPAL,
    CASE
    	WHEN e.ESTADO = 'RO' THEN 'RONDONIA'
    	WHEN e.ESTADO = 'AC' THEN 'ACRE'
    	WHEN e.ESTADO = 'AM' THEN 'AMAZONAS'
    	WHEN e.ESTADO = 'RR' THEN 'RORAIMA'
    	WHEN e.ESTADO = 'RO' THEN 'RONDONIA'
    	WHEN e.ESTADO = 'PA' THEN 'PARA'
    	WHEN e.ESTADO = 'AP' THEN 'AMAPA'
    	WHEN e.ESTADO = 'TO' THEN 'TOCANTINS'
    	WHEN e.ESTADO = 'MA' THEN 'MARANHAO'
    	WHEN e.ESTADO = 'PI' THEN 'PIAUI'
    	WHEN e.ESTADO = 'CE' THEN 'CEARA'
    	WHEN e.ESTADO = 'RN' THEN 'RIO GRANDE DO NORTE'
    	WHEN e.ESTADO = 'PB' THEN 'PARAIBA'
    	WHEN e.ESTADO = 'PE' THEN 'PERNAMBUCO'
    	WHEN e.ESTADO = 'AL' THEN 'ALAGOAS'
    	WHEN e.ESTADO = 'SE' THEN 'SERGIPE'
    	WHEN e.ESTADO = 'BA' THEN 'BAHIA'
    	WHEN e.ESTADO = 'MG' THEN 'MINAS GERAIS'
    	WHEN e.ESTADO = 'ES' THEN 'ESPIRITO SANTO'
    	WHEN e.ESTADO = 'RJ' THEN 'RIO DE JANEIRO'
    	WHEN e.ESTADO = 'SP' THEN 'SAO PAULO'
    	WHEN e.ESTADO = 'PR' THEN 'PARANA'
    	WHEN e.ESTADO = 'SC' THEN 'SANTA CATARINA'
    	WHEN e.ESTADO = 'RS' THEN 'RIO GRANDE DO SUL'
    	WHEN e.ESTADO = 'MS' THEN 'MATO GROSSO DO SUL'
    	WHEN e.ESTADO = 'MT' THEN 'MATO GROSSO'
    	WHEN e.ESTADO = 'GO' THEN 'GOIAS'
    	WHEN e.ESTADO = 'DF' THEN 'DISTRITO FEDERAL'
    	ELSE 'INDEFINIDO'
    END AS UF,
    c.CONTEUDO AS SALDO_ATT
FROM
    CONTRATO a
LEFT JOIN CONTR_PORTO_SEGURO b ON
    b.ID_CONTR = a.ID_CONTR
LEFT JOIN PORTO_SEGURO_INDICADOR c ON
    c.ID_CONTR = a.ID_CONTR AND c.INDICADOR = 'SALDOATT'
LEFT JOIN CARTEIRA d ON
    d.ID_CARTEIRA = a.ID_CARTEIRA
LEFT JOIN ENDERECO e ON
    e.ID_END = a.ID_END
WHERE
    a.ID_CARTEIRA IN (110, 111, 112, 113, 114, 115)
"""

query_ef_contracts = """
SELECT
    a.CPF,
    CASE
        WHEN a.cdBanco = 73 THEN 'E&F AMIGAVEL'
        WHEN a.cdBanco = 74 THEN 'E&F CONTENCIOSO SG'
        WHEN a.cdBanco = 75 THEN 'E&F CONTENCIOSO CG'
        ELSE NULL
    END AS SEGMENTO,
    a.ContratoUnico AS CONTRATO,
    a.cdContrato AS ID_CONTR,
    a.Nome AS NOME,
    b.diasAtraso AS ATRASO,
    CASE
        WHEN a.cdBanco = 73 AND b.diasAtraso >= 7 AND b.diasAtraso <= 15 THEN '1. 7 A 15'
        WHEN a.cdBanco = 73 AND b.diasAtraso >= 16 AND b.diasAtraso <= 30 THEN '2. 16 A 30'
        WHEN a.cdBanco = 73 AND b.diasAtraso >= 31 AND b.diasAtraso <= 45 THEN '3. 31 A 45'
        WHEN a.cdBanco = 73 AND b.diasAtraso >= 46 AND b.diasAtraso <= 65 THEN '4. 46 A 65'
        WHEN a.cdBanco = 73 AND (b.diasAtraso < 7 OR b.diasAtraso > 65) THEN '5. SEM FAIXA'
        WHEN a.cdBanco = 75 AND b.diasAtraso <= 66 THEN '1. ATE 66'
        WHEN a.cdBanco = 75 AND b.diasAtraso >= 66 AND b.diasAtraso <= 90 THEN '2. 66 A 90'
        WHEN a.cdBanco = 75 AND b.diasAtraso >= 91 AND b.diasAtraso <= 120 THEN '3. 91 A 120'
        WHEN a.cdBanco = 75 AND (b.diasAtraso < 121 OR b.diasAtraso > 180) THEN '4. 121 A 180'
        WHEN a.cdBanco = 75 AND b.diasAtraso >= 181 AND b.diasAtraso <= 360 THEN '5. 181 A 360'
        WHEN a.cdBanco = 75 AND b.diasAtraso >= 361 AND b.diasAtraso <= 540 THEN '6. 361 A 540'
        WHEN a.cdBanco = 75 AND b.diasAtraso >= 541 AND b.diasAtraso <= 720 THEN '7. 541 A 720'
        WHEN a.cdBanco = 75 AND b.diasAtraso >= 721 AND b.diasAtraso <= 1080 THEN '8. 721 A 1080'
        WHEN a.cdBanco = 75 AND b.diasAtraso >= 1081 AND b.diasAtraso <= 2190 THEN '9. 1081 A 2190'
        WHEN a.cdBanco = 75 AND b.diasAtraso > 2190 THEN '10. MAIOR 2190'
        WHEN a.cdBanco = 74 AND b.diasAtraso >= 91 AND b.diasAtraso <= 120 THEN '1. 91 A 120'
        WHEN a.cdBanco = 74 AND b.diasAtraso >= 121 AND b.diasAtraso <= 180 THEN '2. 121 A 180'
        WHEN a.cdBanco = 74 AND b.diasAtraso >= 181 AND b.diasAtraso <= 360 THEN '3. 181 A 360'
        WHEN a.cdBanco = 74 AND b.diasAtraso >= 361 AND b.diasAtraso <= 540 THEN '4. 361 A 540'
        WHEN a.cdBanco = 74 AND b.diasAtraso < 91 THEN '5. SEM FAIXA'
        WHEN a.cdBanco = 74 AND b.diasAtraso >= 541 AND b.diasAtraso <= 720 THEN '5. 541 A 720'
        WHEN a.cdBanco = 74 AND b.diasAtraso >= 721 AND b.diasAtraso <= 1080 THEN '6. 721 A 1080'
        WHEN a.cdBanco = 74 AND b.diasAtraso >= 1081 AND b.diasAtraso <= 2190 THEN '7.1081 A 2190'
        WHEN a.cdBanco = 74 AND b.diasAtraso > 2190 THEN '8. 2190'
        ELSE NULL
    END AS BUCKET,
    a.VlContrato AS PRINCIPAL,
    CASE
    	WHEN a.UFOperacao = 'RO' THEN 'RONDONIA'
    	WHEN a.UFOperacao = 'AC' THEN 'ACRE'
    	WHEN a.UFOperacao = 'AM' THEN 'AMAZONAS'
    	WHEN a.UFOperacao = 'RR' THEN 'RORAIMA'
    	WHEN a.UFOperacao = 'RO' THEN 'RONDONIA'
    	WHEN a.UFOperacao = 'PA' THEN 'PARA'
    	WHEN a.UFOperacao = 'AP' THEN 'AMAPA'
    	WHEN a.UFOperacao = 'TO' THEN 'TOCANTINS'
    	WHEN a.UFOperacao = 'MA' THEN 'MARANHAO'
    	WHEN a.UFOperacao = 'PI' THEN 'PIAUI'
    	WHEN a.UFOperacao = 'CE' THEN 'CEARA'
    	WHEN a.UFOperacao = 'RN' THEN 'RIO GRANDE DO NORTE'
    	WHEN a.UFOperacao = 'PB' THEN 'PARAIBA'
    	WHEN a.UFOperacao = 'PE' THEN 'PERNAMBUCO'
    	WHEN a.UFOperacao = 'AL' THEN 'ALAGOAS'
    	WHEN a.UFOperacao = 'SE' THEN 'SERGIPE'
    	WHEN a.UFOperacao = 'BA' THEN 'BAHIA'
    	WHEN a.UFOperacao = 'MG' THEN 'MINAS GERAIS'
    	WHEN a.UFOperacao = 'ES' THEN 'ESPIRITO SANTO'
    	WHEN a.UFOperacao = 'RJ' THEN 'RIO DE JANEIRO'
    	WHEN a.UFOperacao = 'SP' THEN 'SAO PAULO'
    	WHEN a.UFOperacao = 'PR' THEN 'PARANA'
    	WHEN a.UFOperacao = 'SC' THEN 'SANTA CATARINA'
    	WHEN a.UFOperacao = 'RS' THEN 'RIO GRANDE DO SUL'
    	WHEN a.UFOperacao = 'MS' THEN 'MATO GROSSO DO SUL'
    	WHEN a.UFOperacao = 'MT' THEN 'MATO GROSSO'
    	WHEN a.UFOperacao = 'GO' THEN 'GOIAS'
    	WHEN a.UFOperacao = 'DF' THEN 'DISTRITO FEDERAL'
    	ELSE 'INDEFINIDO'
    END AS UF,
    a.VlContrato AS SALDO_ATT
FROM
    cadcontrato a
LEFT JOIN caddadosporto b ON b.cdContrato = a.cdContrato
LEFT JOIN cadeventoacionamento c ON c.cdEvento = a.cdUltEventoAcionamento
WHERE
    a.cdBanco IN (73, 74, 75)
"""

# --- Executando as consultas ---
df1, df2, df3 = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

print("--- Carregando dados das bases de dados ---")
try:
    df1 = pd.read_sql(query_message, bpmessage_engine, params=params_message)
    print("✅ Dados de bpmessage carregados com sucesso.")
    print(f"   -> df1 (bpmessage) possui {len(df1)} linhas.")
    if 'CPF' not in df1.columns:
        print("❌ Erro: Coluna 'CPF' não encontrada em df1 (bpmessage) após o carregamento. Verifique a query.")
        df1 = pd.DataFrame(columns=['CPF'])
except Exception as e:
    print(f"❌ Erro ao carregar dados de bpmessage: {e}")

try:
    df2 = pd.read_sql(query_cslog, cslog_engine)
    print("✅ Dados de cslog_prod carregados com sucesso.")
    print(f"   -> df2 (cslog_prod) possui {len(df2)} linhas.")
    if 'CPF' not in df2.columns:
        print("❌ Erro: Coluna 'CPF' não encontrada em df2 (cslog_prod) após o carregamento. Verifique a query.")
        df2 = pd.DataFrame(columns=['CPF'])
    print("   -> Colunas presentes em df2 (cslog_prod):", df2.columns.tolist())
except Exception as e:
    print(f"❌ Erro ao carregar dados de cslog_prod: {e}")

try:
    df3 = pd.read_sql(query_ef_contracts, bpcob_engine)
    print("✅ Dados de bpcob carregados com sucesso.")
    print(f"   -> df3 (bpcob) possui {len(df3)} linhas.")
    if 'CPF' not in df3.columns:
        print("❌ Erro: Coluna 'CPF' não encontrada em df3 (bpcob) após o carregamento. Verifique a query.")
        df3 = pd.DataFrame(columns=['CPF'])
    print("   -> Colunas presentes em df3 (bpcob):", df3.columns.tolist())
except Exception as e:
    print(f"❌ Erro ao carregar dados de bpcob: {e}")
print("--- Fim do carregamento de dados ---")

# --- Normalização e Deduplicação de CPF ---
print("\n--- Processando Normalização e Deduplicação de CPF ---")
dataframes_para_normalizar = []
if not df1.empty: dataframes_para_normalizar.append((df1, "df1 (bpmessage)"))
if not df2.empty: dataframes_para_normalizar.append((df2, "df2 (cslog_prod)"))
if not df3.empty: dataframes_para_normalizar.append((df3, "df3 (bpcob)"))

for df, df_name in dataframes_para_normalizar:
    if 'CPF' in df.columns:
        len_before_norm = len(df)
        df['CPF'] = df['CPF'].astype(str).str.replace(r'\D', '', regex=True).str.strip()
        df.dropna(subset=['CPF'], inplace=True)
        if len(df) < len_before_norm:
            print(f"   -> {df_name}: {len_before_norm - len(df)} linhas removidas por CPF vazio/inválido. Linhas restantes: {len(df)}")
        else:
            print(f"   -> {df_name}: Nenhuma linha removida por CPF vazio/inválido. Total de linhas: {len(df)}")
    else:
        print(f"Aviso: {df_name} não possui a coluna 'CPF' e a normalização foi ignorada.")

# Dedup para garantir 1:1 merge no CPF para os dataframes de look-up
if not df2.empty and 'CPF' in df2.columns:
    num_rows_before_dedup_df2 = len(df2)
    df2.drop_duplicates(subset=['CPF'], keep='first', inplace=True)
    if len(df2) < num_rows_before_dedup_df2:
        print(f"   -> df2 (cslog_prod) deduplicado: {num_rows_before_dedup_df2} linhas para {len(df2)}.")

if not df3.empty and 'CPF' in df3.columns:
    num_rows_before_dedup_df3 = len(df3)
    df3.drop_duplicates(subset=['CPF'], keep='first', inplace=True)
    if len(df3) < num_rows_before_dedup_df3:
        print(f"   -> df3 (bpcob) deduplicado: {num_rows_before_dedup_df3} linhas para {len(df3)}.")
print("--- Fim da Normalização e Deduplicação ---")

# --- Lógica de Merge e Preenchimento ---
COLUNAS_PARA_PREENCHER = [
    'SEGMENTO', 'CONTRATO', 'ID_CONTR', 'NOME', 'ATRASO',
    'BUCKET', 'PRINCIPAL', 'UF', 'SALDO_ATT'
]

if df1.empty:
    print("\n❗ DataFrame base (bpmessage) está vazio. Não é possível realizar merges.")
    df_final = pd.DataFrame(columns=df1.columns.tolist() + COLUNAS_PARA_PREENCHER)
else:
    initial_bpmessage_rows = len(df1)
    print(f"\nContagem inicial de linhas de bpmessage (df1): {initial_bpmessage_rows}")

    # Renomeia colunas em df2
    if not df2.empty:
        rename_map_df2 = {col: f"{col}_cslog" for col in COLUNAS_PARA_PREENCHER if col in df2.columns and col != 'CPF'}
        df2.rename(columns=rename_map_df2, inplace=True)
        print(f"   -> df2 (cslog_prod) colunas renomeadas para: {list(rename_map_df2.values())}")
        
    # Renomeia colunas em df3
    if not df3.empty:
        rename_map_df3 = {col: f"{col}_bpcob" for col in COLUNAS_PARA_PREENCHER if col in df3.columns and col != 'CPF'}
        df3.rename(columns=rename_map_df3, inplace=True)
        print(f"   -> df3 (bpcob) colunas renomeadas para: {list(rename_map_df3.values())}")

    # Merge df1 + df2
    df_final = pd.merge(df1, df2, on='CPF', how='left')
    print(f"ℹ️ Merge inicial de bpmessage com cslog_prod concluído. Linhas após merge: {len(df_final)}")

    # Merge com df3
    if not df3.empty:
        df_final = pd.merge(df_final, df3, on='CPF', how='left')
        print(f"ℹ️ Merge final com bpcob concluído. Linhas após merge: {len(df_final)}")
    else:
        print("❗ DataFrame de bpcob está vazio. O segundo merge não foi realizado.")

    # Checagem de contagem de linhas
    if len(df_final) != initial_bpmessage_rows:
        print(f"⚠️ Aviso: O número de linhas do DataFrame final ({len(df_final)}) é diferente do DataFrame original de bpmessage ({initial_bpmessage_rows}).")
    else:
        print(f"✅ O número de linhas do DataFrame final ({len(df_final)}) respeita a quantidade de linhas da consulta bpmessage.")

    print("\n--- Iniciando Preenchimento de Colunas Prioritárias (cslog_prod > bpcob) ---")
    for col in COLUNAS_PARA_PREENCHER:
        col_cslog_suffixed = f"{col}_cslog"
        col_bpcob_suffixed = f"{col}_bpcob"

        df_final[col] = pd.NA

        if col_cslog_suffixed in df_final.columns:
            df_final[col] = df_final[col_cslog_suffixed]
            print(f"   -> Preenchendo '{col}' com dados de '{col_cslog_suffixed}'. (count non-null: {df_final[col].count()})")
        else:
            print(f"   -> Coluna '{col_cslog_suffixed}' não encontrada em df_final. Não foi possível priorizar cslog_prod para '{col}'.")

        if col_bpcob_suffixed in df_final.columns:
            count_before_fill = df_final[col].count()
            df_final[col] = df_final[col].fillna(df_final[col_bpcob_suffixed])
            count_after_fill = df_final[col].count()
            if count_after_fill > count_before_fill:
                print(f"   -> Complementando '{col}' com dados de '{col_bpcob_suffixed}'. (+{count_after_fill - count_before_fill} novos valores)")
            else:
                print(f"   -> '{col}' não precisou de complemento de '{col_bpcob_suffixed}' ou 'bpcob' não tinha dados.")
        else:
            print(f"   -> Coluna '{col_bpcob_suffixed}' não encontrada em df_final. Não foi possível complementar '{col}' com bpcob.")

    print("--- Fim do Preenchimento de Colunas Prioritárias ---")

    # Remove colunas temporárias com sufixos
    cols_to_drop_suffixes = [c for c in df_final.columns if c.endswith('_cslog') or c.endswith('_bpcob')]
    df_final.drop(columns=cols_to_drop_suffixes, inplace=True, errors='ignore')

    # Limpa quebras de linha em colunas de texto que podem quebrar o layout do CSV
    text_cols = ['Template_fraseologia', 'Sistema', 'Fornecedor', 'Status_Envio']
    for col in text_cols:
        if col in df_final.columns:
            df_final[col] = (
                df_final[col]
                .astype(str)
                .str.replace('\r\n', ' ', regex=False)
                .str.replace('\n', ' ', regex=False)
                .str.replace('\r', ' ', regex=False)
            )

    # Reordenar colunas
    ordered_cols = df1.columns.tolist()
    for col in COLUNAS_PARA_PREENCHER:
        if col not in ordered_cols:
            ordered_cols.append(col)
    final_ordered_cols_actual = [col for col in ordered_cols if col in df_final.columns]

    try:
        df_final = df_final[final_ordered_cols_actual]
    except KeyError as e:
        print(f"Aviso: Não foi possível reordenar todas as colunas. Coluna(s) faltando: {e}")

# --- Salvando o arquivo final ---
if not df_final.empty:
    try:
        df_final.to_csv(
            output_path,
            index=False,
            sep=';',              # separador mais amigável para Excel PT-BR
            encoding='utf-8-sig', # BOM para Excel
            quoting=csv.QUOTE_MINIMAL
        )
        print("\n✅ Arquivo final 'Cartoraria.csv' salvo com sucesso em:")
        print(output_path)
    except Exception as e:
        print(f"❌ Erro ao salvar o arquivo: {e}")
else:
    print("❗ O DataFrame final está vazio. Nenhum arquivo foi gerado.")

# --- Fechar conexões do banco de dados ---
try:
    bpmessage_engine.dispose()
    cslog_engine.dispose()
    bpcob_engine.dispose()
    print("✅ Conexões com o banco de dados fechadas.")
except Exception as e:
    print(f"❌ Erro ao fechar conexões com o banco de dados: {e}")
