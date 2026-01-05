# -*- coding: utf-8 -*-
import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import date

# ======================================
# CONFIGURAÇÕES DE CONEXÃO
# ======================================
USUARIO = "usr_alvaroesteves"
SENHA = quote_plus("AE@esteves0902")  # use quote_plus se tiver caracteres especiais
HOST = "192.168.200.74"                
PORTA = "33306"                  
BANCO = "bpcob"                 

# Exemplo para MySQL:
URL_CONEXAO = f"mysql+pymysql://{USUARIO}:{SENHA}@{HOST}:{PORTA}/{BANCO}?connect_timeout=180"
engine = create_engine(URL_CONEXAO)

# ======================================
# QUERY SQL
# ======================================
sql = """
SELECT 
    a.CPF,
    a.NOME,
    CONCAT(d.DDD, d.NUMERO) AS telefone,
    a.UFOperacao AS UF,
    a.cdContrato,
    a.ContratoUnico
FROM cadcontrato a
LEFT JOIN cadfone d 
    ON d.cdContrato = a.cdContrato  
WHERE
    a.cdBanco IN (74, 75)
    and d.Ativo = 1
    AND a.Ativo = 1;
"""

# ======================================
# CARREGAR DADOS
# ======================================
print("Lendo dados do banco...")
df = pd.read_sql(sql, engine)

# Garante que telefone é string
df["telefone"] = df["telefone"].astype(str)

# ======================================
# FUNÇÃO PARA PIVOTAR TELEFONES (1 LINHA POR CPF/CONTRATO)
# ======================================
def distribuir_telefones(grupo: pd.DataFrame) -> pd.Series:
    """
    Recebe um grupo (mesmo CPF/cdContrato/ContratoUnico/UF/NOME)
    e devolve uma linha com até 10 telefones em colunas separadas.
    """
    # Telefones únicos, não nulos
    telefones = (
        grupo["telefone"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .tolist()
    )

    # Limita a 10 telefones
    telefones = telefones[:10]

    # Pega referência da primeira linha para dados fixos
    first = grupo.iloc[0]

    # Monta dicionário base
    dados = {
        "cpf": first["CPF"],
        "nome": first["NOME"],
        "empresa": "Bellinati",
        "carteira": "Porto Seguro",
        "telefone recado": "08009000102",
        "UF": first["UF"],
        "cdContrato": first["cdContrato"],
        "ContratoUnico": first["ContratoUnico"],
    }

    # Preenche colunas dddtelefone 1 ... 10
    for i in range(1, 11):
        col = f"dddtelefone {i}"
        if i <= len(telefones):
            dados[col] = telefones[i - 1]
        else:
            dados[col] = None

    return pd.Series(dados)

# ======================================
# AGRUPAR E GERAR LAYOUT FINAL
# ======================================
print("Distribuindo telefones em colunas...")

# Define as chaves de agrupamento (uma linha por contrato/CPF)
# Se quiser uma linha por CPF, troque por ["CPF", "UF", "NOME"]
chaves = ["CPF", "cdContrato", "ContratoUnico", "UF", "NOME"]

df_final = (
    df.groupby(chaves, dropna=False)
      .apply(distribuir_telefones)
      .reset_index(drop=True)
)

# ======================================
# ADICIONAR COLUNA COM A DATA DO DIA
# ======================================
data_hoje = date.today()                 # ex.: 2025-11-10
data_hoje_str = data_hoje.isoformat()    # "2025-11-10"

df_final["data_geracao"] = data_hoje_str

# ======================================
# REORDENAR COLUNAS CONFORME LAYOUT + DATA
# ======================================
colunas1 = [
    "cpf","nome","empresa","carteira","telefone recado",
    "dddtelefone 1","dddtelefone 2","dddtelefone 3","dddtelefone 4","dddtelefone 5",
    "dddtelefone 6","dddtelefone 7","dddtelefone 8","dddtelefone 9","dddtelefone 10",
    "UF","cdContrato","ContratoUnico",
]

df_final = df_final[colunas1]

# ======================================
# GERAR NOME DO ARQUIVO COM A DATA
# ======================================
# Formato AAAAMMDD para {data_formatada}
data_formatada = data_hoje.strftime("%d%m%Y")  # ex.: 12112025

nome_arquivo_csv = f"54.1.PORTO SEGURO E&F LOCALIZADOR_{data_formatada}.csv"

# Pasta onde o arquivo será salvo
PASTA_SAIDA = r"\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto"  # ajuste aqui

# Monta caminho completo
CAMINHO_SAIDA_CSV = os.path.join(PASTA_SAIDA, nome_arquivo_csv)

# ======================================
# SALVAR RESULTADO
# ======================================
print(f"Salvando CSV em: {CAMINHO_SAIDA_CSV}")
df_final.to_csv(CAMINHO_SAIDA_CSV, sep=";", index=False, encoding="utf-8-sig")

print("Concluído com sucesso!")
