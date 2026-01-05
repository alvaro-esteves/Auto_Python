import os
import re
from datetime import datetime
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text

# =========================
# CONFIGURAÇÕES
# =========================
DB_USER = "usr_alvaroesteves"
DB_PASS = "AE@esteves0902"
DB_HOST = "192.168.200.35"
DB_PORT = "33306"
DB_NAME = "cslog_prod"

# Escapar credenciais (senha/usuário com caracteres especiais)
DB_USER_ESC = urllib.parse.quote_plus(DB_USER)
DB_PASS_ESC = urllib.parse.quote_plus(DB_PASS)

# Pasta de saída (use raw string no Windows)
PASTA_SAIDA = r"\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto"

# Datas para nomes de arquivos e CORINGA_4
agora = datetime.now()
data_formatada = agora.strftime("%d%m%y")       # para nome de arquivo
DATA_CORINGA_4 = agora.strftime("%d/%m/%Y")     # para campo CORINGA_4

# Arquivo 1 (já existente)
ARQ1_NOME = f"5.1.Porto Seguro Recado_{data_formatada}.csv"

# Arquivo 2 (NOVO)
ARQ2_NOME = f"34.1.Porto Seguro Negociador 7 a 15_{data_formatada}.csv"

TELEFONE_RECADO_FIXO = "08006426613"
WHATSAPP_RECADO_VAZIO = "4133189719"

# =========================
# SQL 1: Base Recado (07–90 dias)
# =========================
SQL_TELEFONES = text("""
SELECT
    a.NOME AS nome,
    a.CPF AS cpf,
    CASE WHEN a.ID_CARTEIRA = 110 THEN 'Porto Seguro Amigavel' END AS carteira,
    a.NUM_CONTRATO AS ContratoUnico,
    a.ID_CONTR AS cdContrato,
    e.ESTADO AS UF,
    CONCAT(t.DDD, t.NUMERO) AS telefone,
    co.DATA_ULT_CPC AS data_ult_cpc
FROM CONTRATO a
LEFT JOIN ENDERECO e         ON e.ID_END   = a.ID_END
JOIN TELEFONE t              ON t.CPF      = a.CPF
LEFT JOIN TELEFONE_COMPL co  ON co.ID_TEL  = t.ID_TEL
WHERE
    a.ID_CARTEIRA IN (110)
    AND a.STATUS = 0
    AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 07 AND 90
    AND LENGTH(CONCAT(t.DDD, t.NUMERO)) > 10
    AND NOT EXISTS (
        SELECT 1
        FROM TELEFONE_BLOQUEADO tb
        WHERE tb.CPF = t.CPF
          AND tb.DDD = t.DDD
          AND tb.NUMERO = t.NUMERO
    )
ORDER BY a.NUM_CONTRATO, co.DATA_ULT_CPC DESC
""")

# =========================
# SQL EXTRA (OPCIONAL): adicionar no Arquivo 1 os contratos MVBANK
# ✅ Pode comentar este bloco inteiro no futuro sem dar erro,
#    pois o código usa globals().get("SQL_RECADO_MVBANK") antes de executar.
# =========================
SQL_RECADO_MVBANK = text("""
SELECT DISTINCT
  a.NOME AS nome,
  a.CPF  AS cpf,
  CASE WHEN a.ID_CARTEIRA = 115 THEN 'Porto Seguro MVBANK' END AS carteira,
  a.NUM_CONTRATO AS ContratoUnico,
  a.ID_CONTR     AS cdContrato,
  GROUP_CONCAT(DISTINCT CONCAT(t.DDD, t.NUMERO) ORDER BY co.DATA_ULT_CPC DESC SEPARATOR ', ') AS telefones,
  e.ESTADO AS UF
FROM CONTRATO a
LEFT JOIN ENDERECO e         ON e.ID_END    = a.ID_END
JOIN TELEFONE t              ON t.CPF       = a.CPF
LEFT JOIN TELEFONE_COMPL co  ON co.ID_TEL   = t.ID_TEL
WHERE
  a.ID_CARTEIRA IN (115)
  AND a.STATUS = 0
  AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 07 AND 90
  AND LENGTH(CONCAT(t.DDD, t.NUMERO)) > 10
  AND NOT EXISTS (
    SELECT 1 FROM TELEFONE_BLOQUEADO tb
    WHERE tb.CPF = t.CPF AND tb.DDD = t.DDD AND tb.NUMERO = t.NUMERO
  )
GROUP BY nome, cpf, carteira, UF, ContratoUnico, cdContrato
ORDER BY cdContrato
""")

# =========================
# SQL 2: Negociador 7 a 15 (06–30 dias)
# =========================
SQL_NEGOCIADOR = text("""
SELECT DISTINCT
  a.NUM_CONTRATO AS CONTRATO,
  a.NOME,
  a.ID_CONTR AS CD_CONTRATO,
  a.CPF,
  e.ESTADO AS UF,
  MAX(REPLACE(REPLACE(FORMAT(ps.CONTEUDO, 2), ',', ''), '.', ',')) AS VALOR_EM_ATRASO,
  DATE_FORMAT(a.DATA_IMPORTACAO, '%d/%m/%Y') AS DATA_DE_IMPORTAÇÃO,
  DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) AS DIAS_DE_ATRASO,
  GROUP_CONCAT(DISTINCT CONCAT(t.DDD, t.NUMERO) ORDER BY co.DATA_ULT_CPC DESC SEPARATOR ', ') AS telefones
FROM CONTRATO a
LEFT JOIN ENDERECO e        ON e.ID_END   = a.ID_END
JOIN TELEFONE t             ON t.CPF      = a.CPF
LEFT JOIN TELEFONE_COMPL co ON co.ID_TEL  = t.ID_TEL
LEFT JOIN PORTO_SEGURO_INDICADOR ps ON ps.ID_CONTR = a.ID_CONTR AND ps.INDICADOR = 'SALDOATT'
WHERE
  a.ID_CARTEIRA IN (110)
  AND a.STATUS = 0
  AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 01 AND 30
  AND LENGTH(CONCAT(t.DDD, t.NUMERO)) > 10
  AND NOT EXISTS (
    SELECT 1 FROM TELEFONE_BLOQUEADO tb
    WHERE tb.CPF = t.CPF AND tb.DDD = t.DDD AND tb.NUMERO = t.NUMERO
  )
GROUP BY NOME, CPF, UF, CD_CONTRATO, CONTRATO
ORDER BY CD_CONTRATO
""")

# =========================
# SQL 3: E-mails por CPF (para CORINGA_3)
# =========================
SQL_EMAILS = text("""
SELECT
    e.CPF,
    MAX(e.EMAIL) AS EMAIL
FROM EMAIL e
JOIN CONTRATO a 
    ON e.CPF = a.CPF
WHERE
    a.ID_CARTEIRA IN (110)
    AND a.STATUS = 0
    AND e.STATUS = 0
GROUP BY
    e.CPF
""")

# =========================
# Funções utilitárias
# =========================
def limpar_telefone(raw: str) -> str:
    if raw is None:
        return ""
    return re.sub(r"\D+", "", str(raw))

def unicos_preservando_ordem(seq):
    seen = set()
    out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def montar_colunas_telefones(telefones, prefixo="dddtelefone ", qtd=10, zero_pad=False):
    d = {}
    for i in range(1, qtd + 1):
        idx = i - 1
        valor = telefones[idx] if idx < len(telefones) else ""
        if zero_pad:
            d[f"{prefixo}{i:02d}"] = valor
        else:
            d[f"{prefixo}{i}"] = valor
    return d

def extrair_lista_telefones(s, max_n=10):
    """
    Recebe 'tel1, tel2, ...' e retorna lista limpa (somente dígitos),
    única e preservando ordem, com tamanho max_n.
    """
    if pd.isna(s) or not str(s).strip():
        return []
    brut = [limpar_telefone(x) for x in str(s).split(",")]
    brut = [x.strip() for x in brut if x and x.strip()]
    brut = [x for x in brut if len(x) >= 11]
    return unicos_preservando_ordem(brut)[:max_n]

# =========================
# Execução
# =========================
def main():
    # Conexão
    engine = create_engine(
        f"mysql+pymysql://{DB_USER_ESC}:{DB_PASS_ESC}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        pool_recycle=3600,
        pool_pre_ping=True,
    )

    # --------- Arquivo 1: Recado (já existente) ---------
    df_raw = pd.read_sql(SQL_TELEFONES, engine)

    if not df_raw.empty:
        df_raw["telefone"] = df_raw["telefone"].apply(limpar_telefone)
        df_raw = df_raw[df_raw["telefone"].str.len() >= 11].copy()
        df_raw["ord_data"] = pd.to_datetime(df_raw["data_ult_cpc"], errors="coerce")
        df_raw = df_raw.sort_values(["cdContrato", "ord_data"], ascending=[True, False])

        agrupamento = []
        for _, grupo in df_raw.groupby("ContratoUnico", sort=False):
            tels = unicos_preservando_ordem(grupo["telefone"].tolist())[:10]
            base = grupo.iloc[0][[
                "nome", "cpf", "carteira", "UF", "cdContrato", "ContratoUnico"
            ]].to_dict()
            base["telefone recado"] = TELEFONE_RECADO_FIXO
            base["whatsapp recado"] = WHATSAPP_RECADO_VAZIO
            base.update(montar_colunas_telefones(tels, prefixo="dddtelefone ", qtd=10, zero_pad=False))
            agrupamento.append(base)

        df_final = pd.DataFrame(agrupamento)

        colunas1 = [
            "nome",
            "cpf",
            "carteira",
            "telefone recado",
            "whatsapp recado",
            "dddtelefone 1",
            "dddtelefone 2",
            "dddtelefone 3",
            "dddtelefone 4",
            "dddtelefone 5",
            "dddtelefone 6",
            "dddtelefone 7",
            "dddtelefone 8",
            "dddtelefone 9",
            "dddtelefone 10",
            "UF",
            "cdContrato",
            "ContratoUnico",
        ]

        # ---- EXTRA OPCIONAL: adicionar MVBANK no mesmo Arquivo 1 ----
        sql_extra = globals().get("SQL_RECADO_MVBANK", None)
        if sql_extra is not None:
            try:
                df_mv = pd.read_sql(sql_extra, engine)

                if not df_mv.empty:
                    linhas_mv = []
                    df_mv["tels_list"] = df_mv["telefones"].apply(lambda x: extrair_lista_telefones(x, max_n=10))

                    for _, r in df_mv.iterrows():
                        base = {
                            "nome": r.get("nome", ""),
                            "cpf": r.get("cpf", ""),
                            "carteira": r.get("carteira", ""),
                            "UF": r.get("UF", ""),
                            "cdContrato": r.get("cdContrato", ""),
                            "ContratoUnico": r.get("ContratoUnico", ""),
                            "telefone recado": TELEFONE_RECADO_FIXO,
                            "whatsapp recado": WHATSAPP_RECADO_VAZIO,
                        }
                        base.update(montar_colunas_telefones(r["tels_list"], prefixo="dddtelefone ", qtd=10, zero_pad=False))
                        linhas_mv.append(base)

                    df_mv_final = pd.DataFrame(linhas_mv)

                    # garantir colunas e ordem
                    for c in colunas1:
                        if c not in df_mv_final.columns:
                            df_mv_final[c] = ""
                    df_mv_final = df_mv_final[colunas1]

                    # concatenar e deduplicar (se houver choque de contrato)
                    df_final = pd.concat([df_final, df_mv_final], ignore_index=True)
                    df_final = df_final.drop_duplicates(subset=["ContratoUnico"], keep="first").copy()
            except Exception as e:
                print(f"⚠️ Extra MVBANK: falhou ao executar/mesclar (seguindo sem). Erro: {e}")

        # garantir colunas no df_final e ordenar
        for c in colunas1:
            if c not in df_final.columns:
                df_final[c] = ""
        df_final = df_final[colunas1]

        os.makedirs(PASTA_SAIDA, exist_ok=True)
        caminho_csv1 = os.path.join(PASTA_SAIDA, ARQ1_NOME)
        df_final.to_csv(caminho_csv1, index=False, encoding="utf-8-sig", sep=";")
        print(f"✅ Arquivo 1 gerado com {len(df_final)} linhas.")
        print(f"📄 {caminho_csv1}")
    else:
        print("⚠️ Arquivo 1: Nenhum registro retornado pela consulta.")

    # --------- Arquivo 2: Negociador 7 a 15 ---------
    df_neg = pd.read_sql(SQL_NEGOCIADOR, engine)

    if not df_neg.empty:
        # 🔹 Buscar e-mails por CPF e mesclar na base de negociador
        df_email = pd.read_sql(SQL_EMAILS, engine)
        df_email = df_email.drop_duplicates(subset=["CPF"])
        df_neg = df_neg.merge(df_email, on="CPF", how="left")

        tels_list = df_neg["telefones"].apply(lambda s: extrair_lista_telefones(s, max_n=15))

        # Base final, começando pelas colunas do SELECT + EMAIL
        df_out = df_neg[[
            "CD_CONTRATO", "NOME", "CONTRATO", "CPF", "UF",
            "VALOR_EM_ATRASO", "DATA_DE_IMPORTAÇÃO", "DIAS_DE_ATRASO",
            "EMAIL"
        ]].copy()

        branco_cols = [
            "MACRORREGIAO", "SITUACAO_DO_CONTRATO", "VALOR_PARCELA",
            "VALOR_DE_RISCO", "PRODUTO", "DATA_DA_ULTIMA_REMESSA",
            "ATRASO_CONGELADO", "OFERECE_QUITAÇÃO?", "FAIXA_ATRASO",
            "PREVENTIVO", "QUEBRA", "PARCELA_ATRASO", "PLANO",
            "VENCIMENTO_PARCELA", "CORINGA_2", "CORINGA_3", "CORINGA_4",
            "CORINGA_5", "CORINGA_6", "CORINGA_7", "CORINGA_8", "CORINGA_9",
            "CORINGA_10", "PLACA", "Marca"
        ]
        for c in branco_cols:
            if c not in df_out.columns:
                df_out[c] = ""

        df_out["CORINGA_1"] = TELEFONE_RECADO_FIXO
        df_out["CORINGA_2"] = WHATSAPP_RECADO_VAZIO
        df_out["CORINGA_3"] = df_out["EMAIL"].fillna("")
        df_out["CORINGA_4"] = DATA_CORINGA_4
        df_out["CORINGA_6"] = df_out["VALOR_EM_ATRASO"]

        tels_cols = {f"TELEFONE_{i:02d}": [] for i in range(1, 16)}
        for lista in tels_list:
            for i in range(1, 16):
                idx = i - 1
                tels_cols[f"TELEFONE_{i:02d}"].append(lista[idx] if idx < len(lista) else "")

        for col, valores in tels_cols.items():
            df_out[col] = valores

        colunas2 = [
            "CD_CONTRATO",
            "NOME",
            "CONTRATO",
            "CPF",
            "UF",
            "MACRORREGIAO",
            "SITUACAO_DO_CONTRATO",
            "VALOR_PARCELA",
            "VALOR_EM_ATRASO",
            "VALOR_DE_RISCO",
            "PRODUTO",
            "DATA_DE_IMPORTAÇÃO",
            "DATA_DA_ULTIMA_REMESSA",
            "DIAS_DE_ATRASO",
            "ATRASO_CONGELADO",
            "OFERECE_QUITAÇÃO?",
            "FAIXA_ATRASO",
            "PREVENTIVO",
            "QUEBRA",
            "PARCELA_ATRASO",
            "PLANO",
            "VENCIMENTO_PARCELA",
            "CORINGA_1",
            "CORINGA_2",
            "CORINGA_3",
            "CORINGA_4",
            "CORINGA_5",
            "CORINGA_6",
            "CORINGA_7",
            "CORINGA_8",
            "CORINGA_9",
            "CORINGA_10",
            "PLACA",
            "Marca",
            "TELEFONE_01",
            "TELEFONE_02",
            "TELEFONE_03",
            "TELEFONE_04",
            "TELEFONE_05",
            "TELEFONE_06",
            "TELEFONE_07",
            "TELEFONE_08",
            "TELEFONE_09",
            "TELEFONE_10",
            "TELEFONE_11",
            "TELEFONE_12",
            "TELEFONE_13",
            "TELEFONE_14",
            "TELEFONE_15",
        ]
        for c in colunas2:
            if c not in df_out.columns:
                df_out[c] = ""

        if "EMAIL" in df_out.columns and "EMAIL" not in colunas2:
            df_out.drop(columns=["EMAIL"], inplace=True)

        df_out = df_out[colunas2]

        os.makedirs(PASTA_SAIDA, exist_ok=True)
        caminho_csv2 = os.path.join(PASTA_SAIDA, ARQ2_NOME)
        df_out.to_csv(caminho_csv2, index=False, encoding="utf-8-sig", sep=";")
        print(f"✅ Arquivo 2 gerado com {len(df_out)} linhas.")
        print(f"📄 {caminho_csv2}")
    else:
        print("⚠️ Arquivo 2: Nenhum registro retornado pela consulta.")

if __name__ == "__main__":
    main()
