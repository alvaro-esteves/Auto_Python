# -*- coding: utf-8 -*-

from sqlalchemy import create_engine, text
import pandas as pd
import os
import logging
import logging.handlers
import shutil
from datetime import datetime

# =========================
# PARÂMETROS GERAIS
# =========================


# =========================
# LOGGING
# =========================
def configurar_logging(nome_arquivo_log: str):
    logger = logging.getLogger()
    # evita handlers duplicados ao reexecutar o script
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.handlers.TimedRotatingFileHandler(
        nome_arquivo_log, when='midnight', interval=1, backupCount=7, encoding='utf-8'
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.propagate = False
    return logger

logger = configurar_logging('log_contratos_carrinhoabandonado.log')

# =========================
# CAMINHOS / SAÍDA
# =========================
REDE_DIR = r'\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto'
LOCAL_TMP_DIR = os.path.join(os.getcwd(), '_tmp_export')
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
os.makedirs(REDE_DIR, exist_ok=True)

data_tag = datetime.now().strftime('%d%m%Y')
saida_local = os.path.join(LOCAL_TMP_DIR, f'BASE_CARRINHO_ABANDONADO_{data_tag}.csv')
saida_rede  = os.path.join(REDE_DIR,      f'BASE_CARRINHO_ABANDONADO_{data_tag}.csv')

# =========================
# CONEXÃO
# =========================
# Use o driver pymysql!
CHAVE_CSLOG = (
    'mysql+pymysql://usr_adriano:j!Z%1vO%7vTcOhhBI@192.168.200.35:33306/'
    'cslog_prod?connect_timeout=180'
)
engine = create_engine(
    CHAVE_CSLOG,
    pool_pre_ping=True,
    pool_recycle=180
)

# =========================
# CONSULTA SQL
# =========================
# Versão com "pré-agrupamento" via tabela derivada (compatível inclusive com MySQL 5.7).
SQL_SELECAO = text("""
WITH base AS (
    SELECT
        CASE
            WHEN a.ID_CARTEIRA = 110 THEN 'AMIGAVEL'
            WHEN a.ID_CARTEIRA = 111 THEN 'CONTENCIOSO'
            WHEN a.ID_CARTEIRA = 112 THEN 'CARRINHO ABANDONADO'
            WHEN a.ID_CARTEIRA = 113 THEN 'PJ AMIGAVEL'
            WHEN a.ID_CARTEIRA = 114 THEN 'PJ CONTENCIOSO'
            WHEN a.ID_CARTEIRA = 115 THEN 'ULTRA'
            ELSE NULL
        END AS SEGMENTO,
        a.BLOQUEIO,
        a.NUM_CONTRATO,
        a.ID_CONTR,
        a.NOME,
        a.CPF,
        DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) AS ATRASO,
        CASE
            WHEN a.ID_CARTEIRA = 110 AND DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 91 THEN 'BK-3'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 7 AND 30 THEN 'BK-1'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 31 AND 60 THEN 'BK-2'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 61 AND 90 THEN 'BK-3'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 91 AND 120 THEN 'A2'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 121 AND 180 THEN 'A3'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 181 AND 360 THEN 'A4'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 361 AND 540 THEN 'A5'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 541 AND 720 THEN 'W1'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 721 AND 1080 THEN 'W2'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 1081 AND 1800 THEN 'W3'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 1801 AND 2190 THEN 'W4'
            WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 2191 THEN 'W5'
            ELSE 'INDEFINIDO'
        END AS BUCKET,
        a.PRINCIPAL,
        CASE 
            WHEN a.STATUS = 0 THEN 'EM COBRANCA'
            WHEN a.STATUS = 4 THEN 'EM ACORDO'
            WHEN a.STATUS = 5 THEN 'ACORDO EM ATRASO'
            WHEN a.STATUS = 8 THEN 'DEVOLVIDO'
        END AS STATUS_CONTRATO,
        CASE 
            WHEN DATE(a.DATA_IMPORTACAO) = CURDATE() THEN 1
            ELSE NULL
        END AS NOVO_CONTRATO,
        e.ESTADO AS UF,
        CONCAT(t.DDD, t.NUMERO) AS TELEFONE,
        t_CPC.DATA_ULT_CPC,
        ROW_NUMBER() OVER (
            PARTITION BY a.ID_CONTR
            ORDER BY t_CPC.DATA_ULT_CPC DESC
        ) AS rn
    FROM CONTRATO a
    LEFT JOIN CONTR_PORTO_SEGURO b 
        ON b.ID_CONTR = a.ID_CONTR
    LEFT JOIN PORTO_SEGURO_INDICADOR c 
        ON c.ID_CONTR = a.ID_CONTR 
    LEFT JOIN CARTEIRA d 
        ON d.ID_CARTEIRA = a.ID_CARTEIRA
    LEFT JOIN ENDERECO e 
        ON e.ID_END = a.ID_END
    LEFT JOIN TELEFONE t 
        ON t.CPF = a.CPF
    LEFT JOIN TELEFONE_COMPL t_CPC 
        ON t_CPC.ID_TEL = t.ID_TEL 
    WHERE 
        a.ID_CARTEIRA IN (112)
        AND a.STATUS NOT IN (8)
)
SELECT
    SEGMENTO,
    BLOQUEIO,
    NUM_CONTRATO,
    ID_CONTR,
    NOME,
    CPF,
    ATRASO,
    BUCKET,
    PRINCIPAL,
    STATUS_CONTRATO,
    NOVO_CONTRATO,
    UF,
    TELEFONE
FROM base
WHERE rn = 1
ORDER BY ID_CONTR;
""")

def main():
    try:
        logger.info(f'Iniciando extração: ')
        with engine.begin() as conn:
            # (opcional) limite de execução por sessão (se suportado)
            try:
                conn.execute(text("SET SESSION MAX_EXECUTION_TIME = 120000;"))
            except Exception:
                # ignora se o servidor não suportar esta variável
                pass

            result = conn.execute(SQL_SELECAO).mappings().all()

        logger.info(f'Linhas retornadas: {len(result)}')

        if not result:
            logger.warning('Consulta sem resultados. Nada a salvar.')
            return

        df = pd.DataFrame(result)

        # Salva LOCAL primeiro (mais rápido/estável)
        df.to_csv(saida_local, sep=';', index=False)

        # Se existir arquivo homônimo na rede, remove para evitar erro do move
        if os.path.exists(saida_rede):
            os.remove(saida_rede)

        # Move para a rede
        shutil.move(saida_local, saida_rede)
        logger.info(f'Arquivo disponibilizado na rede: {saida_rede}')
        logger.info('Processo finalizado com sucesso.')

    except Exception as e:
        logger.exception(f'Falha na execução: {e}')

if __name__ == '__main__':
    main()
