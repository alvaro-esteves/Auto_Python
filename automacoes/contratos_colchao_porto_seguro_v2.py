# -*- coding: utf-8 -*-
"""
Extração de CONTRATOS_COLCHAO - PORTO SEGURO
- Usa driver pymysql (mysql+pymysql)
- Query otimizada com pré-agrupamento (sem subconsulta correlacionada por linha)
- Salva localmente e depois move para a pasta de rede
- Log com arquivo rotativo diário
"""

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
CARTEIRA = 'PORTO SEGURO'  # apenas rótulo no nome do arquivo

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

logger = configurar_logging('log_contratos_colchao_porto_seguro.log')

# =========================
# CAMINHOS / SAÍDA
# =========================
REDE_DIR = r'\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto'
LOCAL_TMP_DIR = os.path.join(os.getcwd(), '_tmp_export')
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
os.makedirs(REDE_DIR, exist_ok=True)

data_tag = datetime.now().strftime('%d%m%Y')
saida_local = os.path.join(LOCAL_TMP_DIR, f'CONTRATOS_COLCHAO_{CARTEIRA}_{data_tag}.csv')
saida_rede  = os.path.join(REDE_DIR,      f'CONTRATOS_COLCHAO_{CARTEIRA}_{data_tag}.csv')

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
SELECT
    DATE(CURDATE()) AS DT_REF,
    CASE
        WHEN a.ID_CARTEIRA = 110 THEN 'AMIGAVEL'
        WHEN a.ID_CARTEIRA = 111 THEN 'CONTENCIOSO'
        WHEN a.ID_CARTEIRA = 112 THEN 'CARRINHO ABANDONADO'
        WHEN a.ID_CARTEIRA = 113 THEN 'PJ AMIGAVEL'
        WHEN a.ID_CARTEIRA = 114 THEN 'PJ CONTENCIOSO'
        WHEN a.ID_CARTEIRA = 115 THEN 'MVBANK'
        ELSE NULL
    END AS SEGMENTO,
    a.NUM_CONTRATO,
    a.ID_CONTR,
    a.CPF,
    a.PRINCIPAL AS VLR_PRINCIPAL,
    CASE 
        WHEN a.STATUS = 0 THEN 'EM COBRANCA'
        WHEN a.STATUS = 4 THEN 'EM ACORDO'
        WHEN a.STATUS = 5 THEN 'ACORDO EM ATRASO'
        WHEN a.STATUS = 8 THEN 'DEVOLVIDO'
        ELSE NULL
    END AS STATUS_CONTRATO,
    CASE 
        WHEN b.STATUS = 0 THEN 'VALIDO'
        WHEN b.STATUS = 4 THEN 'CANCELADO'
        WHEN b.STATUS = 5 THEN 'INVALIDO'
        ELSE NULL
    END AS STATUS_ACORDO,
    b.TOTAL AS VLR_TOTAL_ACORDO,
    (b.TOTAL / NULLIF(b.NUM_PARCELAS, 0)) AS VALOR_PARCELA,
    DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) AS ATRASO,
    CASE
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) BETWEEN 91 AND 540 THEN 'ATIVO'
        WHEN DATEDIFF(CURDATE(), a.PRIM_VENC_ORIGINAL) >= 541 THEN 'WO'
        ELSE 'INDEFINIDO'
    END AS FASE_CONTENCIOSO,
    b.NUM_PARCELAS,
    CAST(b.NUM_PARC_ABERTO AS UNSIGNED) AS NUM_PARC_ABERTO,
    CASE
        WHEN a.STATUS = 5 AND b.NUM_PARCELAS <= 5 THEN 'SIM'
        ELSE 'NAO'
    END AS BSC,
    b.VENC_PARC_ABERTO,
    DATE_ADD(b.VENC_PARC_ABERTO, INTERVAL 12 DAY) AS MAXIMO_PAGAMENTO,
    ( SELECT MAX(d2.DATA_PGTO) FROM PARCELA d2 WHERE d2.ID_ACORDO = b.ID_ACORDO ) AS DATA_ULT_PGTO,
    CONCAT(c.DDD, c.NUMERO) AS TELEFONE,
    CASE WHEN b.ID_FUNCIONARIO = 9999 THEN 1 ELSE NULL END AS INDIRETO
FROM CONTRATO a
JOIN (
    SELECT ID_CONTR, MAX(DT_GRAVACAO) AS DT_GRAVACAO
    FROM ACORDO
    WHERE ID_CARTEIRA = 111
    GROUP BY ID_CONTR
) u
  ON u.ID_CONTR = a.ID_CONTR
JOIN ACORDO b
  ON b.ID_CONTR = u.ID_CONTR
 AND b.DT_GRAVACAO = u.DT_GRAVACAO
 AND b.ID_CARTEIRA = 111
LEFT JOIN TELEFONE c
  ON c.ID_TEL = a.ID_TEL
WHERE
    a.ID_CARTEIRA = 111
    AND a.STATUS <> 8
    AND b.NUM_PARCELAS >= 2
    AND b.NUM_PARC_ABERTO > 1
    AND b.STATUS = 0
    AND DATE(b.VENC_PARC_ABERTO) <= DATE_ADD(CURDATE(), INTERVAL 15 DAY);
""")

def main():
    try:
        logger.info(f'Iniciando extração: {CARTEIRA}')
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
