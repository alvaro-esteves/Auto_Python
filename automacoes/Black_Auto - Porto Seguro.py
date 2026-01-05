# -*- coding: utf-8 -*-
import os
import errno
import pymysql
from datetime import date, timedelta, datetime

# ==========================
# LOG DE DEBUG
# ==========================
DEBUG_LOG_PATH = r"C:\Users\05320897154\Desktop\Codigos\debug_blacklist.txt"

def debug_log(msg: str):
    try:
        os.makedirs(os.path.dirname(DEBUG_LOG_PATH), exist_ok=True)
        with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now()} - {msg}\n")
    except Exception as e:
        # Evita quebrar o job por falha no log
        print(f"❌ Erro ao escrever log de debug: {e}")

# ==========================
# CAMINHOS (PADRONIZADOS)
# ==========================
# Destino final do arquivo .act
CAMINHO_REDE = r"\\192.168.158.19\exclusions"   # <- mesmo do .BAT

# Caminho do log de operação (confirme se tem escrita; senão será criado localmente)
CAMINHO_LOG = r"\\192.168.200.81\PlanejamentoPorto\1.Álvaro Esteves\Relatórios"

# ==========================
# HELPERS
# ==========================
def validar_share_gravavel(pasta: str) -> str:
    """
    Valida se 'pasta' existe e é gravável. Se não for, troca para um fallback local
    e retorna o caminho efetivo.
    """
    debug_log(f"Validando share: {pasta}")
    try:
        if not os.path.isdir(pasta):
            raise FileNotFoundError(errno.ENOENT, f"Pasta não acessível: {pasta}")

        # teste de escrita
        teste = os.path.join(pasta, f"._teste_{int(datetime.now().timestamp())}.tmp")
        with open(teste, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(teste)

        debug_log(f"Share OK para escrita: {pasta}")
        return pasta

    except Exception as e:
        debug_log(f"❌ Share não gravável ({pasta}): {e}")
        # fallback local
        fallback = r"C:\Users\05320897154\Desktop\SAIDA_LOCAL"
        os.makedirs(fallback, exist_ok=True)
        debug_log(f"Usando fallback local: {fallback}")
        return fallback

def garantir_pasta(path_file: str):
    """Cria a pasta do arquivo informado (se não existir)."""
    pasta = os.path.dirname(path_file)
    if pasta:
        os.makedirs(pasta, exist_ok=True)

# ==========================
# CONFIG DE NEGÓCIO
# ==========================
modo  = 'A'      # A = ADICIONAR / D = DELETAR
tipo  = 'A'      # A = ACCOUNT / V = TELEFONE
dias  = 0
data_inicio = date.today().strftime("%b %d %Y")
data_fim    = (date.today() + timedelta(dias)).strftime("%b %d %Y")
hora_inicio = '04:00AM'
hora_fim    = '11:59PM'
bloqueio    = 'ALL'

# ==========================
# ARQUIVOS (depois de validar share)
# ==========================
# valida destino do .act
DESTINO_ATIVO = validar_share_gravavel(CAMINHO_REDE)
# valida destino do log; se não gravável, também cai para local
DESTINO_LOGS  = validar_share_gravavel(CAMINHO_LOG)

nome_arquivo = os.path.join(DESTINO_ATIVO, "Blacklist_Porto_Acordos.act")
nome_log     = os.path.join(DESTINO_LOGS,  "Blacklist_Porto_Acordos.log")

garantir_pasta(nome_arquivo)
garantir_pasta(nome_log)

debug_log(f"Arquivo de saída: {nome_arquivo}")
debug_log(f"Arquivo de log:   {nome_log}")

# ==========================
# FUNÇÃO: carregar registros já logados (opcional)
# ==========================
def carregar_log_existente(caminho_log: str):
    registros_logados = {}
    try:
        with open(caminho_log, 'r', encoding='utf-8') as log:
            for linha in log:
                if 'Adicionado' in linha:
                    partes = linha.strip().split(']')
                    timestamp = partes[0].replace('[', '')
                    identificador = partes[1].split(':', 1)[1].strip()
                    registros_logados[identificador] = timestamp
        debug_log(f"Log existente carregado: {len(registros_logados)} registros.")
    except FileNotFoundError:
        debug_log("Nenhum log anterior encontrado, iniciando vazio.")
    except Exception as e:
        debug_log(f"⚠️ Falha ao ler log existente: {e}")
    return registros_logados

registros_logados = carregar_log_existente(nome_log)

# ==========================
# DB 1: cslog_prod
# ==========================
debug_log("Conectando ao banco cslog_prod...")
try:
    con_cslog = pymysql.connect(
        host='192.168.200.35',
        port=33306,
        user='usr_alvaroesteves',
        password='AE@esteves0902',
        database='cslog_prod',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor,
    )
    debug_log("Conexão cslog_prod estabelecida.")

    query_cslog = """
    WITH historico_hoje AS (
        SELECT a.ID_CONTR, a.CODIGO
        FROM HISTORICO a
        INNER JOIN CODIGO b ON b.COD = a.CODIGO
        WHERE b.HISTORICO IN (11, 18, 90, 484, 884, 958, 1880, 1238, 2149)
          AND DATE(a.DATA) = CURDATE()
    )
    SELECT DISTINCT
        c.ID_CONTR,
        d.DDD,
        d.NUMERO,
        tc.DATA_ULT_CPC
    FROM historico_hoje hh
    INNER JOIN CONTRATO c ON c.ID_CONTR = hh.ID_CONTR
    INNER JOIN TELEFONE d ON d.CPF = c.CPF
    INNER JOIN TELEFONE_COMPL tc ON tc.ID_TEL = d.ID_TEL
    WHERE DATE(tc.DATA_ULT_CPC) = CURDATE()
      AND c.ID_CARTEIRA IN (110, 111, 112, 113, 114, 115)
      AND c.STATUS NOT IN (8)
      union
      WITH historico_hoje AS (
    SELECT 
        a.ID_CONTR,
        a.CODIGO
    FROM HISTORICO a
    INNER JOIN CODIGO b ON b.COD = a.CODIGO
    WHERE 
        b.HISTORICO IN (11, 18, 484, 958, 1238)
        AND DATE(a.DATA) BETWEEN DATE_SUB(CURDATE(), INTERVAL 2 DAY) AND CURDATE()
)
SELECT DISTINCT
    c.ID_CONTR,
    d.DDD,
    d.NUMERO,
    tc.DATA_ULT_CPC
FROM historico_hoje hh
INNER JOIN CONTRATO c ON c.ID_CONTR = hh.ID_CONTR
INNER JOIN TELEFONE d ON d.CPF = c.CPF
INNER JOIN TELEFONE_COMPL tc ON tc.ID_TEL = d.ID_TEL
WHERE
    DATE(tc.DATA_ULT_CPC) = CURDATE()
    AND c.ID_CARTEIRA IN (110, 111, 112, 113, 114, 115)
    AND c.STATUS NOT IN (8);
    """

    with con_cslog.cursor() as cursor:
        cursor.execute(query_cslog)
        resultados_cslog = cursor.fetchall()
        debug_log(f"Query cslog_prod retornou {len(resultados_cslog)} registros.")

    # Escreve/abre arquivo e log
    with open(nome_arquivo, 'w', encoding='utf-8') as arquivo_saida, \
         open(nome_log, 'a', encoding='utf-8') as arquivo_log:

        for row in resultados_cslog:
            id_contr = row[0]
            linha = f"{modo}|{tipo}|{data_inicio} {hora_inicio}|{data_fim} {hora_fim}|{id_contr}|{bloqueio}"
            arquivo_saida.write(linha + '\n')

            if str(id_contr) not in registros_logados:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                registros_logados[str(id_contr)] = timestamp
                arquivo_log.write(f"[{timestamp}] Adicionado ID_CONTR: {id_contr}\n")

    debug_log("Processamento cslog_prod concluído.")

except Exception as e:
    debug_log(f"❌ Erro no cslog_prod: {e}")

finally:
    try:
        con_cslog.close()
        debug_log("Conexão cslog_prod fechada.")
    except Exception:
        pass

# ==========================
# DB 2: bpcob
# ==========================
debug_log("Conectando ao banco bpcob...")
try:
    con_bpcob = pymysql.connect(
        host='192.168.200.74',
        port=33306,
        user='usr_alvaroesteves',
        password='AE@esteves0902',
        database='bpcob',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor,
    )
    debug_log("Conexão bpcob estabelecida.")

    query_bpcob = """
    SELECT c.ContratoUnico
    FROM cadacionamento a
    INNER JOIN cadcontrato c ON c.cdContrato = a.cdContrato
    INNER JOIN cadeventoacionamento e ON e.cdEvento = a.cdEvento
    WHERE DATE(a.DataHora) = CURDATE()
      AND c.cdBanco IN (73, 74, 75)
      AND (
        e.Nome LIKE '%ACORDO%' OR
        e.Nome LIKE '%Acordo Atualizacao%' OR
        e.Nome LIKE '%Acordo cumprido%' OR
        e.Nome LIKE '%ALEGA PAGTO%' OR
        e.Nome LIKE '%CONFIRMAÇÃO DE ACORDO%'
      )
    UNION
    SELECT c.ContratoUnico
    FROM cadContrato c
    LEFT JOIN cadacordoporto cc ON c.cdContrato = cc.cdContrato
    WHERE c.cdBanco IN (73, 74, 75)
      AND DATE(cc.dtVencimentoApi) >= DATE_SUB(CURDATE(), INTERVAL 2 DAY)
      AND DATE(cc.dtVencimentoApi) <= DATE_ADD(CURDATE(), INTERVAL 30 DAY)
      AND cc.status not in (2);
    """

    with con_bpcob.cursor() as cursor:
        cursor.execute(query_bpcob)
        resultados_bpcob = cursor.fetchall()
        debug_log(f"Query bpcob retornou {len(resultados_bpcob)} registros.")

    with open(nome_arquivo, 'a', encoding='utf-8') as arquivo_saida, \
         open(nome_log, 'a', encoding='utf-8') as arquivo_log:

        for row in resultados_bpcob:
            contrato_unico = row[0]
            linha = f"{modo}|{tipo}|{data_inicio} {hora_inicio}|{data_fim} {hora_fim}|{contrato_unico}|{bloqueio}"
            arquivo_saida.write(linha + '\n')

            if str(contrato_unico) not in registros_logados:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                registros_logados[str(contrato_unico)] = timestamp
                arquivo_log.write(f"[{timestamp}] Adicionado CD_CONTRATO: {contrato_unico}\n")

    debug_log("Processamento bpcob concluído.")

except Exception as e:
    debug_log(f"❌ Erro no bpcob: {e}")

finally:
    try:
        con_bpcob.close()
        debug_log("Conexão bpcob fechada.")
    except Exception:
        pass

# ==========================
# FINAL
# ==========================
msg_final = f"✅ Arquivo gerado com sucesso: {nome_arquivo}"
print(msg_final)
debug_log(msg_final)
debug_log(f"Log atualizado em: {nome_log}")
