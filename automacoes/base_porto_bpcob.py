import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import logging.handlers
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import random
import shutil
import string
import calendar
import re
import sys
from concurrent.futures import ThreadPoolExecutor
import getpass

# --- CONFIGURAÇÕES GLOBAIS ---
# EXECUTAR (TESTE = 1 ; PRODUCAO = 2)
EXECUTAR = 1
CARTEIRA = 'PORTO SEGURO BPCOB' # Nome da carteira ajustado para o novo processo

# --- CONFIGURAÇÃO DE LOGGING ---
def _configurar_logging(nome_arquivo_log: str):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        nome_arquivo_log, when='midnight', interval=1, backupCount=7, encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False

    return logger   # 👈 TEM que estar na mesma indentação dos outros

    """
    Configura o sistema de logging para console e arquivo, replicando o comportamento do projeto antigo.
    """
  

# --- FUNÇÕES UTILITÁRIAS ---
def obter_data_hora_atual():
    """Retorna a data atual no formato ddmmaaaa para nomes de arquivo."""
    return datetime.now().strftime('%d%m%Y')

def obter_data_atual_formatada():
    """Retorna a data atual no formato dd/mm/aaaa."""
    return datetime.today().strftime('%d/%m/%Y')

def dia_da_semana_hoje() -> str:
    """Retorna a abreviação do dia da semana para a data de hoje."""
    dias = ["seg", "ter", "qua", "qui", "sex", "sab", "dom"]
    return dias[datetime.today().weekday()]

def semana_do_mes(data: datetime = None) -> int:
    """
    Retorna em qual semana a data do dia atual está alocada (aproximadamente 4 semanas por mês).
    """
    if data is None:
        data = datetime.today()
    
    total_dias = calendar.monthrange(data.year, data.month)[1]
    tamanho_semana = total_dias / 4
    
    return min(int((data.day - 1) // tamanho_semana) + 1, 4)

def obter_data_hora(ajuste_tempo=None):
    """
    Retorna a data e hora atual no formato 'dd/mm/yyyy HH:MM:SS', com ajustes opcionais.
    """
    data_hora_atual = datetime.now()
    
    if ajuste_tempo:
        data_hora_atual += timedelta(
            days=ajuste_tempo.get('D', 0),
            hours=ajuste_tempo.get('HH', 0),
            minutes=ajuste_tempo.get('MM', 0),
            seconds=ajuste_tempo.get('SS', 0)
        )
        data_hora_atual += relativedelta(
            months=ajuste_tempo.get('M', 0),
            years=ajuste_tempo.get('A', 0)
        )
    
    return data_hora_atual.strftime("%d/%m/%Y %H:%M:%S")

def mover_arquivos_para_pasta(diretorio):
    """
    Cria uma pasta com um nome aleatório e move os arquivos do diretório especificado para essa nova pasta.
    :param diretorio: Caminho do diretório onde os arquivos estão localizados.
    """
    if not os.path.exists(diretorio):
        raise ValueError("O diretório especificado não existe.")
    
    if not os.path.isdir(diretorio):
        raise ValueError("O caminho especificado não é um diretório válido.")

    nome_pasta = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    caminho_pasta = os.path.join(diretorio, nome_pasta)
    
    os.makedirs(caminho_pasta)
    
    for item in os.listdir(diretorio):
        caminho_item = os.path.join(diretorio, item)
        if os.path.isfile(caminho_item):
            shutil.move(caminho_item, caminho_pasta)
    
    logger.info(f"Arquivos movidos para a pasta: {caminho_pasta}")

def copiar_arquivos(lista_arquivos, destino):
    """Copia uma lista de arquivos para um diretório de destino."""
    for arquivo in lista_arquivos:
        try:
            shutil.copy2(arquivo, destino)
            logger.info(f"Arquivo {arquivo} copiado com sucesso para {destino}")
        except shutil.Error as e:
            logger.error(f"Erro ao copiar o arquivo {arquivo}: {e}", exc_info=True)

def criar_esquema_pastas(caminho_base: str, estrutura_pastas: dict) -> None:
    """Cria um esquema de diretórios baseado em um dicionário."""
    for pasta_pai, subpastas in estrutura_pastas.items():
        caminho_pasta_pai = os.path.join(caminho_base, pasta_pai)
        
        if not os.path.exists(caminho_pasta_pai):
            os.makedirs(caminho_pasta_pai)
            logger.info(f"Criada: {caminho_pasta_pai}")
        
        if isinstance(subpastas, list):
            for subpasta in subpastas:
                caminho_subpasta = os.path.join(caminho_pasta_pai, subpasta)
                if not os.path.exists(caminho_subpasta):
                    os.makedirs(caminho_subpasta)
                    logger.info(f"Criada: {caminho_subpasta}")

        elif isinstance(subpastas, dict):
            criar_esquema_pastas(caminho_pasta_pai, subpastas)

# Função para extrair o primeiro telefone com 11 dígitos
def extrair_celular(telefone):
    numeros = str(telefone).split('/')
    for num in numeros:
        num = num.strip()
        if re.fullmatch(r'\d{11}', num):
            return num
    return None

# --- CLASSE PRINCIPAL ADAPTADA ---
class ProcessadorDadosPorto:
    """
    Classe unificada para extrair e consolidar a base de dados da carteira Porto Seguro.
    """
    def __init__(self, chave_conexao, caminho_salvamento):
        self.chave_conexao = chave_conexao
        self.caminho_salvamento = caminho_salvamento
        self.engine = create_engine(self.chave_conexao)
        self.df_final = pd.DataFrame()
        logger.info(f"Processador de Dados da Carteira '{CARTEIRA}' inicializado.")

    def _extrair_e_unir_dados(self):
        """
        Executa as consultas fornecidas, une os resultados em memória e gera a base final.
        """
        logger.info("Iniciando extração de dados com múltiplas queries.")
        
        # Query 1: Base de Contratos e Parcelas
        query_base = """
        SELECT
            CASE
                WHEN a.cdBanco = 73 THEN 'E&F AMIGAVEL'
                WHEN a.cdBanco = 74 THEN 'E&F CONTENCIOSO SG'
                WHEN a.cdBanco = 75 THEN 'E&F CONTENCIOSO CG'
                ELSE NULL
            END AS CARTEIRA,
            a.cdContrato AS ID,
            a.ContratoUnico AS CONTRATO,
            a.Nome AS NOME,
            a.CPF,
            a.UFOperacao AS UF,
            -- Dias de atraso considerando a parcela mais recente em aberto
            atraso.DIAS_ATRASO,
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
            CASE
                WHEN a.cdBanco = 73 AND atraso.DIAS_ATRASO BETWEEN 7 AND 30 THEN '1. BK1'
                WHEN a.cdBanco = 73 AND atraso.DIAS_ATRASO BETWEEN 31 AND 65 THEN '2. BK2'
                WHEN a.cdBanco = 73 AND (atraso.DIAS_ATRASO < 7 OR atraso.DIAS_ATRASO > 65) THEN '3. SEM BUCKET'
                WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO BETWEEN 0 AND 540 THEN '1. ATIVA ATE 540'
                WHEN a.cdBanco = 75 AND atraso.DIAS_ATRASO > 540 THEN '2. WO ACIMA 540'
                WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO BETWEEN 0 AND 540 THEN '1. ATIVA ATE 540'
                WHEN a.cdBanco = 74 AND atraso.DIAS_ATRASO > 540 THEN '2. WO ACIMA 540'
            END AS ESTOQUE_PORTO,
            a.VlContrato AS PRINCIPAL,
            a.Plano AS `Total Parcelas`,
            d.numeroPrestacao AS `Parcela atual`,
            d.valorPrincipal AS `Valor da parcela`,
            a.DtProcessamento AS DATA_IMPORTACAO,
            b.dataInicioContrato AS PRIM_VENC,
            DATEDIFF(CURDATE(), DATE(a.DtUltAcionamento)) AS DEFAS_ACIONAMENTO,
            d.codigoProduto AS Prod,
            IF(a.Ativo = 1,'Ativo','Baixado') AS Status,
            IF(a.Suspenso = '0', 'Não', 'Sim') AS Suspenso,
            DescModalidadeProduto,
            IF(a.cdStatusJudicial = 0, NULL, 'Sim') AS `Status Judicial`,
            IF(a.cdStatusJudicial = 0, NULL, e.Nome) AS Tramite
        FROM cadcontrato a
        LEFT JOIN caddadosporto b ON b.cdContrato = a.cdContrato
        LEFT JOIN cadeventoacionamento c ON c.cdEvento = a.cdUltEventoAcionamento
        LEFT JOIN cadprestacaoporto d 
            ON d.cdContrato = a.cdContrato
            AND d.dataVencimento BETWEEN (CURDATE() - INTERVAL 16 DAY) AND (CURDATE() + INTERVAL 16 DAY)
            AND d.dataPagamento = '0001-01-01'
        LEFT JOIN cadtramitejuridico e 
            ON a.cdTramiteJudicial = e.cdTramite
        LEFT JOIN (
            SELECT 
                cdContrato,
                COALESCE(DATEDIFF(CURDATE(), MIN(dataVencimento)),0) AS DIAS_ATRASO
            FROM cadprestacaoporto
            WHERE dataPagamento = '0001-01-01'
            GROUP BY cdContrato
        ) atraso ON atraso.cdContrato = a.cdContrato
        WHERE
            a.cdBanco IN (73, 74, 75)
            AND (a.Ativo = 1 OR DATE(a.DtHoraRetomada) = CURDATE());
        """

        # Query 2: Telefones, já ordenados por prioridade
        query_telefone = """
        SELECT
            c.cdContrato as ID,
            concat(a.DDD, a.Numero) as TELEFONE_CPC,
            IF(a.DtUltCPC IS NOT NULL, 'Sim', 'Não') AS 'CPC',
            'MOVEL' as TIPO_TELEFONE
        FROM cadfone a
        JOIN cadcontrato c ON a.cdContrato = c.cdContrato
            WHERE
                c.cdBanco in (73, 74, 75)
                and c.Ativo = 1
                and a.Bom = 1
                and a.Confirmado in (0, 1)
                and a.cdTipoFone = 3
            ORDER BY
                c.cdContrato asc,
                IF(a.DtUltCPC IS NOT null, 1, 2) asc,
                a.Confirmado desc,
                a.pontuacao DESC;
        """

        # Query 3: E-mails válidos, com filtro e ordenação corretos
        query_email = """
        SELECT
            a.cdContrato as ID,
            c.Endereco as EMAIL
        FROM
            cadcontrato a
        JOIN cademail c ON a.cdContrato = c.cdContrato

            WHERE
                a.cdBanco IN (73, 74, 75)
                AND a.Ativo = 1
                AND c.Bom = 1
            ORDER BY
                a.cdContrato ASC,
                c.Confirmado DESC;
        """
       
        # Query 4: Vencimentos
        query_vencimento = """
        SELECT
            a.cdContrato as 'ID',
            numeroPrestacao,
            dataVencimento
        FROM bpcob.cadprestacaoporto AS a
        LEFT JOIN bpcob.cadcontrato AS b
            ON a.cdContrato = b.cdContrato

            WHERE
                b.cdBanco IN (73, 74, 75)
                AND (b.Ativo = 1 OR DATE(b.DtHoraRetomada) = CURDATE())
                AND a.dataPagamento = '0001-01-01'
            ORDER BY
                CASE
                    WHEN a.dataVencimento BETWEEN (CURDATE() - INTERVAL 15 DAY) AND (CURDATE() + INTERVAL 15 DAY) THEN 1
                    ELSE 2
                END ASC,
                a.dataVencimento ASC,
                a.cdContrato ASC;
        """

        try:
            logger.info("Executando a consulta de base...")
            df_base = pd.read_sql_query(query_base, self.engine)
            logger.info(f"Consulta de base executada. {len(df_base)} registros encontrados.")

            logger.info("Executando a consulta de telefones...")
            df_telefones = pd.read_sql_query(query_telefone, self.engine)
            logger.info(f"Consulta de telefones executada. {len(df_telefones)} registros encontrados.")
            
            logger.info("Executando a consulta de e-mails...")
            df_emails = pd.read_sql_query(query_email, self.engine)
            logger.info(f"Consulta de e-mails executada. {len(df_emails)} registros encontrados.")

            logger.info("Executando a consulta de vencimentos...")
            df_vencimentos = pd.read_sql_query(query_vencimento, self.engine)
            logger.info(f"Consulta de vencimentos executada. {len(df_vencimentos)} registros encontrados.")

            # Pega o "TOP 1" para cada contrato usando pandas.
            logger.info("Removendo duplicatas e selecionando o melhor registro para cada contrato...")
            df_telefones_top1 = df_telefones.drop_duplicates(subset=['ID'], keep='first')
            df_emails_top1 = df_emails.drop_duplicates(subset=['ID'], keep='first')
            df_vencimentos_top1 = df_vencimentos.drop_duplicates(subset=['ID'], keep='first')
            logger.info("Registros únicos selecionados.")

            # Merge da base principal com os dados adicionais
            logger.info("Iniciando merge dos dataframes...")
            df_base = pd.merge(df_base, df_telefones_top1, on='ID', how='left')
            df_base = pd.merge(df_base, df_emails_top1, on='ID', how='left')
            self.df_final = pd.merge(df_base, df_vencimentos_top1, on='ID', how='left')
            logger.info("Merge finalizado com sucesso.")
            
            # Processamento final das colunas
            self._processar_base_final()

        except Exception as e:
            logger.critical(f"Falha crítica na extração de dados: {e}", exc_info=True)
            raise

    def _processar_base_final(self):
        """Ajusta as colunas finais do DataFrame, como o tipo de telefone."""
        logger.info("Processando colunas finais da base de dados.")
        
        # Preenche a coluna TIPO_TELEFONE: se veio do join, é MOVEL. Se não, é SEM TELEFONE.
        self.df_final['TIPO_TELEFONE'] = self.df_final['TIPO_TELEFONE'].fillna('SEM TELEFONE')
        
        logger.info("Processamento final concluído.")

    def _salvar_base_final(self):
        """Salva o DataFrame final consolidado em um único arquivo CSV."""
        logger.info("Iniciando salvamento do arquivo final.")
        if self.df_final.empty:
            logger.warning("DataFrame final está vazio. Nenhum arquivo será salvo.")
            return

        try:
            # MUDANÇA: Removido os.makedirs(os.path.dirname(self.caminho_salvamento), exist_ok=True)
            # A criação do diretório agora é responsabilidade do bloco __main__
            
            # MUDANÇA: Adicionando logs de diagnóstico de permissão antes de salvar
            target_dir = os.path.dirname(self.caminho_salvamento)
            logger.info(f"Tentando salvar arquivo em: {self.caminho_salvamento}")
            logger.info(f"Diretório de destino: {target_dir}")
            logger.info(f"Diretório existe: {os.path.exists(target_dir)}")
            logger.info(f"Diretório é gravável (os.W_OK): {os.access(target_dir, os.W_OK)}")
            
            # MUDANÇA: Adicionando remoção do arquivo de destino se ele já existir
            if os.path.exists(self.caminho_salvamento):
                logger.info(f"Arquivo '{self.caminho_salvamento}' já existe. Tentando remover...")
                try:
                    os.remove(self.caminho_salvamento)
                    logger.info(f"Arquivo '{self.caminho_salvamento}' removido com sucesso.")
                except Exception as remove_e:
                    logger.error(f"Erro ao remover arquivo existente '{self.caminho_salvamento}': {remove_e}", exc_info=True)
                    raise PermissionError(f"Permissão negada ao remover arquivo existente '{self.caminho_salvamento}'.") from remove_e

            # Tenta criar um arquivo temporário para testar a permissão de escrita
            test_file_path = os.path.join(target_dir, 'temp_write_test.tmp')
            try:
                with open(test_file_path, 'w') as f:
                    f.write("Testando permissão de escrita.\n")
                logger.info(f"Teste de escrita em '{test_file_path}' bem-sucedido.")
                os.remove(test_file_path) # Limpa o arquivo de teste
                logger.info(f"Arquivo de teste '{test_file_path}' removido.")
            except Exception as test_e:
                logger.error(f"Falha no teste de escrita em '{test_file_path}': {test_e}", exc_info=True)
                # Se o teste de escrita falhou, re-levante o erro para parar o processo
                raise PermissionError(f"Permissão negada ao tentar escrever no diretório '{target_dir}'. Teste de escrita falhou.") from test_e

            self.df_final.to_csv(self.caminho_salvamento, sep=';', index=False, encoding='utf-8-sig')
            logger.info(f"Base final salva com sucesso em: {self.caminho_salvamento}")
        except Exception as e:
            logger.error(f"Erro ao salvar a base final: {e}", exc_info=True)

    def executar_pipeline(self):
        """Orquestra a execução de todo o processo simplificado."""
        logger.info("--- INICIANDO PIPELINE DE PROCESSAMENTO SIMPLIFICADO ---")
        self._extrair_e_unir_dados()
        self._salvar_base_final()
        logger.info("--- PIPELINE DE PROCESSAMENTO FINALIZADO ---")


if __name__ == "__main__":
    # --- CONFIGURAÇÃO DE AMBIENTE E PASTAS ---
    if EXECUTAR == 1: # Ambiente de Teste (Windows)
        PASTA_SAIDA = r'\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto'
        caminho_modulo_bot = r"\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto"
    elif EXECUTAR == 2: # Ambiente de Produção (Linux)
        PASTA_SAIDA = r'\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto'
        caminho_modulo_bot = r"\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto"
    else:
        raise ValueError("Variável 'EXECUTAR' deve ser 1 (Teste) ou 2 (Produção).")

    # MUDANÇA: Configuração do logger movida para o início do bloco __main__
    NOME_ARQUIVO_LOG = 'log_processamento_porto.log' 
    logger = _configurar_logging(NOME_ARQUIVO_LOG) # AGORA 'logger' ESTÁ DEFINIDO AQUI

    # MUDANÇA: Adicionado os.makedirs(PASTA_SAIDA, exist_ok=True) aqui, como no projeto antigo
    try:
        os.makedirs(PASTA_SAIDA, exist_ok=True)
        logger.info(f"Diretório de saída '{PASTA_SAIDA}' verificado/criado com sucesso.") # AGORA 'logger' PODE SER USADO AQUI
    except PermissionError as e:
        # MUDANÇA: Usando o logger principal para o erro fatal
        logger.critical(f"ERRO FATAL: Permissão negada ao criar/verificar o diretório de saída '{PASTA_SAIDA}': {e}", exc_info=True)
        sys.exit(f"Erro de permissão no diretório de saída. Verifique as permissões de '{PASTA_SAIDA}'.")

    # MUDANÇA: Adicionando logs de diagnóstico no início do script
    try:
        current_user = getpass.getuser()
    except Exception:
        current_user = os.environ.get('USER') or os.environ.get('USERNAME') or "USUARIO_DESCONHECIDO_AUTOMACAO"
    logger.info(f"Script iniciado pelo usuário: '{current_user}'")
    logger.info(f"Diretório de trabalho atual (CWD): '{os.getcwd()}'")
    logger.info(f"Caminho absoluto do script: '{os.path.abspath(__file__)}'")


    # --- DEFINIÇÃO DE CONEXÃO E ARQUIVO DE SAÍDA ---
    CHAVE_BPCOB = 'mysql://usr_alvaroesteves:AE%40esteves0902@192.168.200.74:33306/bpcob?connect_timeout=180'
    DATA_ATUAL_STR = obter_data_hora_atual()
    CAMINHO_ARQUIVO_FINAL = os.path.join(PASTA_SAIDA, f'BASE_{CARTEIRA}_{DATA_ATUAL_STR}.csv')
    
    # --- EXECUÇÃO DO PROCESSO ---
    try:
        processador = ProcessadorDadosPorto(
            chave_conexao=CHAVE_BPCOB,
            caminho_salvamento=CAMINHO_ARQUIVO_FINAL
        )
        processador.executar_pipeline()

    except Exception as e:
        logger.critical(f"Ocorreu um erro fatal no pipeline principal: {e}", exc_info=True)
