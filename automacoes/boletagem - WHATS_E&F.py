import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime

# ==========================
# CONFIGURAÇÃO DO BANCO
# ==========================
USUARIO = "usr_alvaroesteves"
SENHA = quote_plus("AE@esteves0902")   # use quote_plus pra senha com caracteres especiais
HOST = "192.168.200.74"                # ex: "192.168.200.74"
PORTA = "33306"                  # ajuste se for diferente
BANCO = "bpcob"              # ex: "bpcob"

URL_CONEXAO = f"mysql+pymysql://{USUARIO}:{SENHA}@{HOST}:{PORTA}/{BANCO}?connect_timeout=180"
engine = create_engine(URL_CONEXAO)

# ==========================
# CAMINHO DA PASTA DE REDE
# ==========================
# Exemplo UNC:
CAMINHO_PASTA_REDE = r"\\192.168.200.81\PlanejamentoPorto\1.Carteiras\2.Porto"


# ==========================
# QUERY BASE (PARAMETRIZADA)
# ==========================
SQL_BASE = text("""
SELECT DISTINCT
    c.cdContrato        AS cdcontrato,
    cf.DDD              AS ddd,
    cf.numero           AS numero,
    c.CPF               AS cnpj_cpf,
    cba.BoletoBanco     AS boleto_banco
    -- cba.LinhaDigitavel -- disponível se precisar no futuro
FROM
    cadcontrato c
LEFT JOIN cadacordoporto ca
    ON ca.cdContrato = c.cdContrato
LEFT JOIN cadacordoparcelaporto cpa
    ON cpa.cdAcordo = ca.cdAcordo
LEFT JOIN cadboletoparcelaacordoporto cba
    ON cba.idParcelaAcordo = cpa.IdParcelasDoAcordo
LEFT JOIN cadfone cf
    ON cf.cdContrato = c.cdContrato
WHERE
    c.cdBanco = :cd_banco
    AND c.Ativo = 1
    AND ca.status = 0
    AND cf.Bom = 1
    AND cf.Confirmado = 1
    AND cf.cdTipoFone = 3
    AND DATE(ca.dtVencimentoApi) = CURDATE()
    AND cba.BoletoBanco IS NOT NULL
ORDER BY
    ca.cdAcordo DESC,
    IF(cf.DtUltCPC IS NOT NULL, 1, 2) ASC,
    cf.Confirmado DESC,
    cf.pontuacao DESC;
""")

def gerar_base(nome_base: str, cd_banco: int, prefixo_arquivo: str):
    """
    Roda a query para um cd_banco específico, monta a mensagem
    e salva o Excel na pasta de rede.
    """
    print(f"\nGerando base: {nome_base} (cdBanco = {cd_banco})")

    # Executa a query filtrando pelo banco desejado
    df = pd.read_sql(SQL_BASE, engine, params={"cd_banco": cd_banco})

    if df.empty:
        print(f"Nenhum registro encontrado para {nome_base}. Arquivo não será gerado.")
        return

    # Monta o texto da mensagem
    df["mensagem"] = df["boleto_banco"].apply(
        lambda link: f"PORTO BANK: Nao perca seu acordo que vence HOJE: {link}"
    )

    # Layout final
    df_final = df[["cdcontrato", "ddd", "numero", "cnpj_cpf", "mensagem"]]

    # Garante que a pasta de rede exista (se tiver permissão)
    os.makedirs(CAMINHO_PASTA_REDE, exist_ok=True)

    hoje_str = datetime.today().strftime("%Y%m%d")
    nome_arquivo = f"{prefixo_arquivo}_{hoje_str}.xlsx"
    caminho_arquivo = os.path.join(CAMINHO_PASTA_REDE, nome_arquivo)

    df_final.to_excel(caminho_arquivo, index=False)

    print(f"Arquivo gerado com sucesso para {nome_base}: {caminho_arquivo}")
    print(f"Total de registros: {len(df_final)}")

def main():
    # Base 1: E&F Amigavel (cdBanco = 73)
    gerar_base(
        nome_base="E&F Amigavel",
        cd_banco=73,
        prefixo_arquivo="PORTO_EF_AMIGAVEL-BOLETO_WHATS"
    )

    # Base 2: E&F Contencioso SG (cdBanco = 74)
    gerar_base(
        nome_base="E&F Contencioso SG",
        cd_banco=74,
        prefixo_arquivo="PORTO_EF_CONTENCIOSO_SG-BOLETO_WHATS"
    )

    # Base 3: Contencioso CG (cdBanco = 75)
    gerar_base(
        nome_base="Contencioso CG",
        cd_banco=75,
        prefixo_arquivo="PORTO_CONTENCIOSO_CG_-BOLETO_WHATS"
    )

if __name__ == "__main__":
    main()
