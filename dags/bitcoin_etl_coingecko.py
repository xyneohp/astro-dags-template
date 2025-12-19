from __future__ import annotations
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import pendulum
import requests
import pandas as pd

DEFAULT_ARGS = {
    "owner": "Alex Lopes",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@task
def fetch_bitcoin_etl_coingecko():
    """
    Coleta dados do Bitcoin em lote para o intervalo da execução atual.
    Se a DAG for mensal, ctx["data_interval_end"] será o fim do mês.
    """
    ctx = get_current_context()

    # Define o intervalo baseado no agendamento do Airflow
    start_time = ctx["data_interval_start"]
    #end_time = ctx["data_interval_end"]
    end_time = start_time.add(months=1)
    
    print(f"[Bulk] Solicitando dados de {start_time} até {end_time}")

    # Conversão para Unix Timestamp (segundos) para CoinGecko
    start_s = int(start_time.timestamp())
    end_s = int(end_time.timestamp())

    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_s,
        "to": end_s,
    }

    # Chamada à API
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()

    # Processamento dos dados
    prices = payload.get("prices", [])
    if not prices:
        print("Nenhum dado encontrado para este período.")
        return

    # Criando DataFrames e realizando merge
    df_p = pd.DataFrame(prices, columns=["time_ms", "price_usd"])
    df_c = pd.DataFrame(payload.get("market_caps", []), columns=["time_ms", "market_cap_usd"])
    df_v = pd.DataFrame(payload.get("total_volumes", []), columns=["time_ms", "volume_usd"])

    df = df_p.merge(df_c, on="time_ms", how="outer").merge(df_v, on="time_ms", how="outer")
    
    # Tratamento de Tempo e Resample Diário
    # A API retorna dados horários se o range for > 1 dia e < 90 dias
    df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    df.set_index("time", inplace=True)
    df.drop(columns=["time_ms"], inplace=True)

    # GARANTIA: Resample para 1 linha por dia (pega o último preço de cada dia)
    df_daily = df.resample('D').last().dropna()
    
    print(f"Total de dias processados: {len(df_daily)}")
    print(df_daily.head())

    # Gravação no Postgres (Neon Tech)
    hook = PostgresHook(postgres_conn_id="postgres")
    engine = hook.get_sqlalchemy_engine()
    
    # O index=True salva a coluna 'time' como chave primária/coluna de tempo
    df_daily.to_sql("bitcoin_history_thgoliveira", con=engine, if_exists="append", index=True)

@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 0 1 * *",  # Executa uma vez por mês (no dia 1 de cada mês)
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=True,          # Fará o backfill mensal de Setembro, Outubro, Novembro...
    max_active_runs=1,     # Executa um mês por vez para respeitar limites
    tags=["bitcoin", "bulk", "monthly"],
)
def bitcoin_etl_coingecko():
    fetch_bitcoin_etl_coingecko()

dag = bitcoin_etl_coingecko()
