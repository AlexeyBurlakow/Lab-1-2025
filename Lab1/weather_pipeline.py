import json
import io
from datetime import datetime, timedelta, date

import requests
import pandas as pd
from minio import Minio
from clickhouse_connect import get_client
from prefect import flow, task, get_run_logger

MINIO_CONF = {
    "endpoint": "127.0.0.1:9010",
    "access": "minioadmin",
    "secret": "minioadmin",
    "bucket": "raw-weather-data"
}

CLICKHOUSE_SETTINGS = {
    "host": "127.0.0.1",
    "port": 8123,
    "user": "default",
    "password": "",
    "database": "default"
}

TELEGRAM = {
    "token": "8445629359:AAGtlxgDMQCFdaQQ0e-XmVRc00O39KDm1ps",
    "chat": "491533997"
}


def ch_client():
    """Создание подключения к ClickHouse."""
    return get_client(
        host=CLICKHOUSE_SETTINGS["host"],
        port=CLICKHOUSE_SETTINGS["port"],
        username=CLICKHOUSE_SETTINGS["user"],
        password=CLICKHOUSE_SETTINGS["password"],
        database=CLICKHOUSE_SETTINGS["database"]
    )


def prepare_clickhouse():
    """Создание таблиц"""
    cli = ch_client()

    cli.command("""
    CREATE TABLE IF NOT EXISTS weather_hourly (
        city String,
        timestamp DateTime,
        temperature Float32,
        precipitation Float32,
        wind_speed Float32,
        wind_direction UInt16
    )
    ENGINE = MergeTree()
    ORDER BY (city, timestamp)
    """)

    cli.command("""
    CREATE TABLE IF NOT EXISTS weather_daily (
        city String,
        date Date,
        temp_min Float32,
        temp_max Float32,
        temp_avg Float32,
        precip_total Float32
    )
    ENGINE = MergeTree()
    ORDER BY (city, date)
    """)

@task(name="Получение прогноза на завтра", retries=3)
def fetch_forecast():
    log = get_run_logger()

    locations = [
        {"title": "Москва", "lat": 55.75, "lon": 37.62},
        {"title": "Самара", "lat": 53.19, "lon": 50.10}
    ]

    query = {
        "latitude": ",".join([str(x["lat"]) for x in locations]),
        "longitude": ",".join([str(x["lon"]) for x in locations]),
        "hourly": "temperature_2m,precipitation,wind_speed_10m,wind_direction_10m",
        "timezone": "auto",
        "forecast_days": 2
    }

    resp = requests.get("https://api.open-meteo.com/v1/forecast", params=query)
    resp.raise_for_status()
    payload = resp.json()

    if not isinstance(payload, list):
        payload = [payload]

    for i, entry in enumerate(payload):
        entry["city"] = locations[i]["title"]

    log.info(f"Загружено данных: {len(payload)} городов.")
    return payload


@task(name="Сохранение исходников в MinIO")
def store_raw_weather(raw_batch):
    log = get_run_logger()

    m = Minio(
        MINIO_CONF["endpoint"],
        access_key=MINIO_CONF["access"],
        secret_key=MINIO_CONF["secret"],
        secure=False
    )

    if not m.bucket_exists(MINIO_CONF["bucket"]):
        m.make_bucket(MINIO_CONF["bucket"])

    day_tag = str(date.today())

    for rec in raw_batch:
        city = rec["city"]
        filename = f"{day_tag}/{city}.json"

        buf = json.dumps(rec, ensure_ascii=False).encode()
        stream = io.BytesIO(buf)

        m.put_object(
            MINIO_CONF["bucket"],
            filename,
            stream,
            length=len(buf),
            content_type="application/json"
        )
        log.info(f"MinIO saved: {filename}")


@task(name="Формирование почасового DataFrame")
def build_hourly(raw_batch):
    log = get_run_logger()
    rows = []

    tomorrow = str(date.today() + timedelta(days=1))

    for rec in raw_batch:
        city = rec["city"]
        hourly = rec["hourly"]
        for idx, ts in enumerate(hourly["time"]):
            if ts.startswith(tomorrow):
                rows.append({
                    "city": city,
                    "timestamp": datetime.fromisoformat(ts),
                    "temperature": hourly["temperature_2m"][idx],
                    "precipitation": hourly["precipitation"][idx],
                    "wind_speed": hourly["wind_speed_10m"][idx],
                    "wind_direction": hourly["wind_direction_10m"][idx],
                })

    df = pd.DataFrame(rows)
    log.info(f"Hour DF built: {len(df)} rows")
    return df


@task(name="Загрузка почасовых значений в ClickHouse")
def load_hour(df):
    if df.empty:
        return

    cli = ch_client()
    cli.insert_df(
        "weather_hourly",
        df[["city", "timestamp", "temperature", "precipitation", "wind_speed", "wind_direction"]]
    )


@task(name="Агрегация по дням")
def compute_daily(raw_batch):
    log = get_run_logger()

    bundle = []
    target_day = str(date.today() + timedelta(days=1))

    for rec in raw_batch:
        hourly = rec["hourly"]
        for i, ts in enumerate(hourly["time"]):
            if ts.startswith(target_day):
                bundle.append({
                    "city": rec["city"],
                    "timestamp": datetime.fromisoformat(ts),
                    "temperature": hourly["temperature_2m"][i],
                    "precipitation": hourly["precipitation"][i],
                    "wind_speed": hourly["wind_speed_10m"][i]
                })

    df = pd.DataFrame(bundle)
    if df.empty:
        return df

    df["date"] = df["timestamp"].dt.date
    out = df.groupby(["city", "date"]).agg(
        temp_min=("temperature", "min"),
        temp_max=("temperature", "max"),
        temp_avg=("temperature", "mean"),
        precip_total=("precipitation", "sum"),
        max_wind=("wind_speed", "max"),
    ).reset_index()

    log.info(f"Daily aggregated: {len(out)} records")
    return out


@task(name="Загрузка дневной статистики в ClickHouse")
def load_daily(df):
    if df.empty:
        return
    cli = ch_client()
    cli.insert_df("weather_daily", df[["city", "date", "temp_min", "temp_max", "temp_avg", "precip_total"]])


@task(name="Отправка прогноза в Telegram")
def telegram_notify(df):
    if df.empty:
        return

    msg = ["<b>Завтрашний прогноз:</b>\n"]

    for _, r in df.iterrows():
        text = (
            f"Город: <b>{r['city']}</b>: "
            f"{r['temp_min']:.1f}…{r['temp_max']:.1f}°C, "
            f"осадки {r['precip_total']:.1f} мм"
        )

        warns = []
        if r["max_wind"] > 20:
            warns.append("Cильный ветер")
        if r["precip_total"] > 10:
            warns.append("Cильный дождь")

        if warns:
            text += "\nПредупреждение " + ", ".join(warns)

        msg.append(text)

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM['token']}/sendMessage",
        json={"chat_id": TELEGRAM["chat"], "text": "\n".join(msg), "parse_mode": "HTML"}
    )


@flow(name="Weather Pipeline")
def run_etl():
    raw = fetch_forecast()
    store_raw_weather(raw)

    hourly = build_hourly(raw)
    load_hour(hourly)

    daily = compute_daily(raw)
    load_daily(daily)
    telegram_notify(daily)

if __name__ == "__main__":
    prepare_clickhouse()
    run_etl.serve(name="weather-job")
