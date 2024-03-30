from fastapi import FastAPI
from fastapi import APIRouter
from sqlalchemy import text
from datetime import datetime, date, timedelta

from db import DW

app = FastAPI()

battery_router = APIRouter(tags=["Battery"])
consumption_router = APIRouter(tags=["Consumption"])
production_router = APIRouter(tags=["Production"])

app.include_router(consumption_router)
app.include_router(production_router)


"""  
how to fastapi

@app.get("/measurement/mitattava_asia/mahdollinen_lisätieto/{jokinarvo}")
async def kuvaava_nimi_tähän_tyyliin_koska_menee_docs(dw: DW tai db: DB, jokinarvo: int):
    
    _query = text("SELECT value FROM measurements_fact AS m WHERE value = :jokinarvo")
    rows = dw. tai db.execute(_query, {"jokinarvo": jokinarvo})
    data = rows.mappings().all()
    
    return {'dictionary_key': data}
    
    # mappings tekee jokaisesta tuloksen rivistä dictionaryn, 
    # jossa keynä on sql columnin nimi ja tekee niistä listan esim:
    # {
    #   "current_battery_stats": [
    #     {
    #       "sensor": "SoC %",
    #       "value": 46
    #     },
    #     {
    #       "sensor": "Temperature °C",
    #       "value": 17.5
    #     },
    #     {
    #       "sensor": "Voltage V",
    #       "value": 50.1
    #     }
    #   ]
    # }
"""



# Akun kunto palauttaa null tällä hetkellä,
# ehkä pitää poistaa niin saadaan koodista siistimpi.
# Ja samalla varmaan poistaa temp, koska sinänsä aika turha tieto.

# Palauttaa uusimmat tilastot akun tiedoista
@battery_router.get("/api/measurement/battery/current")
async def get_most_recent_values_from_battery(dw: DW):
    _query = text("SELECT s.sensor_name AS sensor, m.value AS value FROM measurements_fact AS m JOIN dates_dim AS d ON d.date_key = m.date_key JOIN sensors_dim AS s ON s.sensor_key = m.sensor_key WHERE s.device_id = 'TB_batterypack' AND m.date_key = (SELECT MAX(date_key) FROM measurements_fact WHERE sensor_key = s.sensor_key) ORDER BY s.sensor_id;")
    rows = dw.execute(_query)
    data = rows.mappings().all()

    return {'current_battery_stats': data}


# Haetaan päiväkohtainen kokonaistuotto tunneittain ryhmiteltynä:
@production_router.get("/api/measurement/production/total/hourly_for_day/{date}")
async def get_total_production_statistics_hourly_for_a_day(dw: DW, date: str):
    """
    Get production stats from a given day grouped by hour. String format YYYY-MM-DD
    """
    _query = text("SELECT SUM(p.value) AS total_production, d.hour "
                  "FROM productions_fact p JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE CONCAT_WS('-', d.year, d.month, d.day) = DATE(:date) "
                  "GROUP BY d.hour;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan päiväkohtainen kokonaistuotto:
@production_router.get("/api/measurement/production/total/day/{date}")
async def get_total_production_statistics_for_a_day(dw: DW, date: str):
    """
    Get production stats from a given day. String format YYYY-MM-DD
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as day, "
                  "SUM(p.value) AS total_production "
                  "FROM productions_fact p JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE CONCAT_WS('-', d.year, d.month, d.day) = DATE(:date);")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan kaikelle tuotolle päiväkohtainen keskiarvo:
@production_router.get("/api/measurement/production/avg/day/{date}")
async def get_avg_production_statistics_for_a_day(dw: DW, date: str):
    """
    Get production stats from a given day. String format YYYY-MM-DD
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as day, "
                  "AVG(p.value) AS avg_production "
                  "FROM productions_fact p JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE CONCAT_WS('-', d.year, d.month, d.day) = DATE(:date);")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan viikkokohtainen kokonaistuotto päivittäin ryhmiteltynä:
@production_router.get("/api/measurement/production/total/daily_for_week/{date}")
async def get_total_production_statistics_daily_for_a_week(dw: DW, date: str):
    """
    Get production stats from a given week grouped by day. String format YYYY-MM-DD
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as day, "
                  "SUM(p.value) AS total_production FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND :date "
                  "GROUP BY d.day;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan viikkokohtainen kokonaistuotto:
@production_router.get("/api/measurement/production/total/week/{date}")
async def get_total_production_statistics_for_a_week(dw: DW, date: str):
    """
    Get production stats from a given week. String format YYYY-MM-DD
    """
    _query = text("SELECT SUM(p.value) AS total_production "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND :date;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan viikkokohtainen keskiarvotuotto:
@production_router.get("/api/measurement/production/avg/week/{date}")
async def get_avg_production_statistics_for_a_week(dw: DW, date: str):
    """
    Get production stats from a given week. String format YYYY-MM-DD
    """
    _query = text("SELECT AVG(p.value) AS total_production "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND :date;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan kuukausikohtainen kokonaistuotto päivittäin ryhmiteltynä:
@production_router.get("/api/measurement/production/total/daily_for_month/{date}")
async def get_total_production_statistics_daily_for_a_month(dw: DW, date: str):
    """
    Get production stats from a given month grouped by day. String format YYYY-MM-DD
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as day, "
                  "SUM(p.value) AS total_production FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 MONTH) AND :date "
                  "GROUP BY d.day;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan kuukausikohtainen kokonaistuotto:
@production_router.get("/api/measurement/production/total/month/{date}")
async def get_total_production_statistics_for_a_month(dw: DW, date: str):
    """
    Get production stats from a given month. String format YYYY-MM-DD
    """
    _query = text("SELECT SUM(p.value) AS total_production "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 MONTH) AND :date;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan kuukausikohtainen keskiarvotuotto:
@production_router.get("/api/measurement/production/avg/month/{date}")
async def get_avg_production_statistics_for_a_month(dw: DW, date: str):
    """
    Get production stats from a given month. String format YYYY-MM-DD
    """
    _query = text("SELECT AVG(p.value) AS total_production "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 MONTH) AND :date;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan vuosikohtainen kokonaistuotto kuukausittain ryhmiteltynä:
@production_router.get("/api/measurement/production/total/monthly_for_year/{date}")
async def get_total_production_statistics_monthly_for_a_year(dw: DW, date: str):
    """
    Get production stats from a given year grouped by month. String format YYYY-MM-DD
    """
    _query = text("SELECT CONCAT_WS('-', d.year, d.month) AS year_and_month, "
                  "SUM(p.value) AS total_production "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 YEAR) AND :date "
                  "GROUP BY d.month;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan vuosikohtainen kokonaistuotto:
@production_router.get("/api/measurement/production/total/year/{date}")
async def get_total_production_statistics_for_a_year(dw: DW, date: str):
    """
    Get production stats from a given year. String format YYYY-MM-DD
    """
    _query = text("SELECT SUM(p.value) AS total_production FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 YEAR) AND :date;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan vuosikohtainen keskiarvotuotto:
@production_router.get("/api/measurement/production/avg/year/{date}")
async def get_avg_production_statistics_for_a_year(dw: DW, date: str):
    """
    Get production stats from a given year. String format YYYY-MM-DD
    """
    _query = text("SELECT AVG(p.value) AS total_production FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 YEAR) AND :date;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan kulutus annetusta päivämäärästä 7-päivän jakso taaksepäin, jotka lajitellaan päiväkohtaisesti.
@consumption_router.get("/api/measurement/consumption/total/daily_for_week/{date}")
async def get_total_consumption_statistics_daily_for_a_week(dw: DW, date: str):
    """
    Get daily consumptions of 7-day period. Last day is the given date. String format YYYY-MM-DD
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as day, sum(value) AS total_kwh FROM total_consumptions_fact f JOIN dates_dim d ON d.date_key = f.date_key WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND :date GROUP BY d.day ORDER BY day;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    return {"data": data}
