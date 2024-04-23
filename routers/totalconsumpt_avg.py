from fastapi import APIRouter
from sqlalchemy import text
from db import DW
from customfunctions import *

router = APIRouter(
    prefix='/api/measurement/consumption/total/avg',
    tags=['Consumption - Total - Avg']
)


# Haetaan 7 päivän jakson AVG
# Tämä on MainScreenin PANEELIN graphia varten.
@router.get("/seven_day_period/{date}")
async def get_total_consumption_statistic_avg_seven_day_period(dw: DW, date: str):
    """
    Get daily consumptions(avg) for a given 7-day period . ISO 8601 format YYYY-MM-DD
    """
    _query = text("SELECT sum(value)/7 AS avg_kwh FROM total_consumptions_fact f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 DAY) AND :date")

    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if data[0]["avg_kwh"] is None:
        data = [{"avg_kwh": 0}]

    return {"data": data}


# Lasketaan päivän tunnittainen keskiarvo. Tämä on consumptionScreenin AVG kohtaa varten
@router.get("/day/{date}")
async def get_total_consumption_statistic_avg_day(dw: DW, date: str):
    """
    Get hourly consumptions(avg) for a given day . ISO 8601 format YYYY-MM-DD
    """
    _current_hour_count_query = text("SELECT COUNT(*) AS record_count FROM "
                                     "(SELECT d.hour as hour, sum(f.value) as total_kwh "
                                     "FROM total_consumptions_fact f "
                                     "JOIN dates_dim d ON d.date_key = f.date_key "
                                     "JOIN sensors_dim s ON s.sensor_key = f.sensor_key "
                                     "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date "
                                     "GROUP BY d.hour) "
                                     "AS subquery;")
    count_rows = dw.execute(_current_hour_count_query, {"date": date})
    count_data = count_rows.mappings().all()
    count = 1
    if len(count_data) != 0:
        count = count_data[0]["record_count"]

    # print(f"Tuntien lukumäärä: {count}")

    _query = text("SELECT sum(f.value)/:count as avg_kwh FROM `total_consumptions_fact` f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date;")

    rows = dw.execute(_query, {"count": count, "date": date})
    data = rows.mappings().all()

    if data[0]["avg_kwh"] is None:
        data = [{"avg_kwh": 0}]

    return {"data": data}


# Lasketaan viikon päivittäinen keskiarvo. Tämä on consumptionScreenin AVG kohtaa varten
@router.get("/week/{date}")
async def get_total_consumption_statistic_avg_week(dw: DW, date: str):
    """
    Get daily consumptions(avg) for a given week . ISO 8601 format YYYY-MM-DD
    """
    _current_day_count_query = text("SELECT COUNT(*) AS record_count FROM "
                                    "(SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, "
                                    "sum(f.value) as total_kwh FROM total_consumptions_fact f "
                                    "JOIN dates_dim d ON d.date_key = f.date_key "
                                    "WHERE WEEK(CONCAT(d.year, '-', d.month, '-', d.day), 1) = WEEK(:date, 1) "
                                    "GROUP BY d.day) "
                                    "AS subquery;")
    count_rows = dw.execute(_current_day_count_query, {"date": date})
    count_data = count_rows.mappings().all()
    count = 1
    if len(count_data) != 0:
        count = count_data[0]["record_count"]

    # print(f"Päivien lukumäärä: {count}")

    _query = text("SELECT sum(f.value)/:count as avg_kwh FROM `total_consumptions_fact` f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE WEEK(CONCAT(d.year, '-', d.month, '-', d.day), 1) = WEEK(:date, 1)")

    rows = dw.execute(_query, {"count": count, "date": date})
    data = rows.mappings().all()

    if data[0]["avg_kwh"] is None:
        data = [{"avg_kwh": 0}]

    return {"data": data}


# Lasketaan kuukauden päivittäinen keskiarvo. Tämä on consumptionScreenin AVG kohtaa varten
@router.get("/month/{date}")
async def get_total_consumption_statistic_avg_month(dw: DW, date: str):
    """
    Get daily consumptions(avg) for a given month . ISO 8601 format YYYY-MM-DD
    """
    _current_day_count_query = text("SELECT COUNT(*) AS record_count FROM "
                                    "(SELECT d.day, sum(f.value) as total_kwh "
                                    "FROM total_consumptions_fact f "
                                    "JOIN dates_dim d ON d.date_key = f.date_key "
                                    "WHERE d.year = YEAR(:date) AND d.month = MONTH(:date) "
                                    "GROUP BY d.day) "
                                    "AS subquery;")
    count_rows = dw.execute(_current_day_count_query, {"date": date})
    count_data = count_rows.mappings().all()
    count = 1
    if len(count_data) != 0:
        count = count_data[0]["record_count"]

    # print(f"Päivien lukumäärä: {count}")

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    number_of_days = monthrange(_date.year, _date.month)[1]

    _query = text("SELECT sum(f.value)/:number_of_days as avg_kwh FROM `total_consumptions_fact` f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE d.month = MONTH(:date) AND d.year = YEAR(:date);")

    rows = dw.execute(_query, {"number_of_days": count, "date": date})
    data = rows.mappings().all()

    if data[0]["avg_kwh"] is None:
        data = [{"avg_kwh": 0}]

    return {"data": data}


# Lasketaan kuukauden päivittäinen keskiarvo. Tämä on consumptionScreenin AVG kohtaa varten
@router.get("/year/{date}")
async def get_total_consumption_statistic_avg_year(dw: DW, date: str):
    """
    Get monthly consumptions(avg) for a given year . ISO 8601 format YYYY-MM-DD
    """
    _current_month_count_query = text("SELECT COUNT(*) AS record_count FROM "
                                      "(SELECT d.month, sum(f.value) as total_kwh "
                                      "FROM total_consumptions_fact f "
                                      "JOIN dates_dim d ON d.date_key = f.date_key "
                                      "WHERE d.year = :date GROUP BY d.month) "
                                      "AS subquery;")
    count_rows = dw.execute(_current_month_count_query, {"date": date})
    count_data = count_rows.mappings().all()
    count = 1
    if len(count_data) != 0:
        count = count_data[0]["record_count"]

    # print(f"Kuukausien lukumäärä: {count}")

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    year = _date.year

    _query = text("SELECT sum(f.value)/:count as avg_kwh FROM `total_consumptions_fact` f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE d.year = :year;")

    rows = dw.execute(_query, {"count": count, "year": year})
    data = rows.mappings().all()

    if data[0]["avg_kwh"] is None:
        data = [{"avg_kwh": 0}]

    return {"data": data}
