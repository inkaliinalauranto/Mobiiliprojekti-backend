from fastapi import APIRouter
from sqlalchemy import text
from db import DW
from customfunctions import *


router = APIRouter(
    prefix='/api/measurement/consumption/total/sum',
    tags=['Consumption - Total - Sum']
)


# Haetaan 7 päivän jakson SUMMA
# Tämä on MainScreenin PANEELIN graphia varten.
@router.get("/seven_day_period/{date}")
async def get_total_consumption_statistics_sum_seven_day_period(dw: DW, date: str):
    """
    Get consumptions(sum) for a given 7-day period . ISO 8601 format YYYY-MM-DD. String ISO 8601 format YYYY-MM-DD
    """
    _query = text("SELECT sum(value) AS sum_kwh FROM total_consumptions_fact f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 DAY) AND :date ")

    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if data[0]["sum_kwh"] is None:
        data = [{"sum_kwh": 0}]

    return {"data": data}


# Lasketaan päivän summa. Tämä on consumptionScreenin SUMMA kohtaa varten
@router.get("/day/{date}")
async def get_total_consumption_statistic_sum_day(dw: DW, date: str):
    """
    Get hourly consumptions(sum) for a given day . ISO 8601 format YYYY-MM-DD
    """

    _query = text("SELECT sum(f.value) as sum_kwh FROM `total_consumptions_fact` f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date;")

    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if data[0]["sum_kwh"] is None:
        data = [{"sum_kwh": 0}]

    return {"data": data}


# Lasketaan viikon summa. Tämä on consumptionScreenin SUMMA kohtaa varten
@router.get("/week/{date}")
async def get_total_consumption_statistic_sum_week(dw: DW, date: str):
    """
    Get daily consumptions(sum) for a given week . ISO 8601 format YYYY-MM-DD
    """

    _query = text("SELECT sum(f.value) as sum_kwh FROM `total_consumptions_fact` f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE WEEK(CONCAT(d.year, '-', d.month, '-', d.day), 1) = WEEK(:date,1)")

    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if data[0]["sum_kwh"] is None:
        data = [{"sum_kwh": 0}]

    return {"data": data}


# Lasketaan kuukauden päivittäinen summa. Tämä on consumptionScreenin SUMMA kohtaa varten
@router.get("/month/{date}")
async def get_total_consumption_statistic_sum_month(dw: DW, date: str):
    """
    Get daily consumptions(sum) for a given month . ISO 8601 format YYYY-MM-DD
    """
    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    _query = text("SELECT sum(f.value) as sum_kwh FROM `total_consumptions_fact` f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE d.month = :month;")

    rows = dw.execute(_query, {"month": _date.month})
    data = rows.mappings().all()

    if data[0]["sum_kwh"] is None:
        data = [{"sum_kwh": 0}]

    return {"data": data}


# Lasketaan kuukauden päivittäinen keskiarvo. Tämä on consumptionScreenin SUMMA kohtaa varten
@router.get("/year/{date}")
async def get_total_consumption_statistic_sum_year(dw: DW, date: str):
    """
    Get monthly consumptions(sum) for a given year . ISO 8601 format YYYY-MM-DD
    """
    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    year = _date.year

    _query = text("SELECT sum(f.value) as sum_kwh FROM `total_consumptions_fact` f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE d.year = :year;")

    rows = dw.execute(_query, {"year": year})
    data = rows.mappings().all()

    if data[0]["sum_kwh"] is None:
        data = [{"sum_kwh": 0}]

    return {"data": data}
