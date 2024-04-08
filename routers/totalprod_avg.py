from calendar import monthrange

from fastapi import APIRouter
from sqlalchemy import text
from db import DW
import datetime

router = APIRouter(
    prefix='/api/measurement/production/total/avg',
    tags=['Production - Total - Avg']
)


# Haetaan edellisen 7 päivän ajalta kokonaistuoton keskiarvo päivää kohden.
# Tämä on MainScreenin PANEELIN graphia varten.
@router.get("/seven_day_period/{date}")
async def get_total_production_statistic_avg_seven_day_period(dw: DW, date: str):
    """
    Get day production (avg) for a given 7-day period.
    ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT sum(value)/7 AS avg_kwh FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 DAY) AND :date")

    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if len(data) == 0:
        data = [{"avg_kwh": 0}]

    return {"data": data}


# Haetaan päivän kokonaistuoton keskiarvo tuntia kohden:
# Tämä on total production screenin DAY-näkymän Avg-kohtaa varten.
@router.get("/day/{date}")
async def get_total_production_statistic_avg_day(dw: DW, date: str):
    """
    Get hour (avg) production stats for a given day.
    ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT SUM(p.value)/24 as avg_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if len(data) == 0:
        data = [{"avg_kwh": 0}]

    return {"data": data}


# Haetaan viikon kokonaistuoton keskiarvo päivää kohden:
# Tämä on total production screenin WEEK-näkymän Avg-kohtaa varten.
@router.get("/week/{date}")
async def get_total_production_statistic_avg_week(dw: DW, date: str):
    """
    Get day (avg) production stats for a given week. ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT SUM(p.value)/7 AS avg_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE WEEK(CONCAT(d.year, '-', d.month, '-', d.day), 1) = WEEK(:date, 1)")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if len(data) == 0:
        data = [{"avg_kwh": 0}]

    return {"data": data}


# Haetaan kuukauden kokonaistuoton keskiarvo päivää kohden.
# Tämä on total production screenin MONTH-näkymän Avg-kohtaa varten.
@router.get("/month/{date}")
async def get_total_production_statistic_avg_month(dw: DW, date: str):
    """
    Get day (avg) production stats for a given month.
    ISO 8601 format YYYY-MM-DD.
    """
    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    number_of_days = monthrange(_date.year, _date.month)[1]

    _query = text("SELECT SUM(p.value)/:number_of_days AS avg_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date;")
    rows = dw.execute(_query, {"number_of_days": number_of_days, "date": date})
    data = rows.mappings().all()

    if len(data) == 0:
        data = [{"avg_kwh": 0}]

    return {"data": data}


# Haetaan vuoden kokonaistuoton keskiarvo kuukautta kohden.
# Tämä on total production screenin YEAR-näkymän Avg-kohtaa varten.
@router.get("/year/{date}")
async def get_avg_production_statistics_for_a_year(dw: DW, date: str):
    """
    Get month (avg) production stats for a given year.
    ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT SUM(p.value)/12 AS avg_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE YEAR(CONCAT(d.year, '-', d.month, '-', d.day)) = YEAR(:date);")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if len(data) == 0:
        data = [{"avg_kwh": 0}]

    return {"data": data}

