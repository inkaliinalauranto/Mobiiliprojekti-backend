from fastapi import APIRouter
from sqlalchemy import text
from db import DW
from customfunctions import *


router = APIRouter(
    prefix='/api/measurement/consumption/total',
    tags=['Consumption - Total']
)


# Haetaan kulutus annetusta päivämäärästä 7-päivän jakso taaksepäin, jotka lajitellaan päiväkohtaisesti.
# Tämä on MainScreenin PANEELIN graphia varten.
@router.get("/seven_day_period/{date}")
async def get_total_consumption_statistics_daily_seven_day_period(dw: DW, date: str):
    """
    Get daily consumptions(total) from 7 days before the given date (7-day period). String ISO 8601 format YYYY-MM-DD
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, sum(value) AS total_kwh "
                  "FROM total_consumptions_fact f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND :date "
                  "GROUP BY d.day "
                  "ORDER BY date;")

    rows = dw.execute(_query, {"date": date})
    fetched_data = rows.mappings().all()

    data = generate_zero_for_missing_days_in_7_day_period(fetched_data)

    return {"data": data}


# Tämä on total consumption chartin DAY nappia varten.
@router.get("/hourly/{date}")
async def get_total_consumption_statistic_hourly_by_day(dw: DW, date: str):
    """
    Get hourly consumptions(total) from a given day. String ISO 8601 format YYYY-MM-DD
    """
    _query = text("SELECT d.hour as hour, sum(f.value) as total_kwh FROM total_consumptions_fact f "
                  "LEFT JOIN dates_dim d ON d.date_key = f.date_key "
                  "LEFT JOIN sensors_dim s ON s.sensor_key = f.sensor_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date "
                  "GROUP BY d.hour;")

    rows = dw.execute(_query, {"date": date})
    fetched_data = rows.mappings().all()

    data = generate_zero_for_missing_hours_in_day_query(fetched_data)

    return {"data": data}


# Tämä on total consumption chartin WEEK nappia varten.
@router.get("/daily/week/{date}")
async def get_total_consumption_statistic_daily_by_week(dw: DW, date: str):
    """
    Get daily consumptions(total) from a given week.
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, sum(f.value) as total_kwh "
                  "FROM total_consumptions_fact f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE WEEK(CONCAT(d.year, '-', d.month, '-', d.day), 1) = WEEK(:date,1) "
                  "GROUP BY d.day;")

    rows = dw.execute(_query, {"date": date})
    fetched_data = rows.mappings().all()

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    data = generate_zero_for_missing_days_in_week_query(fetched_data, _date)

    return {"data": data}


# Tämä on total consumption chartin MONTH nappia varten
@router.get("/daily/month/{date}")
async def get_total_consumption_statistic_daily_by_month(dw: DW, date: str):
    """
    Get daily consumptions(total) from a given month. Month is calculated from date string, ISO 8601 format YYYY-MM-DD
    """

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    year = _date.year
    month = _date.month

    _query = text("SELECT d.day, sum(f.value) as total_kwh FROM total_consumptions_fact f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE d.year = ':year' AND d.month = ':month'"
                  "GROUP BY d.day;")

    rows = dw.execute(_query, {"year": year, "month": month})
    fetched_data = rows.mappings().all()

    # Generoidaan puuttuvat nolla tietueet mukaan dataan ja palautetaan se.
    data = generate_zero_for_missing_days_in_month_query(fetched_data, year, month)
    return {"data": data}


# Tämä on total consumption chartin YEAR nappia varten
@router.get("/monthly/{date}")
async def get_total_consumption_statistic_monthly_by_year(dw: DW, date: str):
    """
    Get monthly consumptions(total) from a given year. ISO 8601 format YYYY-MM-DD
    """

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    year = _date.year

    _query = text("SELECT d.month, sum(f.value) as total_kwh FROM total_consumptions_fact f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE d.year = ':year'"
                  "GROUP BY d.month;")

    rows = dw.execute(_query, {"year": year})
    fetched_data = rows.mappings().all()

    # Generoidaan puuttuvat nolla tietueet mukaan dataan ja palautetaan se.
    data = generate_zero_for_missing_months_in_year_query(fetched_data)

    return {"data": data}

