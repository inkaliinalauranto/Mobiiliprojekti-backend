from fastapi import APIRouter
from sqlalchemy import text

from customfunctions import generate_zero_for_missing_days_in_7_day_period_with_keys, \
    generate_zero_for_missing_hours_in_day_with_keys, generate_zero_for_missing_days_in_week_query, \
    generate_zero_for_missing_days_in_month_query, generate_zero_for_missing_months_in_year_query
from db import DW
import datetime


router = APIRouter(
    prefix='/api/measurement/production/total',
    tags=['Production - Total']
)


# Haetaan 7 edelliseltä päivältä kokonaistuotto, joka ryhmitellään päivittäin.
# Tämä on MainScreenin PANEELIN graafia varten.
@router.get("/seven_day_period/{date}")
async def get_total_production_statistic_daily_seven_day_period(dw: DW, date: str):
    """
    Get production stats (total) from 7 days before the given date
    (7-day period) grouped by day. String format YYYY-MM-DD.
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, "
                  "SUM(p.value) AS total_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(DATE(:date), INTERVAL 6 DAY) AND :date "
                  "GROUP BY d.day "
                  "ORDER BY date;")

    rows = dw.execute(_query, {"date": date})
    fetched_data = rows.mappings().all()
    _date = datetime.datetime.strptime(date, "%Y-%m-%d").date()

    time_key = "date"
    consumption_key = "total_kwh"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        consumption_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_days_in_7_day_period_with_keys(fetched_data, _date, time_key, consumption_key)

    return {"data": data}


# Haetaan päiväkohtainen kokonaistuotto tunneittain ryhmiteltynä:
# Tämä on total production chartin DAY nappia varten.
@router.get("/hourly/{date}")
async def get_total_production_statistic_hourly_by_day(dw: DW, date: str):
    """
    Get production stats (sum) from a given day grouped by hour.
    String format YYYY-MM-DD.
    """
    _query = text("SELECT d.hour, SUM(p.value) AS total_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date "
                  "GROUP BY d.hour "
                  "ORDER BY d.hour;")

    rows = dw.execute(_query, {"date": date})
    fetched_data = rows.mappings().all()
    time_key = "hour"
    consumption_key = "total_kwh"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        consumption_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_hours_in_day_with_keys(fetched_data, time_key, consumption_key)

    return {"data": data}


# Haetaan viikkokohtainen kokonaistuotto päivittäin ryhmiteltynä.
# Tämä on total production chartin WEEK nappia varten.
@router.get("/daily/week/{date}")
async def get_total_production_statistic_daily_by_week(dw: DW, date: str):
    """
    Get production stats from a given week grouped by day. String format YYYY-MM-DD
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, "
                  "SUM(p.value) AS total_kwh FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE WEEK(CONCAT(d.year, '-', d.month, '-', d.day), 1) = WEEK(:date,1) "
                  "GROUP BY d.day;")
    rows = dw.execute(_query, {"date": date})
    fetched_data = rows.mappings().all()

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    data = generate_zero_for_missing_days_in_week_query(fetched_data, _date)

    return {"data": data}


# Haetaan kuukausikohtainen kokonaistuotto päivittäin ryhmiteltynä:
# Tämä on total production chartin MONTH nappia varten.
@router.get("/daily/month/{date}")
async def get_total_production_statistics_daily_for_a_month(dw: DW, date: str):
    """
    Get total production from a given month grouped by day.
    Month is calculated from date string, ISO 8601 format YYYY-MM-DD.
    """

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    year = _date.year
    month = _date.month

    _query = text("SELECT d.day, SUM(p.value) AS total_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE d.year = ':year' AND d.month = ':month' "
                  "GROUP BY d.day;")
    rows = dw.execute(_query, {"year": year, "month": month})
    fetched_data = rows.mappings().all()

    # Generoidaan puuttuvat nollatietueet mukaan dataan ja palautetaan se.
    data = generate_zero_for_missing_days_in_month_query(fetched_data, year, month)

    return {"data": data}


# Haetaan vuosikohtainen kokonaistuotto kuukausittain ryhmiteltynä.
# Tämä on total production chartin YEAR nappia varten.
@router.get("/monthly/{date}")
async def get_total_production_statistic_monthly_by_year(dw: DW, date: str):
    """
    Get production stats from a given year grouped by month. ISO 8601 format YYYY-MM-DD
    """
    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    year = _date.year

    _query = text("SELECT d.month, SUM(p.value) AS total_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE d.year = ':year' "
                  "GROUP BY d.month;")
    rows = dw.execute(_query, {"year": year})
    fetched_data = rows.mappings().all()

    # Generoidaan puuttuvat nollatietueet mukaan dataan ja palautetaan se.
    data = generate_zero_for_missing_months_in_year_query(fetched_data)

    return {"data": data}




