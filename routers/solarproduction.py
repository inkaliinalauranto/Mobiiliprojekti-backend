from fastapi import APIRouter
from sqlalchemy import text
from db import DW
import datetime

from customfunctions import generate_zero_for_missing_days_in_7_day_period_with_keys, \
    generate_zero_for_missing_hours_in_day_with_keys, generate_zero_for_missing_days_in_week_query, \
    generate_zero_for_missing_days_in_month_query, generate_zero_for_missing_months_in_year_query

router = APIRouter(
    prefix='/api/measurement/solar',
    tags=['Solar']
)

# Haetaan 7 edelliseltä päivältä solar tuotto, ryhmitetty päivittäin.
@router.get("/total/seven_day_period/{date}")
async def get_total_solar_production_seven_day_period(dw: DW, date: str):
    """
    Get production stats (solar) from 7 days before the given date
    (7-day period) grouped by day. String format YYYY-MM-DD.
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, "
                  "SUM(p.value) AS total_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(DATE(:date), INTERVAL 6 DAY) AND :date "
                  "AND p.sensor_key = :sensor_key "
                  "GROUP BY date;"
    )
    rows = dw.execute(_query, {"date": date, "sensor_key": 276})

    fetched_data = rows.mappings().all()
    _date = datetime.datetime.strptime(date, "%Y-%m-%d").date()

    time_key = "date"
    solar_unit_key = "total_kwh"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        solar_unit_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_days_in_7_day_period_with_keys(fetched_data, _date, time_key, solar_unit_key)

    return{"data": data}

# Haetaan päiväkohtainen solar tuotto, ryhmitetty tunneittain.
@router.get("/total/hourly/{date}")
async def get_total_solar_production_hourly_by_day(dw: DW, date: str):
    """
    Get production stats (solar) for a given day grouped by hour.
    String format YYYY-MM-DD.
    """
    _query = text("SELECT d.hour, SUM(p.value) AS total_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date "
                  "AND p.sensor_key = :sensor_key "
                  "GROUP BY d.hour "
                  "ORDER BY d.hour;"
    )
    rows = dw.execute(_query, {"date": date, "sensor_key": 276})

    fetched_data = rows.mappings().all()

    time_key = "hour"
    solar_unit_key = "total_kwh"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        solar_unit_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_hours_in_day_with_keys(fetched_data, time_key, solar_unit_key)

    return{"data": data}

# Haetaan viikkokohtainen solar tuotto, ryhmitetty päivittäin.
@router.get("/total/daily/week/{date}")
async def get_total_solar_production_daily_by_week(dw: DW, date: str):
    """
    Get production stats (solar) for a given week grouped by day.
    String format YYYY-MM-DD.
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, "
                  "SUM(p.value) AS total_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE WEEK(DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))), 1) = WEEK(:date, 1) "
                  "AND p.sensor_key = :sensor_key "
                  "GROUP BY d.day;"
    )
    rows = dw.execute(_query, {"date": date, "sensor_key": 276})

    fetched_data = rows.mappings().all()

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    data = generate_zero_for_missing_days_in_week_query(fetched_data, _date)

    return{"data": data}

# Haetaan kuukausikohtainen solar tuotto, ryhmitetty päivittäin.
@router.get("/total/daily/month/{date}")
async def get_total_solar_production_daily_by_month(dw: DW, date: str):
    """
    Get production stats (solar) for a given month grouped by day.
    Month is calculated from a date string. String format YYYY-MM-DD.
    """

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    year = _date.year
    month = _date.month

    _query = text("SELECT d.day, SUM(p.value) AS total_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE d.year = ':year' AND d.month = ':month' "
                  "AND p.sensor_key = :sensor_key "
                  "GROUP BY d.day;"
    )
    rows = dw.execute(_query, {"year": year, "month": month, "sensor_key": 276})

    fetched_data = rows.mappings().all()

    data = generate_zero_for_missing_days_in_month_query(fetched_data, year, month)

    return{"data": data}

# Haetaan vuosikohtainen solar tuotto, ryhmitetty kuukausittain.
@router.get("/total/monthly/{date}")
async def get_total_solar_production_monthly_by_year(dw: DW, date: str):
    """
    Get production stats (solar) for a given year grouped by month.
    String format YYYY-MM-DD.
    """

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    year = _date.year

    _query = text("SELECT d.month, SUM(p.value) AS total_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE d.year = ':year' "
                  "AND p.sensor_key = :sensor_key "
                  "GROUP BY d.month;"
    )
    rows = dw.execute(_query, {"year": year, "sensor_key": 276})

    fetched_data = rows.mappings().all()

    data = generate_zero_for_missing_months_in_year_query(fetched_data)

    return{"data": data}

