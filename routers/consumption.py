from fastapi import APIRouter
from sqlalchemy import text
from db import DW


router = APIRouter(
    prefix='/api/measurement/consumption',
    tags=['Consumption']
)


# Haetaan kulutus annetusta päivämäärästä 7-päivän jakso taaksepäin, jotka lajitellaan päiväkohtaisesti.
# Tämä on MainScreenin PANEELIN graphia varten.
@router.get("/total/seven_days_before_date/{date}")
async def get_total_consumption_statistics_daily_seven_days_before_date(dw: DW, date: str):
    """
    Get daily consumptions(total) from 7 days before the given date (7-day period). String format YYYY-MM-DD
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as day, sum(value) AS total_kwh "
                  "FROM total_consumptions_fact f "
                  "JOIN dates_dim d ON d.date_key = f.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND :date "
                  "GROUP BY d.day "
                  "ORDER BY day;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    return {"data": data}


# Tämä on total consumption chartin DAY nappia varten.
@router.get("/total/hourly/{date}")
async def get_total_consumption_statistic_hourly_by_day(dw: DW, date: str):
    """
    Get hourly consumptions(total) from a given day. String format YYYY-MM-DD
    """
    _query = text("SELECT hours.hour, COALESCE(SUM(total_consumptions_fact.value), 0) AS total_kwh "
                  "FROM ( "
                        "SELECT 0 AS hour UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL "
                        "SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL "
                        "SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL "
                        "SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL "
                        "SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23 "
                  ") AS hours "
                  "LEFT JOIN ( "
                        "SELECT f.value, d.hour "
                        "FROM total_consumptions_fact f "
                        "JOIN dates_dim d ON d.date_key = f.date_key "
                        "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date "
                  ") AS total_consumptions_fact "
                  "ON hours.hour = total_consumptions_fact.hour "
                  "GROUP BY hours.hour "
                  "ORDER BY hours.hour;")

    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    return {"data": data}


# Tämä on total consumption chartin WEEK nappia varten.
@router.get("/total/daily/{date}")
async def get_total_consumption_statistic_daily_by_week(dw: DW, date: str):
    """
    Get daily consumptions(total) from a given week. Week is calculated from given date string, format YYYY-MM-DD
    """
    _query = text("SELECT days.weekday_num, COALESCE(SUM(total_consumptions_fact.value), 0) AS total_kwh "
                  "FROM ( "
                  "    SELECT 0 AS weekday_num UNION ALL "
                  "    SELECT 1 UNION ALL "
                  "    SELECT 2 UNION ALL "
                  "    SELECT 3 UNION ALL "
                  "    SELECT 4 UNION ALL "
                  "    SELECT 5 UNION ALL "
                  "    SELECT 6 "
                  ") AS days "
                  "LEFT JOIN ( "
                  "    SELECT f.value, DAYOFWEEK(CONCAT(d.year, '-', d.month, '-', d.day)) - 2 as day "
                  "    FROM total_consumptions_fact f "
                  "    JOIN dates_dim d ON d.date_key = f.date_key "
                  "    WHERE WEEK(CONCAT(d.year, '-', d.month, '-', d.day), 1) = WEEK(:date, 1) "
                  ") AS total_consumptions_fact "
                  "ON days.weekday_num = total_consumptions_fact.day "
                  "GROUP BY weekday_num;")

    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    return {"data": data}
