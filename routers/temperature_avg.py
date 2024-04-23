import datetime
from fastapi import APIRouter
from sqlalchemy import text
from db import DW

router = APIRouter(
    prefix='/api/measurement/temperature',
    tags=['Temperature - Indoors - Avg']
)


@router.get("/avg/indoor/day/{date}")
async def get_avg_temperature_by_day(dw: DW, date: str):
    """
        Get avg temperature for a given day.
        String ISO 8601 format YYYY-MM-DD.
    """

    _query = text("SELECT avg(value) as avg_temp FROM temperatures_fact f "
                  "JOIN dates_dim d ON f.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date AND sensor_key = 125 "
                  "GROUP BY d.hour;")

    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    avg_sum = 0

    for item in data:
        avg_sum += item["avg_temp"]

    if len(data) == 0:
        avg = 0
    else:
        avg = avg_sum / len(data)

    return {"data": {"avg_temp": avg}}


@router.get("/avg/indoor/week/{date}")
async def get_avg_temperature_by_week(dw: DW, date: str):
    """
        Get avg temperature for a given week.
        String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT AVG(t.value) AS avg_temp "
                  "FROM temperatures_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE (WEEK(CONCAT_WS('-', d.year, d.month, d.day), 1) = WEEK(:date, 1) "
                  "AND t.sensor_key = 125) "
                  "GROUP BY d.day;")

    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    avg_sum = 0

    for item in data:
        avg_sum += item["avg_temp"]

    if len(data) == 0:
        avg = 0
    else:
        avg = avg_sum / len(data)

    return {"data": {"avg_temp": avg}}


@router.get("/avg/indoor/month/{date}")
async def get_avg_temperature_by_month(dw: DW, date: str):
    """
        Get avg temperature for a given month.
        String ISO 8601 format YYYY-MM-DD.
    """
    month = datetime.datetime.strptime(date, '%Y-%m-%d').date().month
    year = datetime.datetime.strptime(date, '%Y-%m-%d').date().year

    _query = text("SELECT AVG(value) as avg_temp FROM temperatures_fact f "
                  "JOIN dates_dim d ON f.date_key = d.date_key "
                  "WHERE (d.month = :month AND d.year = :year "
                  "AND sensor_key = 125) GROUP BY d.day;")

    rows = dw.execute(_query, {"month": month, "year": year})
    data = rows.mappings().all()

    avg_sum = 0

    for item in data:
        avg_sum += item["avg_temp"]

    if len(data) == 0:
        avg = 0
    else:
        avg = avg_sum / len(data)

    return {"data": {"avg_temp": avg}}


@router.get("/avg/indoor/year/{date}")
async def get_avg_temperature_by_year(dw: DW, date: str):
    """
        Get avg temperature for a given year.
        String ISO 8601 format YYYY-MM-DD.
    """
    year = datetime.datetime.strptime(date, '%Y-%m-%d').date().year

    _query = text("SELECT avg(value) as avg_temp FROM temperatures_fact f "
                  "JOIN dates_dim d ON f.date_key = d.date_key "
                  "WHERE d.year = :year "
                  "AND sensor_key = 125 GROUP BY d.month;")

    rows = dw.execute(_query, {"year": year})
    data = rows.mappings().all()

    avg_sum = 0

    for item in data:
        avg_sum += item["avg_temp"]

    if len(data) == 0:
        avg = 0
    else:
        avg = avg_sum / len(data)

    return {"data": {"avg_temp": avg}}
