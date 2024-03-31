from fastapi import APIRouter
from sqlalchemy import text

from customfunctions import generate_zero_for_missing_hours_in_day_query
from db import DW

router = APIRouter(
    prefix='/api/measurement/temperature',
    tags=['Temperature']
)


# Haetaan annetusta ajankohdasta 24 tunnin takaiset keskiarvolämpötilat,
# jotka lajitellaan tuntikohtaisesti.
@router.get("/avg/24_hours_before_date/{date}")
async def get_temperature_statistics_hourly_24_before_date(dw: DW, date: str):
    """
    Get hourly temperatures (avg) from 24 hours before the given date.
    String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, "
                  "d.hour, avg(value) AS temperature "
                  "FROM temperatures_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = DATE(:date) "
                  "GROUP BY d.hour "
                  "ORDER BY d.hour;")
    rows = dw.execute(_query, {"date": date})
    fetched_data = rows.mappings().all()

    data = generate_zero_for_missing_hours_in_day_query(fetched_data)

    return {"data": data}

