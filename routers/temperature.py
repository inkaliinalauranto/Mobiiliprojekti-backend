from fastapi import APIRouter
from sqlalchemy import text
from db import DW

router = APIRouter(
    prefix='/api/measurement/temperature',
    tags=['Temperature']
)


# Haetaan annetun päivämäärän keskiarvolämpötilat, jotka lajitellaan
# tuntikohtaisesti.
@router.get("/avg/hourly_by_a_day/{date}")
async def get_temperature_statistics_hourly_by_day(dw: DW, date: str):
    """
    Get daily temperatures (avg). String ISO 8601 format YYYY-MM-DD
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, avg(value) AS temperature "
                  "FROM temperatures_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date "
                  "GROUP BY d.hour "
                  "ORDER BY d.hour;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    return {"data": data}

