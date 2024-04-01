from fastapi import APIRouter
from sqlalchemy import text
from db import DW

router = APIRouter(
    prefix='/api/measurement/production/total/avg',
    tags=['Production - Total - Avg']
)


# Haetaan kokonaistuoton keskiarvo tuntia kohden:
# Tämä on total production screenin DAY-näkymän Avg-kohtaa varten.
@router.get("/day/{date}")
async def get_total_production_statistic_avg_day(dw: DW, date: str):
    """
    Get hourly (avg) productions stats for a given day.
    ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT SUM(p.value)/24 as avg_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if data[0]["avg_kwh"] is None:
        data = [{"avg_kwh": 0}]

    return {"data": data}
