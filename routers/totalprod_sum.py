from fastapi import APIRouter
from sqlalchemy import text
from db import DW

router = APIRouter(
    prefix='/api/measurement/production/total/sum',
    tags=['Production - Total - Sum']
)


# Haetaan päiväkohtainen kokonaistuotto.
# Tämä on total production screenin DAY-näkymän Total-kohtaa varten.
@router.get("/day/{date}")
async def get_total_production_statistics_sum_day(dw: DW, date: str):
    """
    Get production stats (sum) from a given day. ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT SUM(p.value) AS sum_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = :date;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if data[0]["sum_kwh"] is None:
        data = [{"sum_kwh": 0}]

    return {"data": data}
