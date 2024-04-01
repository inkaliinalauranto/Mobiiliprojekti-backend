import datetime
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
async def get_total_production_statistic_sum_day(dw: DW, date: str):
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


# Haetaan viikkokohtainen kokonaistuotto:
# Tämä on total production screenin WEEK-näkymän Total-kohtaa varten.
@router.get("/week/{date}")
async def get_total_production_statistic_sum_week(dw: DW, date: str):
    """
    Get production (sum) stats for a given week. ISO 8601 format YYYY-MM-DD
    """
    _query = text("SELECT SUM(p.value) AS sum_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE WEEK(CONCAT_WS('-', d.year, d.month, d.day), 1) = WEEK(:date, 1);")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if data[0]["sum_kwh"] is None:
        data = [{"sum_kwh": 0}]

    return {"data": data}


# Haetaan kuukausikohtainen kokonaistuotto.
# Tämä on total production screenin MONTH-näkymän Total-kohtaa varten.
@router.get("/month/{date}")
async def get_total_production_statistic_sum_month(dw: DW, date: str):
    """
    Get production stats from a given month. String format YYYY-MM-DD
    """
    _query = text("SELECT SUM(p.value) AS sum_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE MONTH(CONCAT_WS('-', d.year, d.month, d.day)) = MONTH(:date);")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()

    if data[0]["sum_kwh"] is None:
        data = [{"sum_kwh": 0}]

    return {"data": data}


# Haetaan vuosikohtainen kokonaistuotto.
# Tämä on total production screenin YEAR-näkymän Total-kohtaa varten.
@router.get("/year/{date}")
async def get_total_production_statistic_sum_year(dw: DW, date: str):
    """
    Get production stats from a given year. ISO 8601 format YYYY-MM-DD.
    """
    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    year = _date.year

    _query = text("SELECT SUM(p.value) AS sum_kwh "
                  "FROM productions_fact p "
                  "JOIN dates_dim d ON p.date_key = d.date_key "
                  "WHERE d.year = ':year';")
    rows = dw.execute(_query, {"year": year})
    data = rows.mappings().all()

    if data[0]["sum_kwh"] is None:
        data = [{"sum_kwh": 0}]

    return {"data": data}
