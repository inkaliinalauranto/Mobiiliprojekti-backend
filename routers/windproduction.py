import datetime
from fastapi import APIRouter
from sqlalchemy import text
from customfunctions import generate_zero_for_missing_hours_in_day_with_keys, \
    generate_zero_for_missing_days_in_week_query_with_keys, generate_zero_for_missing_days_in_month_query_with_keys, \
    generate_zero_for_missing_months_in_year_query_with_keys, generate_zero_for_missing_days_in_7_day_period_with_keys
from db import DW

router = APIRouter(
    prefix='/api/measurement/wind',
    tags=['Wind']
)

# Haetaan viimeisimmät tuuligeneraattoritiedot eri sensoreista:
@router.get("/currents")
async def get_most_recent_wind_data(dw: DW):
    """
    Get the most recent wind generation information.
    """
    _timestamps_query = text("SELECT TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day), "
                             "CONCAT_WS(':', d.hour, d.min, d.sec)) AS timestamp "
                             "FROM sensors_dim s "
                             "JOIN productions_fact w ON s.sensor_key = w.sensor_key "
                             "JOIN dates_dim d ON w.date_key = d.date_key "
                             "WHERE (w.date_key, w.sensor_key) IN "
                             "(SELECT MAX(date_key), sensor_key FROM productions_fact GROUP BY sensor_key) "
                             "AND w.sensor_key = :wind_sensor_key;")
    timestamp_rows = dw.execute(_timestamps_query, {
        "wind_sensor_key": 286
    })

    timestamp_data = timestamp_rows.mappings().all()
    dates = [timestamp["timestamp"] for timestamp in timestamp_data]
    oldest_timestamp = str(min(dates))

    _sensor_data_query = text("SELECT CONCAT_WS(': ', s.device_name, s.sensor_name) AS sensor, "
                              "s.sensor_id, w.value AS kWh "
                              "FROM sensors_dim s "
                              "JOIN productions_fact w ON s.sensor_key = w.sensor_key "
                              "JOIN dates_dim d ON w.date_key = d.date_key "
                              "WHERE (w.date_key, w.sensor_key) IN "
                              "(SELECT MAX(date_key), sensor_key FROM productions_fact GROUP BY sensor_key) "
                              "AND w.sensor_key = :wind_sensor_key;")
    sensor_data_rows = dw.execute(_sensor_data_query, {
        "wind_sensor_key": 286
    })
    sensor_data = sensor_data_rows.mappings().all()
    formatted_data = [{"oldest_time": oldest_timestamp}, sensor_data]

    return {"data": formatted_data}


# Haetaan edellisten 7 päivän keskiarvo tuuli generaattori tuotolle, jotka lajitellaan
# päiväkohtaisesti. Tämä on MainScreenin PANEELIN graafia varten.
@router.get("/avg/wind_production/seven_day_period/{date}")
async def get_avg_wind_production_seven_day_period(dw: DW, date: str):
    """
    Get daily wind_productions (avg) from 7 days before the given date
    (7-day period) grouped by day. String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, AVG(t.value) AS avg_C "
                  "FROM productions_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(DATE(:date), INTERVAL 6 DAY) AND :date "
                  "AND t.sensor_key = :sensor_key "
                  "GROUP BY date;")
    rows = dw.execute(_query, {"date": date, "sensor_key": 286})
    fetched_data = rows.mappings().all()

    time_key = "date"
    wind_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        wind_unit_key = tuple(fetched_data[0].keys())[1]

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    data = generate_zero_for_missing_days_in_7_day_period_with_keys(fetched_data, _date, time_key, wind_unit_key)

    return {"data": data}


# Haetaan annetun päivän keskiarvo tuuli generaattori tuotolle, jotka lajitellaan
# tuntikohtaisesti. Tämä on total consumption chartin DAY nappia varten.
@router.get("/avg/wind_production/hourly/{date}")
async def get_avg_wind_production_hourly_by_day(dw: DW, date: str):
    """
    Get hourly wind_productions (avg) from a given day.
    String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT d.hour, AVG(t.value) AS avg_C "
                  "FROM productions_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = DATE(:date) "
                  "AND t.sensor_key = :sensor_key "
                  "GROUP BY d.hour "
                  "ORDER BY d.hour;")
    rows = dw.execute(_query, {"date": date, "sensor_key": 286})
    fetched_data = rows.mappings().all()

    time_key = "hour"
    wind_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        wind_unit_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_hours_in_day_with_keys(fetched_data, time_key, wind_unit_key)

    return {"data": data}


# Haetaan annetun viikon keskiarvo tuuli generaattori tuotolle, jotka lajitellaan
# päiväkohtaisesti. Tämä on total consumption chartin WEEK nappia varten.
@router.get("/avg/wind_production/daily/week/{date}")
async def get_avg_wind_production_daily_by_week(dw: DW, date: str):
    """
    Get daily wind_productions (avg) from a given week.
    String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, AVG(t.value) AS avg_C "
                  "FROM productions_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE WEEK(DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))), 1) = WEEK(:date, 1) "
                  "AND t.sensor_key = :sensor_key "
                  "GROUP BY date;")
    rows = dw.execute(_query, {"date": date, "sensor_key": 286})
    fetched_data = rows.mappings().all()

    time_key = "date"
    wind_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        wind_unit_key = tuple(fetched_data[0].keys())[1]

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    data = generate_zero_for_missing_days_in_week_query_with_keys(fetched_data, _date, time_key, wind_unit_key)

    return {"data": data}


# Haetaan annetun kuukauden keskiarvo tuuli generaattori tuotolle, jotka lajitellaan
# päiväkohtaisesti. Tämä on total consumption chartin MONTH-nappia varten.
@router.get("/avg/wind_production/daily/month/{date}")
async def get_avg_wind_production_daily_by_month(dw: DW, date: str):
    """
    Get daily wind_productions (avg) from a given month.
    String ISO 8601 format YYYY-MM-DD.
    """

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    year = _date.year
    month = _date.month

    _query = text("SELECT d.day, AVG(t.value) AS avg_C "
                  "FROM productions_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE d.year = ':year' AND d.month = ':month' "
                  "AND t.sensor_key = :sensor_key "
                  "GROUP BY d.day;")
    rows = dw.execute(_query, {"year": year, "month": month, "sensor_key": 286})
    fetched_data = rows.mappings().all()

    time_key = "day"
    wind_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        wind_unit_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_days_in_month_query_with_keys(fetched_data, year, month, time_key,
                                                                   wind_unit_key)

    return {"data": data}


# Haetaan annetun vuoden keskiarvo tuuli generaattori tuotolle, jotka lajitellaan
# kuukausikohtaisesti. Tämä on total consumption chartin YEAR-nappia varten.
@router.get("/avg/wind_production/monthly/{date}")
async def get_avg_wind_production_monthly_by_year(dw: DW, date: str):
    """
    Get monthly wind_productions (avg) for a given year.
    String ISO 8601 format YYYY-MM-DD.
    """

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    year = _date.year

    _query = text("SELECT d.month, AVG(t.value) AS avg_C "
                  "FROM productions_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE d.year = ':year' AND t.sensor_key = :sensor_key "
                  "GROUP BY d.month;")
    rows = dw.execute(_query, {"year": year, "sensor_key": 286})
    fetched_data = rows.mappings().all()

    time_key = "month"
    wind_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        wind_unit_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_months_in_year_query_with_keys(fetched_data, time_key, wind_unit_key)

    return {"data": data}