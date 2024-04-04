import datetime
from fastapi import APIRouter
from sqlalchemy import text
from customfunctions import generate_zero_for_missing_hours_in_day_with_keys, \
    generate_zero_for_missing_days_in_week_query_with_keys, generate_zero_for_missing_days_in_month_query_with_keys, \
    generate_zero_for_missing_months_in_year_query_with_keys, generate_zero_for_missing_days_in_7_day_period_with_keys
from db import DW

router = APIRouter(
    prefix='/api/measurement/temperature',
    tags=['Temperature']
)


# Haetaan viimeisimmät lämpötilatiedot eri sensoreista:
@router.get("/currents")
async def get_most_recent_temperatures(dw: DW):
    """
    Get the most recent temperature information.
    """
    _timestamps_query = text("SELECT TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day), "
                             "CONCAT_WS(':', d.hour, d.min, d.sec)) AS timestamp "
                             "FROM sensors_dim s "
                             "JOIN temperatures_fact t ON s.sensor_key = t.sensor_key "
                             "JOIN dates_dim d ON t.date_key = d.date_key "
                             "WHERE (t.date_key, t.sensor_key) IN "
                             "(SELECT MAX(date_key), sensor_key FROM temperatures_fact GROUP BY sensor_key) "
                             "AND t.sensor_key IN "
                             "(:indoor_sensor_key, :outdoor_mast_sensor_key, :tb_indoor_sensor_key, :wc_indoor_sensor_key);")
    timestamp_rows = dw.execute(_timestamps_query, {
        "indoor_sensor_key": 125,
        "outdoor_mast_sensor_key": 229,
        "tb_indoor_sensor_key": 116,
        "wc_indoor_sensor_key": 7
    })

    timestamp_data = timestamp_rows.mappings().all()
    dates = [timestamp["timestamp"] for timestamp in timestamp_data]
    oldest_timestamp = str(min(dates))

    _sensor_data_query = text("SELECT CONCAT_WS(': ', s.device_name, s.sensor_name) AS sensor, "
                              "s.sensor_id, t.value AS C "
                              "FROM sensors_dim s "
                              "JOIN temperatures_fact t ON s.sensor_key = t.sensor_key "
                              "JOIN dates_dim d ON t.date_key = d.date_key "
                              "WHERE (t.date_key, t.sensor_key) IN "
                              "(SELECT MAX(date_key), sensor_key FROM temperatures_fact GROUP BY sensor_key) "
                              "AND t.sensor_key IN "
                              "(:indoor_sensor_key, :outdoor_mast_sensor_key, "
                              ":tb_indoor_sensor_key, :wc_indoor_sensor_key);")
    sensor_data_rows = dw.execute(_sensor_data_query, {
        "indoor_sensor_key": 125,
        "outdoor_mast_sensor_key": 229,
        "tb_indoor_sensor_key": 116,
        "wc_indoor_sensor_key": 7
    })
    sensor_data = sensor_data_rows.mappings().all()
    formatted_data = [{"oldest_time": oldest_timestamp}, sensor_data]

    return {"data": formatted_data}


# Haetaan edellisten 7 päivän keskiarvolämpötilat, jotka lajitellaan
# päiväkohtaisesti. Tämä on MainScreenin PANEELIN graafia varten.
@router.get("/avg/indoor/seven_day_period/{date}")
async def get_indoor_avg_temperature_statistic_seven_day_period(dw: DW, date: str):
    """
    Get daily temperatures (avg) from 7 days before the given date
    (7-day period) grouped by day. String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, AVG(t.value) AS avg_C "
                  "FROM temperatures_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) "
                  "BETWEEN DATE_SUB(DATE(:date), INTERVAL 7 DAY) AND :date "
                  "AND t.sensor_key = :sensor_key "
                  "GROUP BY date;")
    rows = dw.execute(_query, {"date": date, "sensor_key": 125})
    fetched_data = rows.mappings().all()

    time_key = "date"
    temperature_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        temperature_unit_key = tuple(fetched_data[0].keys())[1]

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    data = generate_zero_for_missing_days_in_7_day_period_with_keys(fetched_data, _date, time_key, temperature_unit_key)

    return {"data": data}


# Haetaan annetun päivän keskiarvolämpötilat, jotka lajitellaan
# tuntikohtaisesti. Tämä on total consumption chartin DAY nappia varten.
@router.get("/avg/indoor/hourly/{date}")
async def get_indoor_avg_temperature_statistic_hourly_by_day(dw: DW, date: str):
    """
    Get hourly temperatures (avg) from a given day.
    String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT d.hour, AVG(t.value) AS avg_C "
                  "FROM temperatures_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = DATE(:date) "
                  "AND t.sensor_key = :sensor_key "
                  "GROUP BY d.hour "
                  "ORDER BY d.hour;")
    rows = dw.execute(_query, {"date": date, "sensor_key": 125})
    fetched_data = rows.mappings().all()

    time_key = "hour"
    temperature_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        temperature_unit_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_hours_in_day_with_keys(fetched_data, time_key, temperature_unit_key)

    return {"data": data}


# Haetaan annetun viikon keskiarvolämpötilat, jotka lajitellaan
# päiväkohtaisesti. Tämä on total consumption chartin WEEK nappia varten.
@router.get("/avg/indoor/daily/week/{date}")
async def get_indoor_avg_temperature_statistic_daily_by_week(dw: DW, date: str):
    """
    Get daily temperatures (avg) from a given week.
    String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as date, AVG(t.value) AS avg_C "
                  "FROM temperatures_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE WEEK(DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))), 1) = WEEK(:date, 1) "
                  "AND t.sensor_key = :sensor_key "
                  "GROUP BY date;")
    rows = dw.execute(_query, {"date": date, "sensor_key": 125})
    fetched_data = rows.mappings().all()

    time_key = "date"
    temperature_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        temperature_unit_key = tuple(fetched_data[0].keys())[1]

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    data = generate_zero_for_missing_days_in_week_query_with_keys(fetched_data, _date, time_key, temperature_unit_key)

    return {"data": data}


# Haetaan annetun kuukauden keskiarvolämpötilat, jotka lajitellaan
# päiväkohtaisesti. Tämä on total consumption chartin MONTH-nappia varten.
@router.get("/avg/indoor/daily/month/{date}")
async def get_indoor_avg_temperature_statistic_daily_by_month(dw: DW, date: str):
    """
    Get daily temperatures (avg) from a given month.
    String ISO 8601 format YYYY-MM-DD.
    """

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    year = _date.year
    month = _date.month

    _query = text("SELECT d.day, AVG(t.value) AS avg_C "
                  "FROM temperatures_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE d.year = ':year' AND d.month = ':month' "
                  "AND t.sensor_key = :sensor_key "
                  "GROUP BY d.day;")
    rows = dw.execute(_query, {"year": year, "month": month, "sensor_key": 125})
    fetched_data = rows.mappings().all()

    time_key = "day"
    temperature_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        temperature_unit_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_days_in_month_query_with_keys(fetched_data, year, month, time_key,
                                                                   temperature_unit_key)

    return {"data": data}


# Haetaan annetun vuoden keskiarvolämpötilat, jotka lajitellaan
# kuukausikohtaisesti. Tämä on total consumption chartin YEAR-nappia varten.
@router.get("/avg/indoor/monthly/{date}")
async def get_indoor_avg_temperature_statistic_monthly_by_year(dw: DW, date: str):
    """
    Get monthly temperatures (avg) for a given year.
    String ISO 8601 format YYYY-MM-DD.
    """

    _date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    year = _date.year

    _query = text("SELECT d.month, AVG(t.value) AS avg_C "
                  "FROM temperatures_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE d.year = ':year' AND t.sensor_key = :sensor_key "
                  "GROUP BY d.month;")
    rows = dw.execute(_query, {"year": year, "sensor_key": 125})
    fetched_data = rows.mappings().all()

    time_key = "month"
    temperature_unit_key = "avg_C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        temperature_unit_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_months_in_year_query_with_keys(fetched_data, time_key, temperature_unit_key)

    return {"data": data}

# # Testi
# @router.get("/indoor/wind/nothing")
# async def get_most_recent_wind_nothing(dw: DW):
#     """
#     Get the most recent winfinformation.
#     String ISO 8601 format YYYY-MM-DD.
#     """
#     _query = text("SELECT p.value FROM productions_fact p "
#                   "JOIN sensors_dim s ON p.sensor_key = s.sensor_key "
#                   "WHERE p.date_key = (SELECT MAX(date_key) FROM productions_fact WHERE sensor_key =  286);")
#     rows = dw.execute(_query)
#     data = rows.mappings().all()
#
#     if len(data) == 0:
#         print("DATA", data)
#
#     # Tästä tulee internal server error, koska jos tietueita ei ole, ei
#     # listassa ole myöskään alkioita eli ei edes alkiota indeksillä 0
#     if data[0]["value"] is None:
#         print("DATA2", data)
#
#     return {"data": data}
