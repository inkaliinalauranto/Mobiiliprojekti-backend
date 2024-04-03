from fastapi import APIRouter
from sqlalchemy import text

from customfunctions import generate_zero_for_missing_hours_in_day_with_keys
from db import DW

router = APIRouter(
    prefix='/api/measurement/temperature',
    tags=['Temperature']
)


# Haetaan annetun päivän keskiarvolämpötilat, jotka lajitellaan
# tuntikohtaisesti.
@router.get("/indoor/avg/hourly/{date}")
async def get_indoor_avg_temperature_statistics_hourly_by_day(dw: DW, date: str):
    """
    Get hourly temperatures (avg) from a given day.
    String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT d.hour, AVG(value) AS °C "
                  "FROM temperatures_fact t "
                  "JOIN dates_dim d ON t.date_key = d.date_key "
                  "WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) = DATE(:date) "
                  "AND t.sensor_key = :sensor_key "
                  "GROUP BY d.hour "
                  "ORDER BY d.hour;")
    rows = dw.execute(_query, {"date": date, "sensor_key": 125})
    fetched_data = rows.mappings().all()

    time_key = "hour"
    temperature_unit_key = "°C"

    if len(fetched_data) > 0:
        time_key = tuple(fetched_data[0].keys())[0]
        temperature_unit_key = tuple(fetched_data[0].keys())[1]

    data = generate_zero_for_missing_hours_in_day_with_keys(fetched_data, time_key, temperature_unit_key)

    return {"data": data}


# Haetaan viimeisin lämpötilatieto:
@router.get("/indoor/current")
async def get_most_recent_indoor_temperature(dw: DW):
    """
    Get the most recent indoor temperature information.
    String ISO 8601 format YYYY-MM-DD.
    """
    _query = text("SELECT CONCAT_WS(': ', s.device_name, s.sensor_name) AS sensor, t.value "
                  "FROM sensors_dim s "
                  "JOIN temperatures_fact t ON s.sensor_key = t.sensor_key "
                  "WHERE t.date_key = "
                  "(SELECT MAX(date_key) FROM temperatures_fact WHERE sensor_key = :sensor_key);")
    rows = dw.execute(_query, {"sensor_key": 125})
    data = rows.mappings().all()

    if len(data) == 0:
        data = [{"value": 0}]

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

