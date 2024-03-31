import datetime
from calendar import calendar, monthrange


# Generoi nollatietueet SQL datasta puuttuville kuukausille. Syötä queryssä
# saatu data. Datan pitää olla muotoa [{"month": 1 "total_kwh": 0}]
def generate_zero_for_missing_months_in_year_query(fetched_data):
    # Haetaan tietokannasta saadusta datasta tunnit, joissa on dataa
    months_fetched = [i["month"] for i in fetched_data]

    # Alustetaan new_data list ja index(käytetään returned_datan tiedon hakemisessa)
    data = []
    index = 0

    # Silmukoidaan 12 kuukauden verran
    for month in range(1, 13):

        # Jos kuukausi ei löydy saadusta listasta, luodaan nolla tietue.
        if month not in months_fetched:
            monthly_data = {"month": month, "total_kwh": 0}

        # Muussa tapauksessa listataan tietokannasta tullut datasta
        else:
            monthly_data = fetched_data[index]
            index += 1

        data.append(monthly_data)

    return data


# Tehdään logiikka, jolla luodaan 0 data tietueet niile päiville, joilta ei saada dataa.
# Tehdään se tässä, niin ei tarvitse frontendissä tehdä.
# Datan pitää olla muotoa [{"day": 1 "total_kwh": 0}]
def generate_zero_for_missing_days_in_month_query(fetched_data, year: int, month: int):

    number_of_days = monthrange(year, month)[1]

    # Haetaan tietokannasta saadusta datasta päivät, jotka on saatu.
    days_fetched = [i["day"] for i in fetched_data]

    # Alustetaan new_data list ja index(käytetään returned_datan tiedon hakemisessa)
    data = []
    index = 0

    # Silmukoidaan valitun kuukauden päivien lukumäärän mukaisesti
    for day_number in range(1, number_of_days + 1):

        # Jos kyseinen päivä ei löydy tietokannasta, luodaan nolla tietue
        if day_number not in days_fetched:
            data_of_day = {"day": day_number, "total_kwh": 0}

        # Muussa tapauksessa listataan tietokannasta tullut datasta
        else:
            data_of_day = fetched_data[index]
            index += 1

        data.append(data_of_day)

    return data


# Palauttaa viikon tilastot. Jos viikonpäivälle ei löydy dataa, siihen lisätään tyhjä tietue.
# Tarvitsee vuoden karkausvuoden laskentaa varten, sekä viikonnumeron haluttua viikkoa varten
# Lisäksi tarvitsee datan muotoa [{"date": "2024-03-31", "total_kwh": 0}] (vaati datetime objektin)
def generate_zero_for_missing_days_in_week_query(fetched_data, year, week_number):
    # Lasketaan viikon aloituspäivä
    first_day = datetime.date(year, 1, 1)
    start_of_week = first_day + datetime.timedelta(days=(week_number - 1) * 7 - first_day.weekday())

    # Luodaan lista viikonpäivistä datetime objekteina
    dates_of_week = [start_of_week + datetime.timedelta(days=i) for i in range(7)]

    # Alustetaan datalista ja loopataan 7 kertaa. Index on fetched_data itemeiden hakua
    data = []
    index = 0
    for i in range(7):

        # Jos saadun datan päiväys löytyy viikkolistasta, lisätään se data
        if dates_of_week[i] == fetched_data[index]["date"]:
            data.append(fetched_data[index])
            index += 1
        else:
            data.append({"date": dates_of_week[i], "total_kwh": 0})

    return data


# Nolla tietueet hourly by day hauille. Syötä queryssä saatu data.
# Datan pitää olla muotoa [{"hour": 0 "total_kwh": 0}]
def generate_zero_for_missing_hours_in_day_query(fetched_data):
    # Haetaan tietokannasta saadusta datasta tunnit, joissa on dataa
    hours_fetched = [i["hour"] for i in fetched_data]

    # Alustetaan new_data list ja index(käytetään returned_datan tiedon hakemisessa)
    data = []
    index = 0

    # Silmukoidaan vuorokauden tuntien verran
    for hour in range(24):

        # Jos tunti ei löydy saadusta listasta, luodaan nolla tietue.
        if hour not in hours_fetched:
            hourly_data = {"hour": hour, "total_kwh": 0}

        # Muussa tapauksessa listataan tietokannasta tullut datasta
        else:
            hourly_data = fetched_data[index]
            index += 1

        data.append(hourly_data)

    return data


# Nollatietueet lämpötilaan liittyville hourly by day hauille. Syötä queryssä
# saatu data. Datan pitää olla muotoa [{"hour": 0 "temperature": 0}]
def generate_zero_for_missing_hours_in_day_for_temperature_query(fetched_data):
    # Haetaan tietokannasta saadusta datasta tunnit, joissa on dataa
    hours_fetched = [i["hour"] for i in fetched_data]

    # Alustetaan new_data list ja index(käytetään returned_datan tiedon hakemisessa)
    data = []
    index = 0

    # Silmukoidaan vuorokauden tuntien verran
    for hour in range(24):

        # Jos tunti ei löydy saadusta listasta, luodaan nolla tietue.
        if hour not in hours_fetched:
            hourly_data = {"hour": hour, "temperature": 0}

        # Muussa tapauksessa listataan tietokannasta tullut datasta
        else:
            hourly_data = fetched_data[index]
            index += 1

        data.append(hourly_data)

    return data


# Generoidaan nollatietue 7 päivän jaksoon, jos tietoja ei löydy kyseiselle päivälle.
def generate_zero_for_missing_days_in_7_day_period(fetched_data):

    days_fetched = [i["date"] for i in fetched_data]
    starting_day = days_fetched[len(days_fetched)-1] - datetime.timedelta(days=6)

    # Alustetaan loopissa käytettävät muuttujat
    data = []               # Palautettava data
    index = 0               # Indexin avulla haetaan fetched_datasta oikea data
    date = starting_day     # Date lisätään joka loopilla 1 päivä

    # Loopataan 7-day period ajan verran
    for i in range(7):
        if date not in days_fetched:
            daily_data = {"date": date, "total_kwh": 0}
        else:
            daily_data = fetched_data[index]
            index += 1

        date = date + datetime.timedelta(days=1)
        data.append(daily_data)

    return data
