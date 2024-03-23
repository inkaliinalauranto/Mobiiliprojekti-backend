# Team 1 CoolBox mobiiliprojekti Backend

## Repon kopiointi
- Clone with HTTPS
- PyCharmissa create from VCS

## Virtuaaliympäristön luonti
- python -m venv venv
- python -m pip install -r requirements.txt

## Uvicornin käynnistäminen
- venv\Scripts\activate
- uvicorn main:app

## MySQL tietokantaan yhdistäminen
- luo .env tiedosto
- lisää sinne DB muuttuja
    - DB=driver://kayttaja:salasana@localhost/tietokannan_nimi
