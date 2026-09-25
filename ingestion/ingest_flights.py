"""
Ingestion de l'entité `flights`, découpée en 3 fonctions bronze/silver/gold (même structure que
`ingest_airports.py`).

Observation de la source : chaque fichier journalier est un extrait COMPLET du catalogue de vols
(ajouts, modifications d'horaire/avion/date et suppressions fréquents). C'est un référentiel qui
évolue, donc une dimension :
  - silver_flights : état courant, chargé en UPSERT sur flight_id, avec désactivation (is_active /
    deleted_date) des vols qui disparaissent du fichier — jamais de suppression physique, des
    réservations peuvent déjà les référencer ;
  - silver_flights_history (bonus SCD2) : une ligne par version d'un vol, avec sa période de
    validité, pour savoir par ex. quel avion/horaire était affecté à un vol à une date donnée.
"""
from datetime import date

from common import INIT_DATE, apply_scd2, fetch_csv, get_connection

FLIGHT_COLS = [
    "flight_number", "airline", "origin_airport_id", "destination_airport_id",
    "flight_date", "departure_time", "arrival_time", "aircraft_type",
]

# Durée de vol en minutes, à partir des heures locales de départ/d'arrivée. Une arrivée
# antérieure (ou égale) au départ signifie une arrivée le lendemain : on ajoute 24 h.
DURATION_SQL = """
    CASE
        WHEN arrival_time > departure_time THEN date_diff('minute', departure_time, arrival_time)
        ELSE date_diff('minute', departure_time, arrival_time) + 24 * 60
    END
"""


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "flights.csv"
    assert day is not None
    return "2025-09", f"flights_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Télécharge le snapshot flights du jour (ou de init/) vers bronze/."""
    subdir, filename = _snapshot_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights (
            flight_id VARCHAR PRIMARY KEY,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)
    # Bonus SCD2 : version_id = flight_id@valid_from (clé technique d'une version).
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights_history (
            version_id VARCHAR PRIMARY KEY,
            flight_id VARCHAR,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le snapshot depuis bronze/, l'upsert dans silver_flights et l'historise (SCD2)."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)
    snapshot_date = INIT_DATE if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("raw_snapshot", df)
    # Typage explicite (le CSV est lu en texte) + nettoyage des espaces parasites.
    con.execute("""
        CREATE OR REPLACE TEMP VIEW snapshot AS
        SELECT
            trim(flight_id) AS flight_id,
            trim(flight_number) AS flight_number,
            trim(airline) AS airline,
            trim(origin_airport_id) AS origin_airport_id,
            trim(destination_airport_id) AS destination_airport_id,
            CAST(flight_date AS DATE) AS flight_date,
            CAST(departure_time AS TIME) AS departure_time,
            CAST(arrival_time AS TIME) AS arrival_time,
            trim(aircraft_type) AS aircraft_type
        FROM raw_snapshot
    """)

    # Upsert (même logique que silver_airports) : update_timestamp n'est rafraîchi que si une
    # valeur change réellement, ou si un vol désactivé réapparaît.
    update_cols = FLIGHT_COLS + ["is_active"]
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    set_clause += ", deleted_date = NULL, update_timestamp = now()"
    changed = " OR ".join(f"silver_flights.{c} IS DISTINCT FROM excluded.{c}" for c in update_cols)
    con.execute(f"""
        INSERT INTO silver_flights (
            flight_id, {", ".join(update_cols)}, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT flight_id, {", ".join(FLIGHT_COLS)}, true, NULL, now(), now()
        FROM snapshot
        ON CONFLICT (flight_id) DO UPDATE SET {set_clause} WHERE {changed}
    """)

    # Vols disparus du snapshot : désactivés, jamais supprimés.
    con.execute(
        """
        UPDATE silver_flights SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND flight_id NOT IN (SELECT flight_id FROM snapshot)
        """,
        [snapshot_date],
    )

    apply_scd2(con, "silver_flights_history", "flight_id", FLIGHT_COLS, "snapshot", snapshot_date)

    con.execute("DROP VIEW snapshot")
    con.unregister("raw_snapshot")
    con.close()


def ingest_gold():
    """Reconstruit dim_flight (état courant) et dim_flight_history (SCD2) depuis le silver."""
    con = get_connection()
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_flight AS
        SELECT
            flight_id, flight_number, airline, origin_airport_id, destination_airport_id,
            flight_date, departure_time, arrival_time, aircraft_type,
            {DURATION_SQL} AS duration_minutes,
            is_active, deleted_date, insert_timestamp, update_timestamp
        FROM silver_flights
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_flight_history AS
        SELECT
            version_id, flight_id, flight_number, airline, origin_airport_id,
            destination_airport_id, flight_date, departure_time, arrival_time, aircraft_type,
            {DURATION_SQL} AS duration_minutes,
            valid_from, valid_to, is_current, insert_timestamp, update_timestamp
        FROM silver_flights_history
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Vols (init) ingérés.")


if __name__ == "__main__":
    init()
