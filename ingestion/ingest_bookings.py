"""
Ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold (même structure que
`ingest_airports.py`).

Observation de la source : contrairement aux 3 autres entités, chaque fichier journalier ne
contient QUE les réservations du jour (pas de cumul), avec un booking_id jamais réutilisé. Une
réservation est un évènement immuable : c'est la table de fait. Chargement en INSERT simple,
rendu idempotent par la clé primaire (ON CONFLICT DO NOTHING) : rejouer un jour n'ajoute rien.
Pas de désactivation : une réservation absente du fichier du jour n'est pas "supprimée", elle
appartient simplement à un autre jour.

ingest_gold construit aussi, en fin de chaîne, les tables gold transverses :
  - dim_date : calendrier couvrant réservations et vols ;
  - agg_daily_revenue_airline : CA agrégé par jour de réservation et par compagnie.
Elles vivent ici car les réservations sont la dernière entité traitée par run_month.py : au moment
où ce gold tourne, dim_flight (dont on a besoin pour la compagnie) est déjà à jour pour le jour.
"""
from datetime import date

from common import INIT_DATE, fetch_csv, get_connection


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "bookings.csv"
    assert day is not None
    return "2025-09", f"bookings_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Télécharge le fichier bookings du jour (ou de init/) vers bronze/."""
    subdir, filename = _snapshot_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_bookings (
            booking_id VARCHAR PRIMARY KEY,
            passenger_id VARCHAR,
            flight_id VARCHAR,
            airport_id VARCHAR,
            seat_class VARCHAR,
            amount DECIMAL(12, 2),
            currency VARCHAR,
            booking_date DATE,
            booking_channel VARCHAR,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le fichier du jour depuis bronze/ et insère les nouvelles réservations."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)

    con = get_connection()
    create_silver_table(con)
    con.register("raw_bookings", df)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW new_bookings AS
        SELECT
            trim(booking_id) AS booking_id,
            trim(passenger_id) AS passenger_id,
            trim(flight_id) AS flight_id,
            trim(airport_id) AS airport_id,
            trim(seat_class) AS seat_class,
            CAST(amount AS DECIMAL(12, 2)) AS amount,
            upper(trim(currency)) AS currency,
            CAST(booking_date AS DATE) AS booking_date,
            trim(booking_channel) AS booking_channel
        FROM raw_bookings
    """)

    # Contrôles de qualité (non bloquants) : références vers les dimensions silver déjà
    # chargées pour ce jour (run_month.py passe le silver des vols/passagers/aéroports avant
    # celui des réservations) et devise connue de dim_currency.
    checks = {
        "passager inconnu": "passenger_id NOT IN (SELECT passenger_id FROM silver_passengers)",
        "vol inconnu": "flight_id NOT IN (SELECT flight_id FROM silver_flights)",
        "aéroport inconnu": "airport_id NOT IN (SELECT airport_id FROM silver_airports)",
        "devise inconnue": "currency NOT IN (SELECT currency FROM dim_currency)",
    }
    for label, condition in checks.items():
        try:
            n = con.execute(f"SELECT count(*) FROM new_bookings WHERE {condition}").fetchone()[0]
        except Exception:  # table de référence pas (encore) créée : contrôle ignoré
            continue
        if n:
            print(f"[bookings {filename}] attention : {n} réservation(s) avec {label}")

    # Insert simple : une réservation déjà connue n'est jamais modifiée.
    con.execute("""
        INSERT INTO silver_bookings
        SELECT *, now(), now() FROM new_bookings
        ON CONFLICT (booking_id) DO NOTHING
    """)

    con.execute("DROP VIEW new_bookings")
    con.unregister("raw_bookings")
    con.close()


def ingest_gold():
    """Reconstruit fact_booking, dim_date et agg_daily_revenue_airline depuis le silver/gold."""
    con = get_connection()

    # fact_booking : grain = une réservation. Clés vers les dimensions (passenger_id, flight_id,
    # airport_id, currency, booking_date -> dim_date) + mesures (amount, amount_eur).
    # Bonus SCD2 : flight_version_id / passenger_version_id pointent vers la version de la
    # dimension en vigueur le jour de la réservation (ASOF JOIN). Les réservations d'août sont
    # antérieures au premier snapshot connu (init = 2025-08-31) : on les rattache à cette
    # première version, faute d'historique plus ancien.
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_booking AS
        WITH b AS (
            SELECT *, greatest(booking_date, DATE '{INIT_DATE.isoformat()}') AS version_ref_date
            FROM silver_bookings
        )
        SELECT
            b.booking_id,
            b.passenger_id,
            b.flight_id,
            b.airport_id,
            b.booking_date,
            fh.version_id AS flight_version_id,
            ph.version_id AS passenger_version_id,
            b.seat_class,
            b.booking_channel,
            b.currency,
            b.amount,
            CAST(round(b.amount * c.rate_to_eur, 2) AS DECIMAL(12, 2)) AS amount_eur,
            b.insert_timestamp,
            b.update_timestamp
        FROM b
        LEFT JOIN dim_currency c ON c.currency = b.currency
        ASOF LEFT JOIN silver_flights_history fh
            ON fh.flight_id = b.flight_id AND b.version_ref_date >= fh.valid_from
        ASOF LEFT JOIN silver_passengers_history ph
            ON ph.passenger_id = b.passenger_id AND b.version_ref_date >= ph.valid_from
    """)

    # dim_date : calendrier de la première réservation au dernier vol programmé.
    con.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        WITH bounds AS (
            SELECT
                least((SELECT min(booking_date) FROM fact_booking),
                      (SELECT min(flight_date) FROM dim_flight)) AS d_min,
                greatest((SELECT max(booking_date) FROM fact_booking),
                         (SELECT max(flight_date) FROM dim_flight)) AS d_max
        )
        SELECT
            CAST(d AS DATE) AS date_day,
            year(d) AS year,
            month(d) AS month,
            strftime(d, '%Y-%m') AS year_month,
            day(d) AS day_of_month,
            isodow(d) AS day_of_week,
            strftime(d, '%A') AS day_name,
            isodow(d) >= 6 AS is_weekend,
            now() AS insert_timestamp,
            now() AS update_timestamp
        FROM bounds, range(d_min, d_max + INTERVAL 1 DAY, INTERVAL 1 DAY) t(d)
    """)

    # Agrégat gold : CA par jour de réservation et par compagnie (en EUR, devise pivot).
    con.execute("""
        CREATE OR REPLACE TABLE agg_daily_revenue_airline AS
        SELECT
            f.booking_date,
            fl.airline,
            count(*) AS nb_bookings,
            sum(f.amount_eur) AS revenue_eur,
            round(avg(f.amount_eur), 2) AS avg_basket_eur,
            min(f.insert_timestamp) AS insert_timestamp,
            max(f.update_timestamp) AS update_timestamp
        FROM fact_booking f
        JOIN dim_flight fl ON fl.flight_id = f.flight_id
        GROUP BY f.booking_date, fl.airline
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
