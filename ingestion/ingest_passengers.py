"""
Ingestion de l'entité `passengers`, découpée en 3 fonctions bronze/silver/gold (même structure
que `ingest_airports.py`).

Observation de la source : deux CRM distincts livrent chacun un extrait COMPLET quotidien, avec
des schémas différents (noms de colonnes, genre Male/Female vs Homme/Femme, dates ISO vs
JJ/MM/AAAA). Les passenger_id sont uniques sur les deux fichiers (séquence globale). Les
passagers sont un référentiel qui évolue (ajouts, corrections, suppressions rares) : dimension.
  - silver_passengers : schéma unique (EN + FR consolidés), UPSERT sur passenger_id, désactivation
    des passagers qui disparaissent des deux fichiers ;
  - silver_passengers_history (bonus SCD2) : une ligne par version d'un passager.
"""
from datetime import date

from common import INIT_DATE, apply_scd2, fetch_csv, get_connection

PASSENGER_COLS = [
    "first_name", "last_name", "gender", "nationality", "email",
    "birth_date", "signup_date", "source_system",
]

# Tranches d'âge calculées en gold. L'âge est calculé à la date de référence du modèle, i.e. la
# date du dernier snapshot chargé (max(valid_from) de l'historique) : le gold est ainsi
# reproductible, il ne dépend pas du jour où on relance le pipeline.
AGE_BAND_SQL = """
    CASE
        WHEN age < 18 THEN '0-17'
        WHEN age < 25 THEN '18-24'
        WHEN age < 35 THEN '25-34'
        WHEN age < 45 THEN '35-44'
        WHEN age < 55 THEN '45-54'
        WHEN age < 65 THEN '55-64'
        ELSE '65+'
    END
"""


def _snapshot_files(day: date = None, init: bool = False):
    if init:
        return [
            ("init", "passengers_en.csv"),
            ("init", "passengers_fr.csv"),
        ]

    assert day is not None
    return [
        ("2025-09", f"passengers_en_{day.isoformat()}.csv"),
        ("2025-09", f"passengers_fr_{day.isoformat()}.csv"),
    ]


def ingest_bronze(day: date = None, init: bool = False):
    """Télécharge les deux snapshots (EN et FR) du jour (ou de init/) vers bronze/."""
    for subdir, filename in _snapshot_files(day, init):
        fetch_csv(subdir, filename)


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_passengers (
            passenger_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            nationality VARCHAR,
            email VARCHAR,
            birth_date DATE,
            signup_date DATE,
            source_system VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_passengers_history (
            version_id VARCHAR PRIMARY KEY,
            passenger_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            nationality VARCHAR,
            email VARCHAR,
            birth_date DATE,
            signup_date DATE,
            source_system VARCHAR,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Consolide EN + FR en un schéma unique, upsert dans silver_passengers et historise (SCD2)."""
    (en_dir, en_file), (fr_dir, fr_file) = _snapshot_files(day, init)
    df_en = fetch_csv(en_dir, en_file)
    df_fr = fetch_csv(fr_dir, fr_file)
    snapshot_date = INIT_DATE if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("raw_en", df_en)
    con.register("raw_fr", df_fr)

    # Consolidation : noms de colonnes EN, genre normalisé en Male/Female, dates typées
    # (ISO côté EN, JJ/MM/AAAA côté FR), email en minuscules, et source d'origine conservée.
    con.execute("""
        CREATE OR REPLACE TEMP VIEW snapshot AS
        SELECT
            trim(passenger_id) AS passenger_id,
            trim(first_name) AS first_name,
            trim(last_name) AS last_name,
            CASE lower(trim(gender)) WHEN 'male' THEN 'Male' WHEN 'female' THEN 'Female' END AS gender,
            trim(nationality) AS nationality,
            lower(trim(email)) AS email,
            CAST(birth_date AS DATE) AS birth_date,
            CAST(signup_date AS DATE) AS signup_date,
            'EN' AS source_system
        FROM raw_en
        UNION ALL
        SELECT
            trim(id_passager),
            trim(prenom),
            trim(nom),
            CASE lower(trim(genre)) WHEN 'homme' THEN 'Male' WHEN 'femme' THEN 'Female' END,
            trim(nationalite),
            lower(trim(email)),
            CAST(strptime(date_naissance, '%d/%m/%Y') AS DATE),
            CAST(strptime(date_inscription, '%d/%m/%Y') AS DATE),
            'FR'
        FROM raw_fr
    """)

    dup = con.execute("""
        SELECT count(*) FROM (SELECT passenger_id FROM snapshot GROUP BY 1 HAVING count(*) > 1)
    """).fetchone()[0]
    if dup:
        raise ValueError(f"{dup} passenger_id présents dans les deux fichiers EN/FR du {snapshot_date}")

    update_cols = PASSENGER_COLS + ["is_active"]
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    set_clause += ", deleted_date = NULL, update_timestamp = now()"
    changed = " OR ".join(f"silver_passengers.{c} IS DISTINCT FROM excluded.{c}" for c in update_cols)
    con.execute(f"""
        INSERT INTO silver_passengers (
            passenger_id, {", ".join(update_cols)}, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT passenger_id, {", ".join(PASSENGER_COLS)}, true, NULL, now(), now()
        FROM snapshot
        ON CONFLICT (passenger_id) DO UPDATE SET {set_clause} WHERE {changed}
    """)

    # Passagers absents des deux fichiers du jour : désactivés, jamais supprimés.
    con.execute(
        """
        UPDATE silver_passengers SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND passenger_id NOT IN (SELECT passenger_id FROM snapshot)
        """,
        [snapshot_date],
    )

    apply_scd2(
        con, "silver_passengers_history", "passenger_id", PASSENGER_COLS, "snapshot", snapshot_date
    )

    con.execute("DROP VIEW snapshot")
    con.unregister("raw_en")
    con.unregister("raw_fr")
    con.close()


def ingest_gold():
    """Reconstruit dim_passenger (état courant + tranche d'âge) et dim_passenger_history (SCD2)."""
    con = get_connection()
    ref_date = "(SELECT max(valid_from) FROM silver_passengers_history)"
    age_sql = f"CAST(date_part('year', age({ref_date}, birth_date)) AS INTEGER)"
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_passenger AS
        SELECT
            passenger_id, first_name, last_name, gender, nationality, email,
            birth_date, signup_date, source_system,
            age, {AGE_BAND_SQL} AS age_band,
            is_active, deleted_date, insert_timestamp, update_timestamp
        FROM (SELECT *, {age_sql} AS age FROM silver_passengers)
    """)
    con.execute("""
        CREATE OR REPLACE TABLE dim_passenger_history AS
        SELECT * FROM silver_passengers_history
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Passagers (init) ingérés.")


if __name__ == "__main__":
    init()
