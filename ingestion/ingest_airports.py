"""
Exemple pour l'entité `airports` (la dimension la plus stable du jeu de données) : `ingest_silver`
et `ingest_gold` sont complets, sert de modèle pour les 3 scripts restants. `ingest_bronze` (et
`fetch_csv` dans `common.py`, dont elle dépend) sont à votre charge ici aussi — même sur cet
exemple : c'est simple, mais ça vaut le coup de l'écrire et de la comprendre vous-même plutôt que
de partir d'une ingestion déjà faite.

Découpé en 3 fonctions correspondant aux 3 couches du médaillon — un peu comme 3 tâches
distinctes qu'exécuterait un pipeline orchestré (cf. bonus Dagster) : chaque fonction relit ce
que la précédente a persisté (fichier bronze, puis table silver), elle ne reçoit jamais rien
directement d'un objet Python de l'étape d'avant.
"""
from datetime import date

from common import INIT_DATE, fetch_csv, get_connection

AIRPORT_COLS = ["iata_code", "airport_name", "city", "country"]


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "airports.csv"
    assert day is not None
    return "2025-09", f"airports_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Télécharge le snapshot du jour (ou de init/) vers bronze/, s'il n'y est pas déjà."""
    subdir, filename = _snapshot_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    # insert_timestamp / update_timestamp : colonnes techniques à reproduire sur TOUTES vos
    # tables silver (et, comme on le voit dans ingest_gold ci-dessous, elles suivent
    # automatiquement en gold puisqu'on y fait un simple `SELECT *` depuis le silver).
    # insert_timestamp = quand la ligne a été vue pour la première fois, ne change plus jamais.
    # update_timestamp = à chaque fois que la ligne est retouchée (upsert ou désactivation).
    # deleted_date = NULL tant que la ligne est active, puis figée à la date de désactivation.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_airports (
            airport_id VARCHAR PRIMARY KEY,
            iata_code VARCHAR,
            airport_name VARCHAR,
            city VARCHAR,
            country VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le snapshot depuis bronze/ et l'upsert dans silver_airports."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)  # déjà en cache local : pas de nouveau téléchargement
    snapshot_date = INIT_DATE if init else day

    df = df.copy()
    df["is_active"] = True

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot", df)

    # Upsert : on insère les nouvelles lignes, on met à jour celles dont la clé existe déjà.
    # insert_timestamp n'apparaît PAS dans le SET du conflit : sur un conflit (ligne déjà
    # connue), on veut garder sa valeur d'origine, pas l'écraser. update_timestamp, lui, n'est
    # rafraîchi que si la ligne change réellement (clause WHERE du DO UPDATE) : sinon il
    # bougerait tous les jours sur les 80 aéroports et ne permettrait plus de repérer ceux qui
    # ont vraiment été modifiés.
    # deleted_date est remis à NULL sur un conflit : la ligne est présente dans le snapshot du
    # jour, donc active — si elle avait été désactivée puis réapparaissait (pas le cas dans ce
    # jeu de données, mais une vraie source pourrait le faire), elle redevient active proprement.
    update_cols = AIRPORT_COLS + ["is_active"]
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    set_clause += ", deleted_date = NULL, update_timestamp = now()"
    changed = " OR ".join(f"silver_airports.{c} IS DISTINCT FROM excluded.{c}" for c in update_cols)

    con.execute(f"""
        INSERT INTO silver_airports (
            airport_id, {", ".join(update_cols)}, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT airport_id, {", ".join(update_cols)}, NULL, now(), now()
        FROM snapshot
        ON CONFLICT (airport_id) DO UPDATE SET {set_clause} WHERE {changed}
    """)

    # Les aéroports absents du snapshot du jour ont disparu de la source : on les désactive,
    # sans jamais les supprimer physiquement (des réservations passées peuvent déjà les référencer).
    # deleted_date prend la date du jour où la disparition est constatée, et ne bougera plus
    # ensuite (contrairement à update_timestamp, rafraîchi lui aussi ici, qui continuerait de
    # bouger si un jour ultérieur retouchait la ligne pour une autre raison).
    con.execute(
        """
        UPDATE silver_airports SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND airport_id NOT IN (SELECT airport_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.unregister("snapshot")
    con.close()


def ingest_gold():
    """Reconstruit dim_airport à partir de l'état courant de silver_airports."""
    con = get_connection()
    # SELECT * : insert_timestamp/update_timestamp suivent automatiquement en gold, sans rien
    # ajouter de spécifique ici.
    con.execute("CREATE OR REPLACE TABLE dim_airport AS SELECT * FROM silver_airports")
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Aéroports (init) ingérés.")


if __name__ == "__main__":
    init()
