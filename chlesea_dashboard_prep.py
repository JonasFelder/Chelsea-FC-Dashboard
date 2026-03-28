"""
================================================================================
Chelsea FC Financial Dashboard — Data Pipeline
================================================================================
Author  : Jonas Nikolaus Felder  (jonasfelder)
Project : Chelsea FC Financial Analysis & Premier League Comparison
Source  : Transfermarkt dataset via Kaggle (davidcariboo/player-scores)
Purpose : Transform raw CSV data into Power BI-ready tables covering Chelsea's
          finances and comparisons to the full Premier League and the Top 6.

Usage
-----
  python chelsea_dashboard_pipeline.py

Output directory : C:\\Users\\jonas\\Documents\\Projects\\Chelsea\\data
Raw data directory: C:\\Users\\jonas\\Documents\\Projects\\Chelsea\\raw

Tables produced
---------------
  dim_clubs.csv                – club master data (PL clubs only)
  dim_players.csv              – player master data (PL clubs, latest valuation)
  dim_competitions.csv         – competition reference
  fact_transfers_chelsea.csv   – every Chelsea transfer (in & out)
  fact_transfers_pl.csv        – all PL club transfers
  fact_squad_valuations.csv    – latest squad market value per club per season
  fact_player_valuations.csv   – most-recent market value per player
  kpi_chelsea_finances.csv     – aggregated financial KPIs for Chelsea
  kpi_pl_averages.csv          – PL-wide and Top-6 financial KPIs per season
================================================================================
"""

import os
import sqlite3
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
RAW_DIR  = r"C:\Users\jonas\Documents\Projects\Chelsea\raw"
OUT_DIR  = r"C:\Users\jonas\Documents\Projects\Chelsea\data"
DB_PATH  = os.path.join(OUT_DIR, "chelsea_pipeline.db")   # ephemeral SQLite

os.makedirs(OUT_DIR, exist_ok=True)

# ── Constants ─────────────────────────────────────────────────────────────────
CHELSEA_ID      = 631
PL_COMPETITION  = "GB1"

# Premier League Top 6 club IDs (stable historic clubs)
TOP6_IDS = [
    631,   # Chelsea
    11,    # Arsenal
    31,    # Liverpool
    281,   # Manchester City
    985,   # Manchester United
    148,   # Tottenham Hotspur
]

# ── Load raw CSVs ─────────────────────────────────────────────────────────────
print("Loading raw CSVs …")

def read(name):
    path = os.path.join(RAW_DIR, f"{name}.csv")
    df = pd.read_csv(path, low_memory=False)
    print(f"  {name:25s}  {len(df):>8,} rows")
    return df

clubs           = read("clubs")
competitions    = read("competitions")
games           = read("games")
club_games      = read("club_games")
players         = read("players")
player_valuations = read("player_valuations")
transfers       = read("transfers")

# ── Load into SQLite for SQL-based transformations ────────────────────────────
print("\nLoading into SQLite …")
con = sqlite3.connect(DB_PATH)

clubs.to_sql("raw_clubs",             con, if_exists="replace", index=False)
competitions.to_sql("raw_competitions", con, if_exists="replace", index=False)
games.to_sql("raw_games",             con, if_exists="replace", index=False)
club_games.to_sql("raw_club_games",   con, if_exists="replace", index=False)
players.to_sql("raw_players",         con, if_exists="replace", index=False)
player_valuations.to_sql("raw_player_valuations", con, if_exists="replace", index=False)
transfers.to_sql("raw_transfers",     con, if_exists="replace", index=False)

print("  All tables loaded.\n")

# ═══════════════════════════════════════════════════════════════════════════════
# SQL TRANSFORMATIONS
# ═══════════════════════════════════════════════════════════════════════════════

# ── 1. dim_clubs  ─────────────────────────────────────────────────────────────
# Only Premier League clubs; tag Chelsea and Top-6 membership.
sql_dim_clubs = """
SELECT
    c.club_id,
    c.name                                       AS club_name,
    c.domestic_competition_id,
    c.squad_size,
    c.average_age,
    c.foreigners_number,
    c.foreigners_percentage,
    c.national_team_players,
    c.stadium_name,
    c.stadium_seats,
    c.net_transfer_record,
    c.coach_name,
    c.last_season,
    CASE WHEN c.club_id = 631 THEN 1 ELSE 0 END AS is_chelsea,
    CASE WHEN c.club_id IN (631,11,31,281,985,148)
         THEN 1 ELSE 0 END                       AS is_top6
FROM raw_clubs c
WHERE c.domestic_competition_id = 'GB1'
ORDER BY c.name
"""

dim_clubs = pd.read_sql(sql_dim_clubs, con)
print(f"dim_clubs            : {len(dim_clubs):>6,} rows")

# ── 2. dim_competitions ───────────────────────────────────────────────────────
sql_dim_comps = """
SELECT
    competition_id,
    name        AS competition_name,
    sub_type,
    type,
    country_name,
    confederation
FROM raw_competitions
"""
dim_competitions = pd.read_sql(sql_dim_comps, con)
print(f"dim_competitions     : {len(dim_competitions):>6,} rows")

# ── 3. fact_player_valuations  ────────────────────────────────────────────────
# KEY RULE: For each player take ONLY the most recent valuation date.
# This prevents double-counting when a player has multiple historical entries.
sql_latest_valuation = """
WITH ranked AS (
    SELECT
        pv.player_id,
        pv.date                        AS valuation_date,
        pv.market_value_in_eur,
        pv.current_club_name,
        pv.current_club_id,
        pv.player_club_domestic_competition_id,
        ROW_NUMBER() OVER (
            PARTITION BY pv.player_id
            ORDER BY pv.date DESC
        )                              AS rn
    FROM raw_player_valuations pv
)
SELECT
    r.player_id,
    p.name          AS player_name,
    r.valuation_date,
    r.market_value_in_eur,
    r.current_club_name,
    r.current_club_id,
    r.player_club_domestic_competition_id,
    CASE WHEN r.current_club_id = 631 THEN 1 ELSE 0 END AS is_chelsea_player,
    CASE WHEN r.current_club_id IN (631,11,31,281,985,148)
         THEN 1 ELSE 0 END                               AS is_top6_player
FROM ranked r
LEFT JOIN raw_players p ON p.player_id = r.player_id
WHERE r.rn = 1
"""
fact_player_valuations = pd.read_sql(sql_latest_valuation, con)
print(f"fact_player_valuations: {len(fact_player_valuations):>6,} rows  (one per player, latest date only)")

# ── 4. dim_players ────────────────────────────────────────────────────────────
# Player master — enrich with the latest market value already computed above.
sql_dim_players = """
SELECT
    p.player_id,
    p.name                      AS player_name,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.country_of_birth,
    p.country_of_citizenship,
    p.position,
    p.sub_position,
    p.foot,
    p.height_in_cm,
    p.contract_expiration_date,
    p.current_club_id,
    p.current_club_name,
    p.current_club_domestic_competition_id,
    p.market_value_in_eur       AS current_market_value_eur,
    p.highest_market_value_in_eur,
    p.last_season,
    CASE WHEN p.current_club_id = 631 THEN 1 ELSE 0 END AS is_chelsea_player,
    CASE WHEN p.current_club_id IN (631,11,31,281,985,148)
         THEN 1 ELSE 0 END                               AS is_top6_player
FROM raw_players p
WHERE p.current_club_domestic_competition_id = 'GB1'
   OR p.current_club_id IN (
        SELECT DISTINCT club_id FROM raw_clubs WHERE domestic_competition_id = 'GB1'
   )
ORDER BY p.name
"""
dim_players = pd.read_sql(sql_dim_players, con)
print(f"dim_players          : {len(dim_players):>6,} rows")

# ── 5. fact_transfers_pl  ────────────────────────────────────────────────────
# All transfers involving at least one Premier League club.
# transfer_fee is the actual recorded fee (not a running sum).
sql_transfers_pl = """
SELECT
    t.player_id,
    t.player_name,
    t.transfer_date,
    t.transfer_season,
    t.from_club_id,
    t.from_club_name,
    t.to_club_id,
    t.to_club_name,
    t.transfer_fee,
    t.market_value_in_eur                           AS player_market_value_at_transfer,
    CASE WHEN t.to_club_id   = 631 THEN 1 ELSE 0 END AS chelsea_signing,
    CASE WHEN t.from_club_id = 631 THEN 1 ELSE 0 END AS chelsea_departure,
    CASE WHEN t.to_club_id   IN (631,11,31,281,985,148) THEN 1 ELSE 0 END AS top6_signing,
    CASE WHEN t.from_club_id IN (631,11,31,281,985,148) THEN 1 ELSE 0 END AS top6_departure,
    -- Positive = money spent, negative = money received (Chelsea PoV)
    CASE
        WHEN t.to_club_id   = 631 THEN  COALESCE(t.transfer_fee, 0)
        WHEN t.from_club_id = 631 THEN -COALESCE(t.transfer_fee, 0)
        ELSE NULL
    END AS chelsea_net_fee
FROM raw_transfers t
WHERE t.to_club_id   IN (SELECT club_id FROM raw_clubs WHERE domestic_competition_id = 'GB1')
   OR t.from_club_id IN (SELECT club_id FROM raw_clubs WHERE domestic_competition_id = 'GB1')
ORDER BY t.transfer_date DESC
"""
fact_transfers_pl = pd.read_sql(sql_transfers_pl, con)
print(f"fact_transfers_pl    : {len(fact_transfers_pl):>6,} rows")

# ── 6. fact_transfers_chelsea (dedicated Chelsea view) ────────────────────────
sql_transfers_chelsea = """
SELECT
    t.player_id,
    t.player_name,
    t.transfer_date,
    t.transfer_season,
    t.from_club_id,
    t.from_club_name,
    t.to_club_id,
    t.to_club_name,
    t.transfer_fee,
    t.market_value_in_eur                           AS player_market_value_at_transfer,
    CASE WHEN t.to_club_id = 631 THEN 'IN' ELSE 'OUT' END AS transfer_direction,
    CASE
        WHEN t.to_club_id   = 631 THEN  COALESCE(t.transfer_fee, 0)
        WHEN t.from_club_id = 631 THEN -COALESCE(t.transfer_fee, 0)
    END AS net_fee_eur
FROM raw_transfers t
WHERE t.to_club_id = 631 OR t.from_club_id = 631
ORDER BY t.transfer_date DESC
"""
fact_transfers_chelsea = pd.read_sql(sql_transfers_chelsea, con)
print(f"fact_transfers_chelsea: {len(fact_transfers_chelsea):>6,} rows")

# ── 7. fact_squad_valuations  ────────────────────────────────────────────────
# Per-club, per-season squad market value using ONLY the LATEST valuation
# snapshot each player has within that season. Avoids summing multiple
# entries per player per season.
sql_squad_val = """
WITH season_snapshots AS (
    -- Derive season from valuation date (Aug–Jul football year)
    SELECT
        pv.player_id,
        pv.current_club_id,
        pv.market_value_in_eur,
        pv.date,
        CASE
            WHEN CAST(strftime('%m', pv.date) AS INTEGER) >= 8
            THEN CAST(strftime('%Y', pv.date) AS INTEGER)
            ELSE CAST(strftime('%Y', pv.date) AS INTEGER) - 1
        END AS season_start_year,
        ROW_NUMBER() OVER (
            PARTITION BY pv.player_id,
                CASE
                    WHEN CAST(strftime('%m', pv.date) AS INTEGER) >= 8
                    THEN CAST(strftime('%Y', pv.date) AS INTEGER)
                    ELSE CAST(strftime('%Y', pv.date) AS INTEGER) - 1
                END
            ORDER BY pv.date DESC
        ) AS rn_in_season
    FROM raw_player_valuations pv
    -- Only players who were at some point at a PL club
    WHERE pv.current_club_id IN (
        SELECT club_id FROM raw_clubs WHERE domestic_competition_id = 'GB1'
    )
),
latest_per_season AS (
    SELECT
        player_id,
        current_club_id,
        market_value_in_eur,
        season_start_year
    FROM season_snapshots
    WHERE rn_in_season = 1
)
SELECT
    lps.current_club_id                                      AS club_id,
    c.name                                                   AS club_name,
    lps.season_start_year,
    lps.season_start_year || '/' ||
        CAST(lps.season_start_year + 1 AS TEXT)             AS season_label,
    COUNT(DISTINCT lps.player_id)                           AS player_count,
    SUM(lps.market_value_in_eur)                            AS total_squad_value_eur,
    AVG(lps.market_value_in_eur)                            AS avg_player_value_eur,
    MAX(lps.market_value_in_eur)                            AS max_player_value_eur,
    CASE WHEN lps.current_club_id = 631 THEN 1 ELSE 0 END  AS is_chelsea,
    CASE WHEN lps.current_club_id IN (631,11,31,281,985,148)
         THEN 1 ELSE 0 END                                  AS is_top6
FROM latest_per_season lps
JOIN raw_clubs c ON c.club_id = lps.current_club_id
GROUP BY lps.current_club_id, lps.season_start_year
ORDER BY lps.season_start_year DESC, total_squad_value_eur DESC
"""
fact_squad_valuations = pd.read_sql(sql_squad_val, con)
print(f"fact_squad_valuations: {len(fact_squad_valuations):>6,} rows")

# ── 8. kpi_chelsea_finances ──────────────────────────────────────────────────
# Season-by-season financial summary for Chelsea:
# spend, income, net spend, squad value.
sql_kpi_chelsea = """
WITH chelsea_spend AS (
    SELECT
        transfer_season,
        SUM(CASE WHEN to_club_id   = 631 THEN COALESCE(transfer_fee,0) ELSE 0 END) AS total_spend_eur,
        SUM(CASE WHEN from_club_id = 631 THEN COALESCE(transfer_fee,0) ELSE 0 END) AS total_income_eur,
        COUNT(CASE WHEN to_club_id   = 631 THEN 1 END)                             AS signings,
        COUNT(CASE WHEN from_club_id = 631 THEN 1 END)                             AS departures
    FROM raw_transfers
    WHERE to_club_id = 631 OR from_club_id = 631
    GROUP BY transfer_season
),
chelsea_squad AS (
    SELECT
        season_label,
        season_start_year,
        total_squad_value_eur,
        avg_player_value_eur,
        max_player_value_eur,
        player_count
    FROM (
        /* re-use the squad valuation logic inline */
        WITH season_snapshots AS (
            SELECT
                pv.player_id,
                pv.current_club_id,
                pv.market_value_in_eur,
                CASE
                    WHEN CAST(strftime('%m', pv.date) AS INTEGER) >= 8
                    THEN CAST(strftime('%Y', pv.date) AS INTEGER)
                    ELSE CAST(strftime('%Y', pv.date) AS INTEGER) - 1
                END AS season_start_year,
                ROW_NUMBER() OVER (
                    PARTITION BY pv.player_id,
                        CASE
                            WHEN CAST(strftime('%m', pv.date) AS INTEGER) >= 8
                            THEN CAST(strftime('%Y', pv.date) AS INTEGER)
                            ELSE CAST(strftime('%Y', pv.date) AS INTEGER) - 1
                        END
                    ORDER BY pv.date DESC
                ) AS rn
            FROM raw_player_valuations pv
            WHERE pv.current_club_id = 631
        )
        SELECT
            season_start_year,
            season_start_year || '/' || CAST(season_start_year+1 AS TEXT) AS season_label,
            SUM(market_value_in_eur)  AS total_squad_value_eur,
            AVG(market_value_in_eur)  AS avg_player_value_eur,
            MAX(market_value_in_eur)  AS max_player_value_eur,
            COUNT(DISTINCT player_id) AS player_count
        FROM season_snapshots
        WHERE rn = 1
        GROUP BY season_start_year
    )
)
SELECT
    sp.transfer_season,
    sp.total_spend_eur,
    sp.total_income_eur,
    sp.total_spend_eur - sp.total_income_eur AS net_spend_eur,
    sp.signings,
    sp.departures,
    sq.total_squad_value_eur,
    sq.avg_player_value_eur,
    sq.max_player_value_eur,
    sq.player_count                           AS squad_size_valued
FROM chelsea_spend sp
LEFT JOIN chelsea_squad sq
       ON sq.season_label = sp.transfer_season
          OR (SUBSTR(sp.transfer_season,1,4) = CAST(sq.season_start_year AS TEXT))
ORDER BY sp.transfer_season DESC
"""
kpi_chelsea = pd.read_sql(sql_kpi_chelsea, con)
print(f"kpi_chelsea_finances : {len(kpi_chelsea):>6,} rows")

# ── 9. kpi_pl_averages ────────────────────────────────────────────────────────
# Average financial KPIs per season across all PL clubs and Top-6 separately,
# for direct Power BI comparison with Chelsea.
sql_kpi_pl = """
WITH pl_spend AS (
    SELECT
        t.to_club_id                                                                AS club_id,
        t.transfer_season,
        SUM(CASE WHEN to_club_id IN (
                SELECT club_id FROM raw_clubs WHERE domestic_competition_id='GB1')
             THEN COALESCE(t.transfer_fee,0) ELSE 0 END)                           AS spend_eur,
        SUM(CASE WHEN from_club_id IN (
                SELECT club_id FROM raw_clubs WHERE domestic_competition_id='GB1')
             THEN COALESCE(t.transfer_fee,0) ELSE 0 END)                           AS income_eur
    FROM raw_transfers t
    WHERE t.to_club_id IN (SELECT club_id FROM raw_clubs WHERE domestic_competition_id='GB1')
       OR t.from_club_id IN (SELECT club_id FROM raw_clubs WHERE domestic_competition_id='GB1')
    GROUP BY t.to_club_id, t.transfer_season
),
pl_squad AS (
    SELECT
        current_club_id AS club_id,
        season_start_year,
        season_label,
        total_squad_value_eur
    FROM (
        WITH ss AS (
            SELECT
                pv.player_id,
                pv.current_club_id,
                pv.market_value_in_eur,
                CASE
                    WHEN CAST(strftime('%m', pv.date) AS INTEGER) >= 8
                    THEN CAST(strftime('%Y', pv.date) AS INTEGER)
                    ELSE CAST(strftime('%Y', pv.date) AS INTEGER) - 1
                END AS season_start_year,
                ROW_NUMBER() OVER (
                    PARTITION BY pv.player_id,
                        CASE
                            WHEN CAST(strftime('%m', pv.date) AS INTEGER) >= 8
                            THEN CAST(strftime('%Y', pv.date) AS INTEGER)
                            ELSE CAST(strftime('%Y', pv.date) AS INTEGER) - 1
                        END
                    ORDER BY pv.date DESC
                ) AS rn
            FROM raw_player_valuations pv
            WHERE pv.current_club_id IN (
                SELECT club_id FROM raw_clubs WHERE domestic_competition_id='GB1'
            )
        )
        SELECT
            current_club_id,
            season_start_year,
            season_start_year || '/' || CAST(season_start_year+1 AS TEXT) AS season_label,
            SUM(market_value_in_eur) AS total_squad_value_eur
        FROM ss
        WHERE rn = 1
        GROUP BY current_club_id, season_start_year
    )
),
combined AS (
    SELECT
        ps.club_id,
        ps.transfer_season,
        ps.spend_eur,
        ps.income_eur,
        ps.spend_eur - ps.income_eur                                       AS net_spend_eur,
        sq.total_squad_value_eur,
        CASE WHEN ps.club_id IN (631,11,31,281,985,148) THEN 1 ELSE 0 END  AS is_top6
    FROM pl_spend ps
    LEFT JOIN pl_squad sq
           ON sq.club_id = ps.club_id
          AND sq.season_label = ps.transfer_season
)
SELECT
    transfer_season,
    'all_pl'                        AS group_label,
    AVG(spend_eur)                  AS avg_spend_eur,
    AVG(income_eur)                 AS avg_income_eur,
    AVG(net_spend_eur)              AS avg_net_spend_eur,
    AVG(total_squad_value_eur)      AS avg_squad_value_eur,
    SUM(spend_eur)                  AS total_spend_eur,
    COUNT(DISTINCT club_id)         AS club_count
FROM combined
GROUP BY transfer_season

UNION ALL

SELECT
    transfer_season,
    'top6'                          AS group_label,
    AVG(spend_eur)                  AS avg_spend_eur,
    AVG(income_eur)                 AS avg_income_eur,
    AVG(net_spend_eur)              AS avg_net_spend_eur,
    AVG(total_squad_value_eur)      AS avg_squad_value_eur,
    SUM(spend_eur)                  AS total_spend_eur,
    COUNT(DISTINCT club_id)         AS club_count
FROM combined
WHERE is_top6 = 1
GROUP BY transfer_season

ORDER BY transfer_season DESC, group_label
"""
kpi_pl_averages = pd.read_sql(sql_kpi_pl, con)
print(f"kpi_pl_averages      : {len(kpi_pl_averages):>6,} rows")

# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION CHECKS
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Validation ────────────────────────────────────────────────────────────")

# 1. No player should appear more than once in fact_player_valuations
dupes = fact_player_valuations.duplicated("player_id").sum()
assert dupes == 0, f"Duplicate player_ids in fact_player_valuations: {dupes}"
print("  ✓  fact_player_valuations: no duplicate player_ids")

# 2. Squad valuations: spot-check Chelsea value is non-zero in recent seasons
chelsea_val = fact_squad_valuations[
    (fact_squad_valuations["club_id"] == CHELSEA_ID) &
    (fact_squad_valuations["season_start_year"] >= 2020)
]
assert len(chelsea_val) > 0, "No Chelsea squad valuation found after 2020"
print(f"  ✓  Chelsea squad valuations found ({len(chelsea_val)} seasons from 2020+)")
print(chelsea_val[["season_label","player_count","total_squad_value_eur"]].head(5).to_string(index=False))

# 3. Transfer fees are never negative (the net column can be, raw fees not)
bad_fees = fact_transfers_pl[fact_transfers_pl["transfer_fee"] < 0]
assert len(bad_fees) == 0, f"Negative raw transfer fees: {len(bad_fees)}"
print("  ✓  No negative raw transfer fees")

# 4. All dim_clubs rows are PL
non_pl = dim_clubs[dim_clubs["domestic_competition_id"] != "GB1"]
assert len(non_pl) == 0, "Non-PL clubs in dim_clubs"
print(f"  ✓  dim_clubs contains {len(dim_clubs)} PL clubs only")

# 5. Top-6 all present
found_top6 = set(dim_clubs[dim_clubs["is_top6"] == 1]["club_id"].tolist())
assert set(TOP6_IDS).issubset(found_top6), f"Missing Top-6 clubs: {set(TOP6_IDS)-found_top6}"
print(f"  ✓  All 6 Top-6 clubs present in dim_clubs")

print("\n── All validation checks passed ✓ ────────────────────────────────────────\n")

# ═══════════════════════════════════════════════════════════════════════════════
# EXPORT TO CSV
# ═══════════════════════════════════════════════════════════════════════════════

tables = {
    "dim_clubs"               : dim_clubs,
    "dim_players"             : dim_players,
    "dim_competitions"        : dim_competitions,
    "fact_player_valuations"  : fact_player_valuations,
    "fact_transfers_chelsea"  : fact_transfers_chelsea,
    "fact_transfers_pl"       : fact_transfers_pl,
    "fact_squad_valuations"   : fact_squad_valuations,
    "kpi_chelsea_finances"    : kpi_chelsea,
    "kpi_pl_averages"         : kpi_pl_averages,
}

print("Saving output CSVs …")
for name, df in tables.items():
    path = os.path.join(OUT_DIR, f"{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  Saved  {name}.csv  ({len(df):,} rows, {len(df.columns)} cols)")

# Clean up ephemeral DB
con.close()
os.remove(DB_PATH)

print(f"\n✅  Pipeline complete. All files saved to:\n    {OUT_DIR}")
print("""
─────────────────────────────────────────────────────
Power BI Import Guide
─────────────────────────────────────────────────────
1. Open Power BI Desktop → Get Data → Text/CSV
2. Import each CSV from the data folder above.
3. Recommended relationships:
   dim_clubs.club_id          → fact_transfers_pl.[from/to_club_id]
   dim_clubs.club_id          → fact_squad_valuations.club_id
   dim_players.player_id      → fact_player_valuations.player_id
   dim_players.player_id      → fact_transfers_chelsea.player_id
4. Key measures to create in DAX:
   • Chelsea Net Spend = SUM(fact_transfers_chelsea[net_fee_eur])
   • Squad Value       = SUM(fact_squad_valuations[total_squad_value_eur])
   • PL Avg Spend      = AVERAGE(kpi_pl_averages[avg_spend_eur])
─────────────────────────────────────────────────────
""")