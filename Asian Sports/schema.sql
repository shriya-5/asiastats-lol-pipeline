-- schema.sql
-- Run once against the asiastats database before starting spark_consumer.py
-- Connect via pgAdmin (localhost:5050) or psql and paste this

-- ── Raw events ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS match_events_raw (
    event_id            TEXT PRIMARY KEY,
    event_type          TEXT,
    match_id            INTEGER,
    match_name          TEXT,
    match_status        TEXT,
    league_name         TEXT,
    league_id           INTEGER,
    tournament_name     TEXT,
    tournament_tier     TEXT,
    tournament_region   TEXT,
    game_title          TEXT,
    games_count         INTEGER,
    avg_game_length_mins FLOAT,
    simulated           BOOLEAN,
    _source             TEXT,
    ingested_at         TIMESTAMPTZ,
    match_begin_at      TIMESTAMPTZ
);

-- ── Windowed team win rate ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS team_win_rate_window (
    id                  SERIAL PRIMARY KEY,
    window_start        TIMESTAMPTZ,
    window_end          TIMESTAMPTZ,
    team_id             INTEGER,
    team_name           TEXT,
    team_acronym        TEXT,
    league_name         TEXT,
    tournament_tier     TEXT,
    tournament_region   TEXT,
    matches_in_window   INTEGER,
    total_score         FLOAT,
    avg_score           FLOAT,
    unique_matches      BIGINT,
    data_source         TEXT,
    computed_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_winrate_team    ON team_win_rate_window (team_id);
CREATE INDEX IF NOT EXISTS idx_winrate_window  ON team_win_rate_window (window_start);
CREATE INDEX IF NOT EXISTS idx_winrate_league  ON team_win_rate_window (league_name);

-- ── League activity ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS league_activity (
    id                   SERIAL PRIMARY KEY,
    window_start         TIMESTAMPTZ,
    window_end           TIMESTAMPTZ,
    league_name          TEXT,
    tournament_tier      TEXT,
    tournament_region    TEXT,
    event_count          BIGINT,
    unique_matches       BIGINT,
    avg_game_length_mins FLOAT,
    simulated_count      BIGINT,
    live_count           BIGINT,
    computed_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_activity_league ON league_activity (league_name);
CREATE INDEX IF NOT EXISTS idx_activity_window ON league_activity (window_start);

-- ── Anomaly alerts ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_alerts (
    id                  SERIAL PRIMARY KEY,
    window_start        TIMESTAMPTZ,
    window_end          TIMESTAMPTZ,
    team_id             INTEGER,
    team_name           TEXT,
    team_acronym        TEXT,
    league_name         TEXT,
    tournament_region   TEXT,
    match_count         BIGINT,
    avg_score_window    FLOAT,
    stddev_score        FLOAT,
    max_score           FLOAT,
    anomaly_type        TEXT,
    detected_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_anomaly_team    ON anomaly_alerts (team_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_time    ON anomaly_alerts (detected_at);

-- ── Useful views for the dashboard ────────────────────────────────────
CREATE OR REPLACE VIEW team_leaderboard AS
SELECT
    team_id,
    team_name,
    team_acronym,
    tournament_region,
    SUM(matches_in_window)              AS total_matches,
    SUM(total_score)                    AS total_score,
    ROUND(AVG(avg_score)::numeric, 3)   AS avg_score,
    MAX(computed_at)                    AS last_updated
FROM team_win_rate_window
GROUP BY team_id, team_name, team_acronym, tournament_region
ORDER BY avg_score DESC;

CREATE OR REPLACE VIEW live_league_pulse AS
SELECT
    league_name,
    tournament_region,
    SUM(event_count)                    AS total_events,
    SUM(live_count)                     AS live_events,
    SUM(simulated_count)                AS simulated_events,
    ROUND(AVG(avg_game_length_mins)::numeric, 1) AS avg_game_mins,
    MAX(computed_at)                    AS last_updated
FROM league_activity
WHERE window_start >= NOW() - INTERVAL '1 hour'
GROUP BY league_name, tournament_region
ORDER BY total_events DESC;
