import pandas as pd
import numpy as np
import ast
import json

df = pd.read_csv('C:\\Users\\SHRIYA SENTHILKUMAR\\Documents\\Asian Sports\\pandascore_extract.csv')

def safe_parse(val):
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except:
            try:
                return json.loads(val)
            except:
                return []
    return []

def normalize_matches(raw_df: pd.DataFrame):

    df = raw_df.copy()
    df.columns = [
        col.split('](')[0].lstrip('[') if '](http' in col else col
        for col in df.columns
    ]

    tier_weights = {'s': 5, 'a': 4, 'b': 3, 'c': 2, 'd': 1}

    # ── 1. MATCH FACTS ──────────────────────────────────────────────
    keep = {
        'id': 'match_id', 'name': 'match_name', 'status': 'status',
        'match_type': 'match_type', 'number_of_games': 'number_of_games',
        'begin_at': 'begin_at', 'end_at': 'end_at',
        'draw': 'is_draw', 'forfeit': 'is_forfeit',
        'league.id': 'league_id', 'league.name': 'league_name',
        'tournament.id': 'tournament_id', 'tournament.name': 'tournament_name',
        'tournament.tier': 'tournament_tier', 'tournament.region': 'tournament_region',
        'tournament.prizepool': 'prizepool',
        'serie.name': 'serie_name', 'serie.season': 'serie_season', 'serie.year': 'serie_year',
        'videogame.name': 'game_title',
    }
    valid = {k: v for k, v in keep.items() if k in df.columns}
    match_facts = df[list(valid.keys())].rename(columns=valid).copy()
    match_facts['begin_at'] = pd.to_datetime(match_facts['begin_at'], utc=True)
    match_facts['end_at']   = pd.to_datetime(match_facts['end_at'],   utc=True)
    match_facts['series_duration_mins'] = (
        (match_facts['end_at'] - match_facts['begin_at'])
        .dt.total_seconds().div(60).round(1)
    )
    match_facts['tier_weight'] = (
        match_facts['tournament_tier'].str.lower()
        .map(tier_weights).fillna(1).astype(int)
    )

    # ── 2. TEAM MATCH STATS ─────────────────────────────────────────
    finished = df[df['status'] == 'finished'].copy()
    records  = []

    for _, row in finished.iterrows():
        opponents = safe_parse(row['opponents'])  # ← fixed
        results   = safe_parse(row['results'])    # ← fixed
        games     = safe_parse(row['games'])      # ← fixed

        if not opponents or not results:
            continue

        result_map         = {r['team_id']: r['score'] for r in results}
        winner_id          = max(result_map, key=result_map.get)
        finished_games     = [g for g in games if g.get('finished')]
        total_games_played = len(finished_games)
        game_lengths       = [g['length'] for g in finished_games if g.get('length')]
        avg_game_len       = round(np.mean(game_lengths) / 60, 1) if game_lengths else None
        t_tier             = row['tournament.tier']
        t_region           = row['tournament.region']
        l_name             = row['league.name']

        for opp in opponents:
            team      = opp['opponent']
            team_id   = team['id']
            s_score   = result_map.get(team_id, 0)
            opp_score = sum(v for k, v in result_map.items() if k != team_id)
            games_won = sum(
                1 for g in finished_games
                if isinstance(g.get('winner'), dict)
                and g['winner'].get('id') == team_id
            )
            records.append({
                'match_id':             row['id'],
                'team_id':              team_id,
                'team_name':            team['name'],
                'match_name': row.get('name', ''),
                'team_acronym':         team.get('acronym'),
                'team_location':        team.get('location'),
                'series_score':         s_score,
                'opponent_score':       opp_score,
                'is_winner':            team_id == winner_id,
                'games_won':            games_won,
                'games_played':         total_games_played,
                'win_rate_in_series':   round(games_won / total_games_played, 3) if total_games_played else None,
                'avg_game_length_mins': avg_game_len,
                'begin_at':             row['begin_at'],
                'tournament_tier':      t_tier,
                'tier_weight':          tier_weights.get(str(t_tier).lower(), 1),
                'league_name':          l_name,
                'tournament_region':    t_region,
            })

    team_match_stats = pd.DataFrame(records)

    # ── 3. GAME FACTS ───────────────────────────────────────────────
    game_records = []
    for _, row in df.iterrows():
        games = safe_parse(row['games'])          # ← fixed
        for g in games:                           # ← fixed
            game_records.append({
                'game_id':        g['id'],
                'match_id':       g['match_id'],
                'position':       g['position'],
                'status':         g['status'],
                'finished':       g['finished'],
                'length_secs':    g.get('length'),
                'length_mins':    round(g['length'] / 60, 1) if g.get('length') else None,
                'begin_at':       pd.to_datetime(g.get('begin_at'), utc=True),
                'end_at':         pd.to_datetime(g.get('end_at'),   utc=True),
                'tournament_tier': t_tier,
                'league_name':     l_name, 
                'tournament_region': t_region, 
                'winner_id':      g['winner'].get('id') if isinstance(g.get('winner'), dict) else None,
                'is_forfeit':     g.get('forfeit', False),
                'detailed_stats': g.get('detailed_stats', False),
            })

    game_facts = pd.DataFrame(game_records)
    return match_facts, team_match_stats, game_facts


# ── Run ─────────────────────────────────────────────────────────────
match_facts, team_match_stats, game_facts = normalize_matches(df)

print(f"match_facts:      {match_facts.shape}")
print(f"team_match_stats: {team_match_stats.shape}")
print(f"game_facts:       {game_facts.shape}")

print("\n--- Rows per match (should all be 2) ---")
print(team_match_stats.groupby('match_id').size().value_counts())

print("\n--- Team performance ---")
team_perf = (
    team_match_stats
    .groupby(['team_id', 'team_name'])
    .agg(
        matches_played       = ('match_id', 'count'),
        wins                 = ('is_winner', 'sum'),
        avg_game_length_mins = ('avg_game_length_mins', 'mean'),
    )
    .reset_index()
)
team_perf['win_rate'] = (team_perf['wins'] / team_perf['matches_played']).round(3)
print(team_perf.sort_values('win_rate', ascending=False).to_string(index=False))

print("\n--- Finished games ---")
print(game_facts[game_facts['finished'] == True][[
    'game_id', 'match_id', 'position', 'length_mins', 'winner_id'
]].to_string(index=False))