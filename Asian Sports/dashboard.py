import sys
import os

# ── Import directly from your normalization.py ───────────────────────
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from normalization import safe_parse, normalize_matches

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import datetime, timezone

# ── Page config ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="AsiaStats — LoL Esports Pipeline",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@400;500&display=swap');
html, body, [class*="css"] { font-family: 'Syne', sans-serif; }
.block-container { padding-top: 1.5rem !important; }
.kpi-card {
    background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
    border: 1px solid #30363d;
    border-radius: 12px;
    padding: 1.2rem 1.4rem;
    text-align: center;
}
.kpi-value { font-size: 2rem; font-weight: 800; color: #58a6ff; line-height: 1; }
.kpi-label { font-size: 0.72rem; color: #8b949e; letter-spacing: 0.08em; text-transform: uppercase; margin-top: 4px; }
.live-badge {
    display: inline-block;
    background: #0d2618;
    border: 1px solid #3fb950;
    color: #3fb950;
    font-size: 0.7rem;
    padding: 2px 10px;
    border-radius: 20px;
    font-weight: 600;
    letter-spacing: 0.06em;
    animation: pulse 2s infinite;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.55} }
[data-testid="stSidebar"] { background: #0d1117; border-right: 1px solid #21262d; }
div[data-testid="metric-container"] {
    background: #161b22; border: 1px solid #30363d;
    border-radius: 10px; padding: 0.8rem 1rem;
}
</style>
""", unsafe_allow_html=True)

PLOT_STYLE = dict(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#e6edf3'))
GRID = dict(gridcolor='#21262d')


# ── Data loaders ─────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def fetch_live(api_key: str) -> pd.DataFrame:
    headers = {"Authorization": f"Bearer {api_key}"}
    rows = []
    for status in ["finished", "running", "not_started"]:
        r = requests.get(
            "https://api.pandascore.co/lol/matches",
            headers=headers,
            params={"filter[status]": status, "sort": "-begin_at", "per_page": 100},
        )
        if r.status_code == 401:
            st.error("❌ Invalid API key.")
            return pd.DataFrame()
        if r.status_code == 200:
            rows.extend(r.json())
    return pd.json_normalize(rows) if rows else pd.DataFrame()

@st.cache_data(ttl=300)
def fetch_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


# ── Sidebar ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🎮 AsiaStats")
    st.markdown("<p style='color:#8b949e;font-size:0.8rem;margin-top:-10px'>LoL Esports · Live Pipeline</p>", unsafe_allow_html=True)
    st.divider()

    source = st.radio("Data source", ["📁 CSV file", "⚡ Live API"])

    if source == "⚡ Live API":
        #api_key = st.text_input("Pandascore API key", type="password", placeholder="paste key here")
        api_key='PANDASCORE_API_KEY'
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🔄 Refresh", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
        with c2:
            auto = st.toggle("Auto 5m")
        if auto:
            st.markdown("<div class='live-badge'>● LIVE</div>", unsafe_allow_html=True)
    else:
        api_key = None
        csv_path = st.text_input(
            "CSV path",
            value=r"C:\Users\SHRIYA SENTHILKUMAR\Documents\Asian Sports\pandascore_extract.csv"
        )

    st.divider()
    st.markdown("<p style='color:#8b949e;font-size:0.75rem;font-weight:600;letter-spacing:0.08em'>FILTERS</p>", unsafe_allow_html=True)
    # Placeholders — repopulated after data loads
    region_ph = st.empty()
    league_ph = st.empty()
    tier_ph   = st.empty()
    min_m     = st.slider("Min matches (teams)", 1, 5, 1)
    st.divider()
    st.markdown(f"<p style='color:#484f58;font-size:0.7rem'>Updated: {datetime.now(timezone.utc).strftime('%H:%M UTC · %d %b %Y')}</p>", unsafe_allow_html=True)


# ── Load & normalize ──────────────────────────────────────────────────
with st.spinner("Running normalization pipeline..."):
    try:
        raw_df = fetch_live(api_key) if source == "⚡ Live API" and api_key else fetch_csv(csv_path) if source == "📁 CSV file" else pd.DataFrame()

        if source == "⚡ Live API" and not api_key:
            st.info("👈 Enter your Pandascore API key in the sidebar.")
            st.stop()

        if raw_df.empty:
            st.warning("No data returned. Check your source.")
            st.stop()

        # Uses YOUR functions from normalization.py
        match_facts, team_stats, game_facts = normalize_matches(raw_df)

    except ImportError as e:
        st.error(f"Import error: `{e}`")
        st.info("Ensure `normalization.py` is in the same folder and `normalize_matches` returns `(match_facts, team_stats, game_facts)`.")
        st.stop()
    except Exception as e:
        st.error(f"Error: `{e}`")
        st.stop()

if team_stats.empty:
    st.warning("No finished matches yet.")
    st.stop()


# ── Live matches banner ───────────────────────────────────────────────
if source == "⚡ Live API" and 'status' in raw_df.columns:
    running = raw_df[raw_df['status'] == 'running']
    if not running.empty:
        st.markdown(f"<div class='live-badge'>● {len(running)} MATCH{'ES' if len(running)>1 else ''} LIVE NOW</div><br>", unsafe_allow_html=True)
        live_cols = [c for c in ['name', 'league.name', 'tournament.name', 'begin_at'] if c in running.columns]
        st.dataframe(
            running[live_cols].rename(columns={'name':'Match','league.name':'League','tournament.name':'Tournament','begin_at':'Started'}),
            use_container_width=True, hide_index=True
        )
        st.divider()


# ── Fill filter dropdowns ─────────────────────────────────────────────
all_regions = sorted(team_stats['tournament_region'].dropna().unique())
all_leagues = sorted(team_stats['league_name'].dropna().unique())
all_tiers   = sorted(team_stats['tournament_tier'].dropna().unique())

region_filter = region_ph.multiselect("Region",          all_regions, placeholder="All regions")
league_filter = league_ph.multiselect("League",          all_leagues, placeholder="All leagues")
tier_filter   = tier_ph.multiselect("Tournament tier",   all_tiers,   placeholder="All tiers")


# ── Apply filters ─────────────────────────────────────────────────────
fts = team_stats.copy()
if region_filter: fts = fts[fts['tournament_region'].isin(region_filter)]
if league_filter: fts = fts[fts['league_name'].isin(league_filter)]
if tier_filter:   fts = fts[fts['tournament_tier'].isin(tier_filter)]

fg = game_facts.copy()
if league_filter: fg = fg[fg['league_name'].isin(league_filter)]
if tier_filter:   fg = fg[fg['tournament_tier'].isin(tier_filter)]

if fts.empty:
    st.warning("No data matches filters. Try removing some.")
    st.stop()


# ── Team aggregation ──────────────────────────────────────────────────
tp = (
    fts.groupby(['team_id','team_name','team_acronym','team_location','tournament_region'])
    .agg(
        matches_played       = ('match_id','count'),
        wins                 = ('is_winner','sum'),
        avg_game_length_mins = ('avg_game_length_mins','mean'),
        avg_tier_weight      = ('tier_weight','mean'),
        leagues              = ('league_name', lambda x: ', '.join(sorted(x.unique()))),
    ).reset_index()
)
tp['win_rate']       = (tp['wins'] / tp['matches_played']).round(3)
tp['win_rate_pct']   = (tp['win_rate'] * 100).round(1)
tp['weighted_score'] = (tp['win_rate'] * tp['avg_tier_weight']).round(3)
tp['losses']         = tp['matches_played'] - tp['wins']
tp = tp[tp['matches_played'] >= min_m]


# ── Header + KPIs ─────────────────────────────────────────────────────
st.markdown("## 🏆 LoL Esports — Match Analytics Pipeline")
src_badge = "<span class='live-badge'>⚡ LIVE API</span>" if source == "⚡ Live API" else "📁 CSV"
st.markdown(f"<p style='color:#8b949e;margin-top:-10px'>{src_badge} &nbsp;·&nbsp; <b style='color:#e6edf3'>{fts['match_id'].nunique()} matches</b> &nbsp;·&nbsp; <b style='color:#e6edf3'>{len(tp)} teams</b></p>", unsafe_allow_html=True)
st.divider()

k1,k2,k3,k4,k5 = st.columns(5)
avg_gl   = fg[fg['finished'] & fg['length_mins'].notna()]['length_mins'].mean()
top_team = tp.sort_values('weighted_score', ascending=False).iloc[0] if not tp.empty else None
top_lbl  = (top_team['team_acronym'] or top_team['team_name'][:6]) if top_team is not None else '—'

for col, v, l in zip([k1,k2,k3,k4,k5],
    [fts['match_id'].nunique(), len(tp), f"{avg_gl:.1f}m" if pd.notna(avg_gl) else '—', fts['tournament_region'].nunique(), top_lbl],
    ["Matches tracked","Teams","Avg game length","Regions","Top team"]):
    col.markdown(f"<div class='kpi-card'><div class='kpi-value'>{v}</div><div class='kpi-label'>{l}</div></div>", unsafe_allow_html=True)

st.divider()


# ── Tabs ──────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📊 Leaderboard","⚔️ Head-to-Head","⏱ Game Lengths","📋 Raw Data"])


with tab1:
    c1, c2 = st.columns([1.5, 1])
    with c1:
        st.markdown("#### Win rate leaderboard")
        sort_by  = st.selectbox("Sort by", ["Win rate %","Weighted score","Wins","Matches played"])
        sort_col = {"Win rate %":"win_rate_pct","Weighted score":"weighted_score","Wins":"wins","Matches played":"matches_played"}[sort_by]
        fig = px.bar(
            tp.sort_values(sort_col, ascending=True).tail(20),
            x=sort_col, y='team_name', orientation='h',
            color='win_rate_pct', color_continuous_scale='Blues', text=sort_col,
            hover_data={'team_acronym':True,'matches_played':True,'wins':True,'losses':True},
            labels={sort_col: sort_by, 'team_name':''}, height=460,
        )
        fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
        fig.update_layout(**PLOT_STYLE, coloraxis_showscale=False, margin=dict(l=0,r=60,t=10,b=10), xaxis=GRID, yaxis=GRID)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("#### Wins vs Losses (top 10)")
        t10 = tp.sort_values('weighted_score', ascending=False).head(10)
        fig2 = go.Figure([
            go.Bar(name='Wins',   x=t10['team_acronym'], y=t10['wins'],   marker_color='#3fb950', text=t10['wins'],   textposition='inside'),
            go.Bar(name='Losses', x=t10['team_acronym'], y=t10['losses'], marker_color='#f85149', text=t10['losses'], textposition='inside'),
        ])
        fig2.update_layout(**PLOT_STYLE, barmode='stack', height=460,
                           legend=dict(orientation='h',y=1.08), margin=dict(l=0,r=0,t=30,b=0), xaxis=GRID, yaxis=GRID)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("#### Tier weight vs win rate")
    fig3 = px.scatter(
        tp, x='avg_tier_weight', y='win_rate_pct', size='matches_played', color='tournament_region',
        hover_name='team_name', hover_data={'team_acronym':True,'matches_played':True,'wins':True,'leagues':True},
        labels={'avg_tier_weight':'Avg tier (D=1→S=5)','win_rate_pct':'Win rate %','tournament_region':'Region'},
        color_discrete_sequence=px.colors.qualitative.Safe, height=360,
    )
    fig3.update_layout(**PLOT_STYLE, margin=dict(l=0,r=0,t=10,b=10), xaxis=GRID, yaxis=GRID)
    st.plotly_chart(fig3, use_container_width=True)


with tab2:
    st.markdown("#### Compare two teams")
    all_t = sorted(tp['team_name'].unique())
    if len(all_t) < 2:
        st.info("Need at least 2 teams. Adjust filters.")
    else:
        ca, cb = st.columns(2)
        ta = ca.selectbox("Team A", all_t, index=0)
        tb = cb.selectbox("Team B", all_t, index=min(1, len(all_t)-1))
        if ta == tb:
            st.warning("Pick two different teams.")
        else:
            sa, sb = tp[tp['team_name']==ta].iloc[0], tp[tp['team_name']==tb].iloc[0]
            cats = ['Win rate','Game length','Matches','Tier weight','Weighted score']
            cols = ['win_rate_pct','avg_game_length_mins','matches_played','avg_tier_weight','weighted_score']
            def norm(v, c): mn,mx=tp[c].min(),tp[c].max(); return round((v-mn)/(mx-mn)*100,1) if mx>mn else 50
            va = [norm(sa[c],c) if pd.notna(sa[c]) else 50 for c in cols]
            vb = [norm(sb[c],c) if pd.notna(sb[c]) else 50 for c in cols]
            fig_r = go.Figure([
                go.Scatterpolar(r=va+[va[0]], theta=cats+[cats[0]], fill='toself', name=ta, line=dict(color='#58a6ff'), fillcolor='rgba(88,166,255,0.15)'),
                go.Scatterpolar(r=vb+[vb[0]], theta=cats+[cats[0]], fill='toself', name=tb, line=dict(color='#f0883e'), fillcolor='rgba(240,136,62,0.15)'),
            ])
            fig_r.update_layout(
                polar=dict(bgcolor='rgba(0,0,0,0)',
                           radialaxis=dict(visible=True,range=[0,100],gridcolor='#30363d',color='#8b949e'),
                           angularaxis=dict(gridcolor='#30363d',color='#8b949e')),
                paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#e6edf3'),
                height=400, margin=dict(l=60,r=60,t=40,b=40),
            )
            st.plotly_chart(fig_r, use_container_width=True)
            m1,m2,m3 = st.columns(3)
            m1.metric("Win rate",       f"{sa['win_rate_pct']}%",     delta=f"{sa['win_rate_pct']-sb['win_rate_pct']:.1f}%")
            m2.metric("Matches played", int(sa['matches_played']),     delta=int(sa['matches_played']-sb['matches_played']))
            m3.metric("Weighted score", f"{sa['weighted_score']:.3f}", delta=f"{sa['weighted_score']-sb['weighted_score']:.3f}")

            st.markdown("#### Head-to-head history")
            h2h_ids = (fts.groupby('match_id')['team_name'].apply(set)
                       .loc[lambda x: x.apply(lambda s: ta in s and tb in s)].index.tolist())
            if h2h_ids:
                st.dataframe(
                    fts[fts['match_id'].isin(h2h_ids)][['match_name','team_name','series_score','is_winner','begin_at','league_name','tournament_tier']]
                    .sort_values('begin_at', ascending=False),
                    use_container_width=True, hide_index=True
                )
            else:
                st.info(f"No direct matches between {ta} and {tb} in current filter.")


with tab3:
    fin = fg[fg['finished'] & fg['length_mins'].notna()]
    if fin.empty:
        st.info("No finished game data available.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### Distribution")
            fh = px.histogram(fin, x='length_mins', nbins=20, color_discrete_sequence=['#58a6ff'],
                              labels={'length_mins':'Length (mins)'}, height=320)
            fh.add_vline(x=fin['length_mins'].mean(), line_dash='dash', line_color='#f0883e',
                         annotation_text=f"Mean: {fin['length_mins'].mean():.1f}m", annotation_font_color='#f0883e')
            fh.update_layout(**PLOT_STYLE, margin=dict(l=0,r=0,t=10,b=10), xaxis=GRID, yaxis=GRID, bargap=0.1)
            st.plotly_chart(fh, use_container_width=True)
        with c2:
            # Replace the "By tournament tier" block with this:
            if 'tournament_tier' in fin.columns and fin['tournament_tier'].notna().any():
                st.markdown("##### By tournament tier")
                tl = fin.groupby('tournament_tier')['length_mins'].agg(['mean','count']).reset_index()
                tl.columns = ['Tier','Mean','Games']
                ft = px.bar(tl, x='Tier', y='Mean', color='Mean', color_continuous_scale='Blues',
                            text='Mean', hover_data={'Games':True}, height=320)
                ft.update_traces(texttemplate='%{text:.1f}m', textposition='outside')
                ft.update_layout(**PLOT_STYLE, coloraxis_showscale=False,
                                margin=dict(l=0,r=0,t=20,b=10), xaxis=GRID, yaxis=GRID)
                st.plotly_chart(ft, use_container_width=True)
            else:
                st.info("No tier data available for finished games.")

        st.markdown("##### Over time")
        ts = fin.groupby(fin['begin_at'].dt.date)['length_mins'].mean().reset_index()
        ts.columns = ['date','avg']
        fl = px.line(ts, x='date', y='avg', markers=True, color_discrete_sequence=['#58a6ff'],
                     labels={'date':'Date','avg':'Avg length (mins)'}, height=280)
        fl.update_layout(**PLOT_STYLE, margin=dict(l=0,r=0,t=10,b=10), xaxis=GRID, yaxis=GRID)
        st.plotly_chart(fl, use_container_width=True)


with tab4:
    view = st.radio("Table", ["Team stats","Match facts","Game facts"], horizontal=True)
    tables = {"Team stats": tp, "Match facts": match_facts, "Game facts": game_facts}
    st.dataframe(tables[view], use_container_width=True, hide_index=True,
                 column_config={"win_rate_pct": st.column_config.ProgressColumn('Win %', min_value=0, max_value=100, format='%.1f%%')} if view=="Team stats" else {})
    st.download_button(f"⬇️ Download {view} as CSV",
                       data=tables[view].to_csv(index=False).encode(),
                       file_name=f"{view.lower().replace(' ','_')}.csv", mime='text/csv')

st.divider()

st.markdown("<p style='text-align:center;color:#484f58;font-size:0.72rem'>AsiaStats · normalization.py → dashboard.py · Next: Kafka + Spark layer</p>", unsafe_allow_html=True)
