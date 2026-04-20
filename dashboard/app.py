"""
Carbon Markets Dashboard

Single-page Streamlit app showing:
  - Key metrics row: total auctions, latest EUA clearing price, latest stream tick
  - EEX EUA clearing price over time (line chart)
  - KRBN intraday OHLCV candlestick (stream data)
  - EUTL top emitters by country (bar chart, when available)

Data is read directly from S3 Silver layer.
All panels degrade gracefully when data is not yet available.

Usage:
  pip install -r dashboard/requirements.txt
  streamlit run dashboard/app.py
"""

import io
import logging
import os
import sys
from datetime import date, datetime, timezone

import boto3
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from botocore.exceptions import ClientError
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ingestion.batch.utils import load_config

# ── Config ─────────────────────────────────────────────────────────────────────

_cfg       = load_config()
S3_BUCKET  = _cfg["aws"]["s3_bucket"]
AWS_REGION = _cfg["aws"]["region"]

try:
    _session = boto3.Session(
        profile_name=_cfg["aws"].get("profile"),
        region_name=AWS_REGION,
    )
    # Validate the profile exists by touching the session
    _session.get_credentials()
except Exception:
    _session = boto3.Session(region_name=AWS_REGION)
s3 = _session.client("s3")

log = logging.getLogger(__name__)

# ── S3 helpers ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _read_silver(key: str) -> pd.DataFrame:
    """Read a Silver Parquet file from S3. Returns empty DataFrame if missing."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(resp["Body"].read()))
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return pd.DataFrame()
        raise


def load_eex() -> pd.DataFrame:
    return _read_silver("silver/eex_auctions/eex_auctions.parquet")


def load_ohlcv() -> pd.DataFrame:
    return _read_silver("silver/carbon_prices_ohlcv/carbon_prices_ohlcv.parquet")


@st.cache_data(ttl=60)
def load_ticks(trade_date: str, symbol: str) -> pd.DataFrame:
    """Read Gold intraday tick file for a given symbol and date."""
    key = f"silver/stream/intraday_ticks/{symbol}/date={trade_date}.parquet"
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_parquet(io.BytesIO(resp["Body"].read()))
        df["event_time_utc"] = pd.to_datetime(df["event_time_utc"], utc=True)
        return df.sort_values("event_time_utc")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return pd.DataFrame()
        raise


def load_eutl() -> pd.DataFrame:
    return _read_silver("silver/eutl_verified_emissions/eutl_verified_emissions.parquet")


# ── Page layout ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="EU Carbon Markets",
    page_icon="📊",
    layout="wide",
)

# ── Bloomberg-style theme ──────────────────────────────────────────────────────
st.markdown("""
<style>
  /* Dark terminal background */
  .stApp { background-color: #0a0a0a; color: #dddddd; }
  section[data-testid="stSidebar"] { background-color: #111111; }

  /* General body text — readable light grey */
  p, li, span, div { color: #dddddd !important; }

  /* Headings */
  h1, h2, h3 {
    font-family: 'Courier New', monospace !important;
    color: #f5a623 !important;
    letter-spacing: 0.04em !important;
  }
  h1 { border-bottom: 1px solid #f5a623; padding-bottom: 8px; }

  /* Captions */
  .stCaption p, [data-testid="stCaptionContainer"] p {
    color: #cccccc !important;
    font-family: 'Courier New', monospace !important;
    font-size: 0.75rem !important;
  }

  /* Metrics */
  [data-testid="stMetricValue"] {
    color: #f5a623 !important;
    font-family: 'Courier New', monospace !important;
    font-size: 1.4rem !important;
    font-weight: 700 !important;
  }
  [data-testid="stMetricLabel"] {
    color: #bbbbbb !important;
    font-family: 'Courier New', monospace !important;
    font-size: 0.72rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
  }

  /* Divider */
  hr { border-color: #2a2a2a !important; }

  /* Labels (selectbox, radio) */
  label, [data-testid="stWidgetLabel"] p {
    color: #dddddd !important;
    font-family: 'Courier New', monospace !important;
    font-size: 0.82rem !important;
  }

  /* Selectbox input */
  [data-baseweb="select"] > div {
    background-color: #1a1a1a !important;
    border-color: #444444 !important;
    color: #f5a623 !important;
    font-family: 'Courier New', monospace !important;
  }
  [data-baseweb="select"] span { color: #f5a623 !important; }

  /* Selectbox dropdown */
  [data-baseweb="popover"] [role="option"] {
    background-color: #1a1a1a !important;
    color: #dddddd !important;
    font-family: 'Courier New', monospace !important;
  }
  [data-baseweb="popover"] [role="option"]:hover,
  [data-baseweb="popover"] [aria-selected="true"] {
    background-color: #2a2a2a !important;
    color: #f5a623 !important;
  }

  /* Radio */
  [data-testid="stRadio"] label,
  [data-testid="stRadio"] [data-testid="stMarkdownContainer"] p { color: #dddddd !important; }

  /* Expander header */
  [data-testid="stExpander"] summary p { color: #f5a623 !important; font-family: 'Courier New', monospace !important; }

  /* Alert/info boxes */
  [data-testid="stAlert"] { background-color: #111111 !important; border-color: #f5a623 !important; }
  [data-testid="stAlert"] p { color: #dddddd !important; }

  /* Plotly chart border */
  .stPlotlyChart { border: 1px solid #1e1e1e; border-radius: 4px; }

  /* Spinner */
  .stSpinner > div { border-top-color: #f5a623 !important; }
</style>
""", unsafe_allow_html=True)

# ── Header bar ─────────────────────────────────────────────────────────────────
st.markdown(
    f"""
    <div style="display:flex;justify-content:space-between;align-items:baseline;
                border-bottom:2px solid #f5a623;padding-bottom:6px;margin-bottom:16px">
      <span style="font-family:'Courier New',monospace;font-size:1.4rem;font-weight:700;
                   color:#f5a623;letter-spacing:0.06em">
        ◼ EU CARBON MARKETS
      </span>
      <span style="font-family:'Courier New',monospace;font-size:0.75rem;color:#555555">
        {date.today().strftime("%A %d %B %Y").upper()} &nbsp;·&nbsp; AARON ADEKOYA &nbsp;·&nbsp; {datetime.now(timezone.utc).strftime("%H:%M UTC")}
      </span>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── Auto-refresh every 60 s ───────────────────────────────────────────────────
st_autorefresh(interval=60_000, key="autorefresh")

# ── Load data ──────────────────────────────────────────────────────────────────

with st.spinner("Loading data from S3..."):
    eex   = load_eex()
    ohlcv = load_ohlcv()
    eutl  = load_eutl()

# ── Stream: Carbon ETF live prices ────────────────────────────────────────────

st.markdown(
    '<span style="font-family:\'Courier New\',monospace;font-size:1.2rem;font-weight:700;'
    'color:#f5a623">CARBON ETF LIVE PRICES &nbsp;</span>'
    '<span style="background:#22C55E;color:#000;font-family:\'Courier New\',monospace;'
    'font-size:0.65rem;font-weight:700;padding:2px 6px;border-radius:3px;'
    'letter-spacing:0.08em">● LIVE</span>',
    unsafe_allow_html=True,
)
st.caption("Exchange-traded funds tracking carbon markets, streamed tick-by-tick via Finnhub WebSocket → Kafka → S3. Auto-refreshes every 60 seconds.")

if not ohlcv.empty:
    ohlcv["trade_date"] = pd.to_datetime(ohlcv["trade_date"])
    symbols = sorted(ohlcv["symbol"].unique())

    ctrl_l, ctrl_r = st.columns([2, 2])
    selected = ctrl_l.selectbox("Ticker symbol", symbols, index=0)
    view = ctrl_r.radio(
        "Chart view",
        ["Price chart (OHLC candlestick)", "Tick price track"],
        index=1,
        horizontal=True,
        help="OHLC = Open / High / Low / Close prices per day. Tick price track shows every individual trade captured from the stream.",
    )

    df_sym = ohlcv[ohlcv["symbol"] == selected].sort_values("trade_date")
    latest_row = df_sym.iloc[-1]
    is_up = latest_row["close"] >= latest_row["open"]
    price_color = "#22C55E" if is_up else "#EF4444"
    arrow = "▲" if is_up else "▼"
    day_chg = latest_row["close"] - latest_row["open"]
    day_chg_pct = day_chg / latest_row["open"] * 100

    # ── Stream metrics ─────────────────────────────────────────────────────────
    price_col, m1, m2, m3 = st.columns([2, 1, 1, 1])
    price_col.markdown(
        f"""
        <div style="padding:12px 16px;border:1px solid {price_color};border-radius:4px;
                    background:#111111;font-family:'Courier New',monospace">
          <div style="font-size:0.72rem;color:#555555;margin-bottom:2px;text-transform:uppercase;letter-spacing:0.05em">
            {selected} &nbsp;·&nbsp; LAST PRICE
          </div>
          <div style="font-size:2rem;font-weight:700;color:{price_color};line-height:1.1">
            ${latest_row['close']:.2f}
          </div>
          <div style="font-size:0.8rem;color:{price_color};margin-top:4px">
            {arrow} {abs(day_chg):.2f} ({abs(day_chg_pct):.2f}%) &nbsp;·&nbsp;
            <span style="color:#444444">DAY CHANGE &nbsp;·&nbsp; UPDATED EVERY 60s</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    m1.metric(
        "Days of data",
        len(df_sym),
        help="Number of trading days for which we have stream data for this symbol",
    )
    vwap_val = latest_row.get("vwap", None)
    m2.metric(
        "VWAP",
        f"${vwap_val:.2f}" if pd.notna(vwap_val) else "—",
        help="Volume-Weighted Average Price — the average price paid per share today, weighted by trade size. A fairer price benchmark than a simple average.",
    )
    m3.metric(
        "Ticks captured today",
        f"{int(df_sym['tick_count'].sum()):,}",
        help="Number of individual trades received from the Finnhub WebSocket stream today. Low-volume ETFs will have fewer ticks.",
    )

    # ── Stream chart ───────────────────────────────────────────────────────────
    if "OHLC" in view:
        if len(df_sym) >= 2:
            fig = go.Figure(go.Candlestick(
                x=df_sym["trade_date"],
                open=df_sym["open"],
                high=df_sym["high"],
                low=df_sym["low"],
                close=df_sym["close"],
                name=selected,
                increasing_line_color="#22C55E",
                decreasing_line_color="#EF4444",
            ))
        else:
            row = df_sym.iloc[0]
            bar_color = price_color
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=[row["trade_date"], row["trade_date"]],
                y=[row["low"], row["high"]],
                mode="lines",
                line=dict(color=bar_color, width=4),
                showlegend=False,
                hoverinfo="skip",
            ))
            fig.add_trace(go.Scatter(
                x=[row["trade_date"], row["trade_date"]],
                y=[row["open"], row["close"]],
                mode="markers",
                marker=dict(size=14, color=["#94A3B8", bar_color],
                            symbol=["line-ew", "line-ew"],
                            line=dict(width=3, color=["#94A3B8", bar_color])),
                showlegend=False,
                hovertemplate=["Open: $%{y:.2f}<extra></extra>",
                               "Close: $%{y:.2f}<extra></extra>"],
            ))
            for label, val, ay in [
                ("High",  row["high"],  -20),
                ("Close", row["close"],  20 if is_up else -20),
                ("Open",  row["open"],  -20 if is_up else 20),
                ("Low",   row["low"],    20),
            ]:
                fig.add_annotation(
                    x=row["trade_date"], y=val,
                    text=f"<b>{label}</b> ${val:.2f}",
                    showarrow=True, arrowhead=0, arrowcolor="#6B7280",
                    ax=60, ay=ay, font=dict(size=12),
                )
        fig.update_layout(
            yaxis_title="Price (USD)",
            xaxis_title=None,
            xaxis_rangeslider_visible=False,
            hovermode="x unified",
            margin=dict(l=0, r=0, t=10, b=0),
            height=350,
            paper_bgcolor="#0a0a0a",
            plot_bgcolor="#0a0a0a",
            font=dict(family="Courier New", color="#aaaaaa", size=11),
            xaxis=dict(gridcolor="#1e1e1e", linecolor="#333333"),
            yaxis=dict(gridcolor="#1e1e1e", linecolor="#333333"),
        )

    else:  # Tick price track
        today_str = latest_row["trade_date"].strftime("%Y-%m-%d")
        ticks_sym = load_ticks(today_str, selected)

        if ticks_sym.empty:
            st.info(f"No tick data found for {selected} on {today_str}. Check back once the market has been open for a few minutes.")
            fig = go.Figure()
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=ticks_sym["event_time_utc"],
                y=ticks_sym["price"],
                mode="lines+markers",
                marker=dict(size=4, color=price_color),
                line=dict(color=price_color, width=1.5),
                name=f"{selected} price per trade",
                hovertemplate="%{x|%H:%M:%S UTC}<br>$%{y:.2f}<extra></extra>",
            ))
            fig.update_layout(
                yaxis_title="Price (USD)",
                xaxis_title="Time (UTC)",
                hovermode="x unified",
                margin=dict(l=0, r=0, t=10, b=0),
                height=350,
                paper_bgcolor="#0a0a0a",
                plot_bgcolor="#0a0a0a",
                font=dict(family="Courier New", color="#aaaaaa", size=11),
                xaxis=dict(gridcolor="#1e1e1e", linecolor="#333333"),
                yaxis=dict(gridcolor="#1e1e1e", linecolor="#333333"),
            )

    st.plotly_chart(fig, use_container_width=True)

else:
    st.info("Stream data not yet available. Market data is collected during US market hours (09:15–16:15 ET / 14:15–21:15 London).")

st.divider()

# ── EEX: EU Carbon Allowance auction prices ────────────────────────────────────

st.subheader("EU Carbon Allowance — Primary Auction Prices")
st.caption("European Energy Exchange (EEX) runs twice-weekly auctions where EU companies buy carbon allowances (EUAs). Each allowance permits 1 tonne of CO₂ emissions.")

if not eex.empty:
    eex["auction_date"] = pd.to_datetime(eex["auction_date"])
    latest_eex = eex.sort_values("auction_date").iloc[-1]

    m1, m2, m3 = st.columns(3)
    m1.metric(
        "Latest Clearing Price",
        f"€{latest_eex['clearing_price_eur']:.2f} / tonne CO₂",
        help="The price at which the most recent EEX auction cleared — i.e. the price paid per allowance (1 tonne CO₂) by EU emitters.",
    )
    m2.metric(
        "Total Auctions Ingested",
        f"{len(eex):,}",
        help="Total number of EEX auction records loaded into the pipeline since 2020.",
    )
    m3.metric(
        "Most Recent Auction",
        latest_eex["auction_date"].strftime("%d %b %Y"),
    )
    st.caption(f"Last ingested: {latest_eex['auction_date'].strftime('%d %b %Y')} · Silver updated on pipeline run")

    fig_eex = go.Figure()
    if "proceeds_eur" in eex.columns:
        eex_proc = eex.copy()
        eex_proc["year"] = eex_proc["auction_date"].dt.year
        annual_proceeds = eex_proc.groupby("year")["proceeds_eur"].sum().reset_index()
        fig_eex.add_trace(go.Bar(
            x=pd.to_datetime(annual_proceeds["year"].astype(str)),
            y=annual_proceeds["proceeds_eur"] / 1e9,
            name="Annual proceeds (€bn)",
            marker_color="#888888",
            opacity=0.35,
            yaxis="y2",
            hovertemplate="<b>%{x|%Y}</b><br>€%{y:.2f}bn raised<extra></extra>",
        ))
    fig_eex.add_trace(go.Scatter(
        x=eex["auction_date"],
        y=eex["clearing_price_eur"],
        mode="lines+markers",
        marker=dict(size=3),
        line=dict(color="#f5a623", width=1.5),
        name="Clearing price (€/tonne CO₂)",
        yaxis="y1",
        hovertemplate="%{x|%d %b %Y}<br>€%{y:.2f}/tonne CO₂<extra></extra>",
    ))
    fig_eex.update_layout(
        yaxis=dict(title="€ per tonne CO₂", gridcolor="#1e1e1e", linecolor="#333333"),
        yaxis2=dict(title="Annual proceeds (€bn)", overlaying="y", side="right",
                    gridcolor="#1e1e1e", linecolor="#333333"),
        xaxis=dict(gridcolor="#1e1e1e", linecolor="#333333"),
        xaxis_title=None,
        hovermode="x unified",
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
        paper_bgcolor="#0a0a0a",
        plot_bgcolor="#0a0a0a",
        font=dict(family="Courier New", color="#aaaaaa", size=11),
        legend=dict(orientation="h", x=0, y=1.08, font=dict(color="#aaaaaa")),
    )
    st.plotly_chart(fig_eex, use_container_width=True)

    # ── EEX volume & bid-to-cover ──────────────────────────────────────────────
    if "volume_eua" in eex.columns and "bid_to_cover_ratio" in eex.columns:
        fig_vol = go.Figure()
        fig_vol.add_trace(go.Bar(
            x=eex["auction_date"],
            y=eex["volume_eua"],
            name="Volume (allowances)",
            marker_color="#2a2a2a",
            yaxis="y1",
            hovertemplate="%{x|%d %b %Y}<br>%{y:,.0f} allowances<extra></extra>",
        ))
        fig_vol.add_trace(go.Scatter(
            x=eex["auction_date"],
            y=eex["bid_to_cover_ratio"],
            name="Bid-to-Cover",
            line=dict(color="#f5a623", width=1.5),
            yaxis="y2",
            hovertemplate="%{x|%d %b %Y}<br>Bid-to-Cover: %{y:.2f}x<extra></extra>",
        ))
        fig_vol.update_layout(
            yaxis=dict(title="Volume (allowances)", gridcolor="#1e1e1e", linecolor="#333333"),
            yaxis2=dict(title="Bid-to-Cover Ratio", overlaying="y", side="right",
                        gridcolor="#1e1e1e", linecolor="#333333"),
            xaxis=dict(gridcolor="#1e1e1e", linecolor="#333333"),
            hovermode="x unified",
            margin=dict(l=0, r=0, t=10, b=0),
            height=280,
            paper_bgcolor="#0a0a0a",
            plot_bgcolor="#0a0a0a",
            font=dict(family="Courier New", color="#aaaaaa", size=11),
            legend=dict(orientation="h", x=0, y=1.08, font=dict(color="#aaaaaa")),
        )
        st.caption("AUCTION VOLUME & BID-TO-COVER RATIO — demand pressure indicator")
        st.plotly_chart(fig_vol, use_container_width=True)

    with st.expander("Raw auction data"):
        st.dataframe(
            eex[["auction_date", "clearing_price_eur", "volume_eua",
                 "bid_to_cover_ratio", "country"]]
            .rename(columns={
                "auction_date": "Date",
                "clearing_price_eur": "Clearing Price (€)",
                "volume_eua": "Volume (allowances)",
                "bid_to_cover_ratio": "Bid-to-Cover Ratio",
                "country": "Country",
            })
            .sort_values("Date", ascending=False)
            .reset_index(drop=True),
            use_container_width=True,
        )
else:
    st.info("EEX auction data not yet available. Run `processing/silver/eex_to_silver.py` first.")

st.divider()

# ── EUTL: Verified emissions by country ───────────────────────────────────────

st.subheader("Verified Emissions by Country")
st.caption("Annual CO₂ emissions verified under the EU Emissions Trading System, sourced from the EU Transaction Log (EUTL) — the official registry of all ~11,000 regulated industrial installations.")

if not eutl.empty:
    latest_year = int(eutl["year"].dropna().max())
    eutl_year = eutl[eutl["year"] == latest_year]

    country_totals = (
        eutl_year.groupby("country_code")["verified_emissions_tco2"]
        .sum()
        .sort_values(ascending=False)
        .head(15)
        .reset_index()
    )

    fig_eutl = go.Figure(go.Bar(
        x=country_totals["country_code"],
        y=country_totals["verified_emissions_tco2"] / 1e6,
        marker_color="#f5a623",
        hovertemplate="%{x}<br>%{y:.1f} million tonnes CO₂<extra></extra>",
    ))

    # Year-over-year comparison
    years = sorted(eutl["year"].dropna().unique())
    if len(years) >= 2:
        prev_year = int(years[-2])
        prev_totals = (
            eutl[eutl["year"] == prev_year]
            .groupby("country_code")["verified_emissions_tco2"]
            .sum()
        )
        fig_eutl.add_trace(go.Bar(
            x=country_totals["country_code"],
            y=[prev_totals.get(c, 0) / 1e6 for c in country_totals["country_code"]],
            name=str(prev_year),
            marker_color="#333333",
            hovertemplate="%{x}<br>%{y:.1f} Mt CO₂<extra></extra>",
        ))
        fig_eutl.data[0].name = str(latest_year)
        fig_eutl.update_layout(barmode="group")
    fig_eutl.update_layout(
        yaxis_title="Verified emissions (million tonnes CO₂)",
        xaxis_title=None,
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
        paper_bgcolor="#0a0a0a",
        plot_bgcolor="#0a0a0a",
        font=dict(family="Courier New", color="#aaaaaa", size=11),
        xaxis=dict(gridcolor="#1e1e1e", linecolor="#333333"),
        yaxis=dict(gridcolor="#1e1e1e", linecolor="#333333"),
    )
    st.plotly_chart(fig_eutl, use_container_width=True)
    yoy_note = f"{prev_year} vs {latest_year} · " if len(years) >= 2 else ""
    st.caption(f"Top 15 countries by verified emissions · {yoy_note}million tonnes CO₂")
else:
    st.info("Emissions data not yet available. The EEA server is returning errors intermittently — retry later.")

st.divider()

# ── Pipeline Architecture ─────────────────────────────────────────────────────

with st.expander("◼ PIPELINE ARCHITECTURE", expanded=False):
    st.markdown(
        """
        <div style="font-family:'Courier New',monospace;font-size:0.82rem;
                    color:#cccccc;line-height:2">
        <table style="width:100%;border-collapse:collapse">
          <tr>
            <td style="color:#f5a623;padding-right:16px;white-space:nowrap;vertical-align:top">INGESTION</td>
            <td>
              <b style="color:#f5a623">Batch (GitHub Actions)</b><br>
              · EEX xlsx → S3 Bronze &nbsp;|&nbsp; runs every Tuesday &amp; Thursday at 07:00 UTC<br>
              · EUTL bulk CSV → S3 Bronze &nbsp;|&nbsp; runs annually each April<br><br>
              <b style="color:#f5a623">Stream (Raspberry Pi 8 GB)</b><br>
              · Finnhub WebSocket → Kafka (KRaft, local) → S3 Bronze<br>
              · 5 symbols: KRBN, KCCA, ICLN, KEUA, GRN &nbsp;|&nbsp; tick-by-tick, flushed every 60 s
            </td>
          </tr>
          <tr><td colspan=2 style="padding:6px 0"><hr style="border-color:#1e1e1e"></td></tr>
          <tr>
            <td style="color:#f5a623;padding-right:16px;white-space:nowrap;vertical-align:top">STORAGE</td>
            <td>
              <b style="color:#f5a623">S3 Medallion Architecture</b><br>
              · <b>Bronze</b> — raw, immutable, partitioned by date (Parquet)<br>
              · <b>Silver</b> — cleaned, typed, deduplicated, VWAP-enriched (Parquet)<br>
              &nbsp;&nbsp;&nbsp;&nbsp;<code>carbon_prices_ohlcv/</code> &nbsp;·&nbsp; <code>stream/intraday_ticks/{symbol}/date={date}.parquet</code>
            </td>
          </tr>
          <tr><td colspan=2 style="padding:6px 0"><hr style="border-color:#1e1e1e"></td></tr>
          <tr>
            <td style="color:#f5a623;padding-right:16px;white-space:nowrap;vertical-align:top">PROCESSING</td>
            <td>
              · Python transforms: <code>eex_to_silver.py</code>, <code>stream_to_silver.py</code>, <code>stream_to_silver_ticks.py</code><br>
              · Stream Silver &amp; intraday ticks run every 60 s via systemd timers on the Pi<br>
              · VWAP computed as Σ(price × volume) / Σ(volume) per symbol per day<br>
              · <code>stream_to_silver_ticks.py</code> consolidates N Bronze files → 1 Silver object per symbol per day
            </td>
          </tr>
          <tr><td colspan=2 style="padding:6px 0"><hr style="border-color:#1e1e1e"></td></tr>
          <tr>
            <td style="color:#f5a623;padding-right:16px;white-space:nowrap;vertical-align:top">SERVING</td>
            <td>
              · Streamlit dashboard — reads directly from S3 Silver<br>
              · Hosted on Raspberry Pi, exposed publicly via Cloudflare Tunnel (HTTPS)<br>
              · Auto-refreshes every 60 s &nbsp;|&nbsp; AWS IAM least-privilege access
            </td>
          </tr>
          <tr><td colspan=2 style="padding:6px 0"><hr style="border-color:#1e1e1e"></td></tr>
          <tr>
            <td style="color:#f5a623;padding-right:16px;white-space:nowrap;vertical-align:top">INFRA</td>
            <td>
              · AWS IAM &amp; S3 provisioned with Terraform<br>
              · All Pi services managed as systemd units
            </td>
          </tr>
        </table>
        </div>
        """,
        unsafe_allow_html=True,
    )
