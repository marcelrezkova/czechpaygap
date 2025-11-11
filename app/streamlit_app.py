import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="CzechPayGap | Future of Work Insight",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === Load Data ===
@st.cache_data
def load_data():
    return pd.read_csv("data/wages_comparison.csv")

df = load_data()

# === Custom CSS ===
with open("app/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# === Header ===
st.markdown("""
<div class="header">
    <h1>💠 CzechPayGap</h1>
    <p class="subtitle">Explore the real economy of work. Data meets reality.</p>
</div>
""", unsafe_allow_html=True)

# === Key Metrics ===
col1, col2, col3, col4 = st.columns(4)
col1.metric("🧭 Regions", len(df))
col2.metric("� ČSÚ Avg", f"{int(df['avg_wage'].mean()):,} Kč")
col3.metric("�💰 Avg Pay Gap", f"{int(df['pay_gap'].mean()):,} Kč")
col4.metric("� Total Offers", int(df['offers'].sum()))

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

# === ČSÚ Statistics ===
st.markdown("### 📊 Statistiky z Českého statistického úřadu")
csu_col1, csu_col2, csu_col3 = st.columns(3)
csu_col1.metric("📍 Min. průměrná mzda", f"{int(df['avg_wage'].min()):,} Kč", 
                delta=df[df['avg_wage'] == df['avg_wage'].min()]['region'].values[0])
csu_col2.metric("📊 Celkový průměr ČSÚ", f"{int(df['avg_wage'].mean()):,} Kč")
csu_col3.metric("📍 Max. průměrná mzda", f"{int(df['avg_wage'].max()):,} Kč",
                delta=df[df['avg_wage'] == df['avg_wage'].max()]['region'].values[0])

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

# === Visualization 1 ===
st.markdown("### ⚙️ Průměrné vs. Nabízené mzdy podle regionu")
fig = px.bar(
    df,
    x="region",
    y=["avg_wage", "avg_offer"],
    barmode="group",
    text_auto=".0f",
    color_discrete_sequence=["#00C2FF", "#FF00C8"],
    labels={"avg_wage": "ČSÚ Průměr", "avg_offer": "Nabídky z portálů"}
)
fig.update_layout(
    template="plotly_dark",
    xaxis_title="Region",
    yaxis_title="Mzda (Kč)",
    legend_title="Zdroj dat",
)
st.plotly_chart(fig, use_container_width=True)

# === Visualization 2 ===
st.markdown("### 🔮 Index mzdové reality (PayGap Index)")
fig2 = px.scatter(
    df,
    x="avg_wage",
    y="avg_offer",
    color="pay_gap",
    size="offers",
    hover_name="region",
    color_continuous_scale=["#FF004C", "#FFB800", "#00FF9C"],
    labels={"avg_wage": "Oficiální mzda (ČSÚ)", "avg_offer": "Nabízená mzda"},
)
fig2.update_layout(template="plotly_dark")
st.plotly_chart(fig2, use_container_width=True)

# === Footer ===
st.markdown("""
<div class="footer">
    <p>🚀 Built with ❤️ by <b>Marcela Řezková</b> · CzechPayGap 2025</p>
    <p class="note">Because data never lie — but sometimes, salaries do.</p>
</div>
""", unsafe_allow_html=True)
