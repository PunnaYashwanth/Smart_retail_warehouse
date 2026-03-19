import streamlit as st
import pandas as pd
import json
import sqlite3
import plotly.express as px
from pathlib import Path
from etl import extract, transform, load_to_dw, export_outputs
import time
import os

# ---- CONFIG & SETTINGS ----
st.set_page_config(
    page_title="Smart Retail Sales Insights",
    layout="wide",
    page_icon="🛍️",
    initial_sidebar_state="expanded"
)

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "data"
DATA_DIR.mkdir(exist_ok=True)

# ---- CUSTOM CSS (LIGHT BLACK & GRAY THEME) ----
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap');

/* Main Background - Light Black / Soft Dark */
.stApp {
    background-color: #1e1e1e; /* Light Black */
    font-family: 'Outfit', sans-serif;
    color: #e0e0e0; /* Soft White text */
}

/* Cards - Light Gray */
.glass-card {
    background: #2b2b2b; /* Dark Gray Card */
    border: 1px solid #404040;
    border-radius: 16px;
    padding: 30px;
    margin-bottom: 25px;
    box-shadow: 0 4px 6px rgba(0,0,0,0.3);
}

/* Typography */
h1, h2, h3 {
    font-weight: 700;
    letter-spacing: -0.5px;
    color: #ffffff;
}

.gradient-text {
    color: #ffffff; 
    font-size: 2.5rem;
    font-weight: 800;
}

.sub-header {
    font-size: 1.1rem;
    color: #a0a0a0;
    font-weight: 400;
}

/* Sidebar - Matches Main Theme */
section[data-testid="stSidebar"] {
    background-color: #1e1e1e;
    border-right: 1px solid #404040;
}

/* Buttons - Muted Blue Accent */
.stButton>button {
    background: #4a90e2; /* Muted Blue */
    color: white;
    border: none;
    padding: 0.6rem 2rem;
    border-radius: 8px;
    font-weight: 600;
    transition: all 0.2s ease;
}

.stButton>button:hover {
    background: #357abd;
    transform: translateY(-1px);
}

/* Metrics */
[data-testid="stMetricValue"] {
    font-size: 2.5rem !important;
    font-weight: 700;
    color: #ffffff;
}

[data-testid="stMetricLabel"] {
    font-size: 0.9rem;
    color: #a0a0a0;
}

/* Uploader Area */
[data-testid="stFileUploader"] {
    background: #2b2b2b;
    padding: 25px;
    border-radius: 12px;
    border: 1px dashed #4a90e2;
    text-align: center;
}

/* Success/Info Alerts */
.stAlert {
    background-color: #2b2b2b;
    color: #e0e0e0;
    border: 1px solid #404040;
}

/* Sidebar Navigation Text */
[data-testid="stSidebarNav"] span {
    color: #e0e0e0 !important;
    font-weight: 500;
}

div[data-testid="stRadio"] label {
    color: #e0e0e0 !important;
    font-size: 1rem !important;
}
div[data-testid="stRadio"] div[role="radiogroup"] > label {
    color: #e0e0e0 !important;
}
p {
    color: #e0e0e0;
}

</style>
""", unsafe_allow_html=True)

# ---- UTILS ----
def get_db_path():
    return BASE / "warehouse.db"

def save_uploaded_file(uploaded_file):
    try:
        file_ext = Path(uploaded_file.name).suffix.lower()
        if file_ext == ".csv":
            target_path = DATA_DIR / "Sample - Superstore.csv"
            return target_path, "Structured (CSV)", "📄"
        elif file_ext == ".json":
            target_path = DATA_DIR / "orders.json"
            return target_path, "Semi-Structured (JSON)", "📜"
        elif file_ext == ".txt":
            target_path = DATA_DIR / "reviews.txt"
            return target_path, "Unstructured (TXT)", "📝"
        else:
            return None, "Unknown", "❓"
    except Exception:
        return None, "Error", "❌"

# ---- SIDEBAR ----
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3081/3081559.png", width=50) 
    st.markdown('<div style="font-size: 1.2rem; font-weight: 700; color: white; margin-bottom: 20px;">Smart Retail<br><span style="color: #4a90e2;">Sales Insights</span></div>', unsafe_allow_html=True)
    
    st.markdown("### Navigation")
    # UPDATED ORDER: Dashboard First
    page = st.radio(
        "Menu",
        ["Dashboard", "Upload & Process", "Architecture", "Warehouse Data"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    st.markdown("### System Status")
    if get_db_path().exists():
        st.caption(f"Status: ✅ Online")
    else:
        st.caption("Status: ❌ Empty")

# ---- MAIN PAGES ----

try:
    if page == "Upload & Process":
        st.markdown('<h1 class="gradient-text">Upload & Process</h1>', unsafe_allow_html=True)
        st.write("Step 1: Upload your Raw Data (CSV, JSON, or TXT). System Auto-Detects Format.")
        
        col_up, col_info = st.columns([3, 2])
        
        with col_up:
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            uploaded_file = st.file_uploader("Upload File", type=["csv", "json", "txt"])
            
            if uploaded_file:
                path, dtype, icon = save_uploaded_file(uploaded_file)
                if path:
                    st.success(f"Uploaded: {uploaded_file.name}")
                    
                    st.markdown(f"""
                    <div style="background: #2b2b2b; border-left: 4px solid #4a90e2; padding: 15px; margin: 20px 0; border-radius: 4px;">
                        <h4 style="margin:0; color: #4a90e2;">{icon} Detected Format: {dtype}</h4>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Preview Raw
                    with st.expander("👀 View Raw File Content"):
                        with open(path, "rb") as f:
                            if "CSV" in dtype:
                                st.dataframe(pd.read_csv(path, encoding='latin1').head(5), use_container_width=True)
                            elif "JSON" in dtype:
                                st.json(json.load(f))
                            elif "TXT" in dtype:
                                st.text(f.read().decode('latin1')[:500] + "...")

                    st.markdown("---")
                    
                    if st.button("🚀 Transform to Structured Output", type="primary"):
                        with st.spinner("ETL Running: Extracting -> Transforming -> Loading..."):
                            try:
                                # Run ETL
                                df_raw, o_raw, r_raw = extract(target_type=dtype)
                                sales, prod, cust, o_df, r_df, agg = transform(df_raw, o_raw, r_raw)
                                load_to_dw(sales, prod, cust, o_df, r_df, agg)
                                export_outputs(agg, r_df)
                                
                                st.balloons()
                                st.success("✅ Transformation Complete!")
                                
                                # IMMEDIATE STRUCTURED OUTPUT DISPLAY
                                st.markdown("### ✨ Transformed Structured Data")
                                st.info("The system has converted your uploaded file into a Structured Table format.")
                                
                                tab1, tab2, tab3 = st.tabs(["Fact_Sales", "Fact_Orders", "Dim_Sentiment"])
                                
                                with tab1:
                                    if "CSV" in dtype or not sales.empty:
                                        st.write("**From CSV (Structured):**")
                                        st.dataframe(sales.head(50), use_container_width=True)
                                    else:
                                        st.caption("No CSV data processed.")
                                        
                                with tab2:
                                    if "JSON" in dtype or not o_df.empty:
                                        st.write("**From JSON (Semi-Structured):**")
                                        st.dataframe(o_df.head(50), use_container_width=True)
                                    else:
                                        st.caption("No JSON data processed.")
                                        
                                with tab3:
                                    if "TXT" in dtype or not r_df.empty:
                                        st.write("**From TXT (Unstructured to Structured):**")
                                        st.dataframe(r_df.head(50), use_container_width=True)
                                    else:
                                        st.caption("No Text data processed.")

                            except Exception as e:
                                st.error(f"ETL Failed: {e}")
            else:
                st.info("Waiting for file upload...")
            st.markdown('</div>', unsafe_allow_html=True)

        with col_info:
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.subheader("Supported Transformations")
            st.markdown("""
            **📄 Structured (CSV)**
            - Direct mapping to Relational Tables.
            
            **📜 Semi-Structured (JSON)**
            - Flattens nested JSON objects into Rows & Columns.
            
            **📝 Unstructured (TXT)**
            - NLP extracts Sentiment.
            - Converts Free Text -> Structured Sentiment Score.
            """)
            st.markdown('</div>', unsafe_allow_html=True)
            
    elif page == "Dashboard":
        st.markdown('<h1 class="gradient-text">Insights Dashboard</h1>', unsafe_allow_html=True)
        st.write("Aggregated view of all data in the warehouse.")
        
        if not get_db_path().exists():
            st.warning("No data found in Warehouse. Please go to Upload & Process.")
        else:
            conn = sqlite3.connect(get_db_path())
            try:
                # KPIs
                sales_val = pd.read_sql("SELECT SUM(sales) as v FROM fact_sales", conn).iloc[0]['v']
                orders_val = pd.read_sql("SELECT COUNT(DISTINCT order_id) as v FROM fact_sales", conn).iloc[0]['v']
                
                # Sentiment
                sent_df = pd.read_sql("SELECT sentiment FROM dim_review", conn)
                if not sent_df.empty:
                    pos = sent_df[sent_df['sentiment']=='positive'].shape[0]
                    score = int((pos/len(sent_df))*100)
                else:
                    score = 0
                
                # Cards
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown(f"""<div class="glass-card">
                        <h3>Revenue</h3>
                        <div style="font-size: 2.5rem; font-weight: 800; color: #ffffff;">${sales_val:,.0f}</div>
                    </div>""", unsafe_allow_html=True)
                with c2:
                    st.markdown(f"""<div class="glass-card">
                        <h3>Orders</h3>
                        <div style="font-size: 2.5rem; font-weight: 800; color: #ffffff;">{orders_val:,}</div>
                    </div>""", unsafe_allow_html=True)
                with c3:
                    st.markdown(f"""<div class="glass-card">
                        <h3>Sentiment</h3>
                        <div style="font-size: 2.5rem; font-weight: 800; color: #ffffff;">{score}%</div>
                    </div>""", unsafe_allow_html=True)

                # Charts
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
                    st.subheader("Sales by Category")
                    cat_df = pd.read_sql("SELECT dp.category, SUM(fs.sales) as s FROM fact_sales fs JOIN dim_product dp ON fs.product_id=dp.product_id GROUP BY dp.category", conn)
                    # Muted Blue Palette
                    fig1 = px.pie(cat_df, values='s', names='category', hole=0.5, 
                                  color_discrete_sequence=['#4a90e2', '#7fb3d5', '#a9cce3', '#d4e6f1'])
                    fig1.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#e0e0e0", showlegend=False)
                    st.plotly_chart(fig1, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)

                with col2:
                    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
                    st.subheader("Sales Trend")
                    trend_df = pd.read_sql("SELECT sale_date, SUM(sales) as s FROM fact_sales GROUP BY sale_date ORDER BY sale_date", conn)
                    fig2 = px.area(trend_df, x='sale_date', y='s', 
                                   color_discrete_sequence=['#4a90e2'])
                    fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#e0e0e0", 
                                       xaxis_showgrid=False, yaxis_gridcolor='rgba(255,255,255,0.05)')
                    st.plotly_chart(fig2, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)

            except Exception:
                st.error("Data syncing error. Please reload data.")
            finally:
                conn.close()

    elif page == "Architecture":
        st.markdown('<h1 class="gradient-text">System Architecture</h1>', unsafe_allow_html=True)
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.graphviz_chart("""
            digraph G {
                bgcolor="transparent";
                node [style=filled, fillcolor="#2b2b2b", fontcolor="white", color="#4a90e2", shape=box, fontname="Outfit", penwidth=1.5];
                edge [color="#606060", fontcolor="#a0a0a0"];
                
                Input [label="Input Files", shape=note, fillcolor="#404040"];
                ETL [label="ETL Engine", shape=gear, fillcolor="#4a90e2"];
                DW [label="Warehouse", shape=cylinder, fillcolor="#505050"];
                
                Input -> ETL;
                ETL -> DW;
            }
        """)
        st.markdown('</div>', unsafe_allow_html=True)

    elif page == "Warehouse Data":
        st.markdown('<h1 class="gradient-text">Warehouse Explorer</h1>', unsafe_allow_html=True)
        st.info("ℹ️ **What is this?** This is the **Data Warehouse** - the permanent storage where specific file uploads are combined into a historical record.")
        
        if get_db_path().exists():
            conn = sqlite3.connect(get_db_path())
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()
            
            t = st.selectbox("Select Table", tables)
            if t:
                df = pd.read_sql(f"SELECT * FROM {t}", conn)
                st.dataframe(df.head(100), use_container_width=True)
            conn.close()
        else:
            st.error("Warehouse is empty.")

except Exception:
    st.error("Application Error. Please refresh.")
