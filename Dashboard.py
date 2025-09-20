import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import os

# --- 1. Database Connection & Caching ---
@st.cache_data
def load_data():
    """Connects to the MySQL database, queries the nonfarm_payrolls table, and caches the data."""
    try:
        conn = mysql.connector.connect(
            host=os.environ.get("DB_HOST", "localhost"),
            user=os.environ.get("DB_USER", "root"),
            password=os.environ.get("DB_PASSWORD", "1155"),
            database=os.environ.get("DB_NAME", "fredetl")
        )
        query = "SELECT * FROM nonfarm_payrolls;"
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Ensure the date column is in datetime format
        df['date'] = pd.to_datetime(df['date'])
        
        st.success("Data loaded and cached successfully!")
        return df
    except Exception as e:
        st.error(f"Error connecting to the database or loading data: {e}")
        return None

# --- 2. Custom Styling ---
def add_custom_css():
    st.markdown("""
        <style>
        .main {
            background-color: #f5f5f5;
        }
        .css-1av0vzn {
            display: flex;
            justify-content: center;
        }
        h1 {
            color: #333;
            text-align: center;
        }
        .st-emotion-cache-1q1n1p {
            border-radius: 10px;
            box-shadow: 0 4px 8px 0 rgba(0,0,0,0.1), 0 6px 20px 0 rgba(0,0,0,0.1);
            padding: 20px;
            margin-bottom: 20px;
            background-color: white;
        }
        .css-1f7l053 {
            border-radius: 10px;
            box-shadow: 0 4px 8px 0 rgba(0,0,0,0.05), 0 6px 20px 0 rgba(0,0,0,0.05);
            padding: 10px;
            background-color: white;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-top: 20px;
        }
        th, td {
            text-align: left;
            padding: 8px;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        </style>
    """, unsafe_allow_html=True)

# --- 3. OLAP Analyses & Visualizations ---
def create_slicing_charts(df):
    st.header("Slicing Analysis")

    # Slicing 1: Average payroll employment by year (2010-2025)
    st.subheader("Average Annual Payroll Employment (2010-2025)")
    # Slider for selecting year range
    min_year = int(df['date'].dt.year.min())
    max_year = int(df['date'].dt.year.max())
    start_year, end_year = st.slider(
        "Select year range:",
        min_value=2010 if min_year <= 2010 else min_year,
        max_value=2025 if max_year >= 2025 else max_year,
        value=(2010, 2025),
        step=1
    )
    df_slice1 = df[(df['date'].dt.year >= start_year) & (df['date'].dt.year <= end_year)]
    avg_annual_employment = df_slice1.groupby(df_slice1['date'].dt.year)['total_nonfarm'].mean().reset_index()
    avg_annual_employment.columns = ['Year', 'Average Employment']
    # Calculate the overall average for the selected interval
    overall_avg = avg_annual_employment['Average Employment'].mean() if not avg_annual_employment.empty else 0
    st.metric(f"Average Jobs Created ({start_year}-{end_year})", f"{overall_avg:,.0f}")
    fig1 = px.bar(
        avg_annual_employment, x='Year', y='Average Employment',
        title=f"Average Total Payroll Employment by Year ({start_year}-{end_year})",
        labels={'Year': 'Year', 'Average Employment': 'Average Employment (in thousands)'}
    )
    st.plotly_chart(fig1)

    # Slicing 2: Monthly employment comparison for Mar-Dec 2020 vs. 2019
    st.subheader("Monthly Employment Comparison (Mar-Dec 2020 vs. 2019)")
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['month_name'] = df['date'].dt.strftime('%b')
    df_slice2 = df[((df['year'] == 2019) | (df['year'] == 2020)) & (df['month'].between(3, 12))]
    # Pivot the data so each year is a column, index is month
    df_pivot = df_slice2.pivot_table(index=['month', 'month_name'], columns='year', values='total_nonfarm').reset_index()
    # Sort by month to ensure correct order
    df_pivot = df_pivot.sort_values('month')
    # Melt for plotly express
    df_melt = df_pivot.melt(id_vars=['month', 'month_name'], value_vars=[2019, 2020], var_name='Year', value_name='Total Employment')
    # Set color map
    color_map = {2019: 'red', 2020: 'blue'}
    fig2 = px.bar(
        df_melt,
        x='month_name',
        y='Total Employment',
        color='Year',
        barmode='group',
        category_orders={'month_name': ['Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']},
        color_discrete_map={str(k): v for k, v in color_map.items()},
        title="Monthly Employment: March-December 2020 vs. 2019",
        labels={'Total Employment': 'Total Employment (in thousands)', 'month_name': 'Month'}
    )
    fig2.update_traces(marker_line_width=0)
    st.plotly_chart(fig2)

def create_dicing_charts(df):
    st.header("Dicing Analysis")

    st.subheader("Months with > 2% Month-over-Month Employment Drop")
    df['mom_growth'] = df['total_nonfarm'].pct_change() * 100
    df['month_year'] = df['date'].dt.strftime('%b-%Y')
    
    significant_drops = df[df['mom_growth'] < -2].copy()
    if not significant_drops.empty:
        st.write("Months with a greater than 2% month-over-month employment drop:")
        st.dataframe(significant_drops[['month_year', 'mom_growth']].round(2).rename(columns={'mom_growth': 'MoM Growth (%)'}))
        
        recovery_data = []
        for index, row in significant_drops.iterrows():
            drop_date = row['date']
            drop_employment = row['total_nonfarm']
            
            pre_drop_data = df[df['date'] < drop_date]
            if not pre_drop_data.empty:
                prior_peak_employment = pre_drop_data['total_nonfarm'].max()
                
                post_drop_data = df[df['date'] > drop_date]
                recovery_month = post_drop_data[post_drop_data['total_nonfarm'] >= prior_peak_employment].first_valid_index()
                
                if recovery_month:
                    months_to_recover = (df.loc[recovery_month]['date'].year - drop_date.year) * 12 + (df.loc[recovery_month]['date'].month - drop_date.month)
                    recovery_data.append({
                        'Drop Month': row['month_year'],
                        'Prior Peak Date': df.loc[pre_drop_data['total_nonfarm'].idxmax()]['date'].strftime('%b-%Y'),
                        'Months to Recover': months_to_recover
                    })
                else:
                    recovery_data.append({'Drop Month': row['month_year'], 'Prior Peak Date': 'N/A', 'Months to Recover': 'Not recovered yet'})
        
        if recovery_data:
            st.write("Time taken to recover to the prior peak:")
            st.dataframe(pd.DataFrame(recovery_data))
    else:
        st.info("No months found with a MoM drop > 2%.")

    st.subheader("Highest Q4 Payroll Growth by Month")
    df_all = df.copy()
    df_all['year'] = df_all['date'].dt.year
    df_all['month'] = df_all['date'].dt.strftime('%b')
    df_all['pct_change_mom'] = df_all['total_nonfarm'].pct_change() * 100

    df_q4 = df_all[df_all['date'].dt.month.isin([10, 11, 12])].copy()

    idx = df_q4.groupby('year')['pct_change_mom'].idxmax()
    df_q4_highest = df_q4.loc[idx]

    color_map = {'Oct': 'red', 'Nov': 'blue', 'Dec': 'green'}

    fig3 = px.bar(df_q4_highest, x='year', y='pct_change_mom', color='month',
                  labels={'year': 'Year', 'pct_change_mom': 'MoM % Change', 'month': 'Month'},
                  barmode='group',
                  color_discrete_map=color_map)

    fig3.update_traces(width=0.8)

    st.plotly_chart(fig3, use_container_width=True)

def create_roll_up_charts(df):
    st.header("Roll-up Analysis")

    st.subheader("Quarter-over-Quarter and Year-over-Year Growth Rates")
    
    df_quarterly = df.set_index('date').resample('QS').mean()
    df_quarterly['qoq_growth'] = df_quarterly['total_nonfarm'].pct_change() * 100
    fig_qoq = px.line(df_quarterly.reset_index(), x='date', y='qoq_growth', 
                      title="Quarter-over-Quarter Employment Growth Rate",
                      labels={'qoq_growth': 'QoQ Growth (%)', 'date': 'Date'})
    st.plotly_chart(fig_qoq)

    df_yearly = df.set_index('date').resample('A').mean()
    df_yearly['yoy_growth'] = df_yearly['total_nonfarm'].pct_change() * 100
    fig_yoy = px.line(df_yearly.reset_index(), x='date', y='yoy_growth', 
                      title="Year-over-Year Employment Growth Rate",
                      labels={'yoy_growth': 'YoY Growth (%)', 'date': 'Date'})
    st.plotly_chart(fig_yoy)
    
    st.subheader("Average Employment in the 2010s vs. the 2000s")
    df['year'] = df['date'].dt.year
    decade_2000s = df[(df['year'] >= 2000) & (df['year'] <= 2009)]
    decade_2010s = df[(df['year'] >= 2010) & (df['year'] <= 2019)]
    
    avg_2000s = decade_2000s['total_nonfarm'].mean()
    avg_2010s = decade_2010s['total_nonfarm'].mean()
    
    comparison_df = pd.DataFrame({
        'Decade': ['2000s', '2010s'],
        'Average Employment': [avg_2000s, avg_2010s]
    })
    
    fig_decades = px.bar(comparison_df, x='Decade', y='Average Employment', 
                         title="Average Employment: 2000s vs. 2010s",
                         labels={'Average Employment': 'Average Employment (in thousands)'})
    st.plotly_chart(fig_decades)

def create_drill_down_charts(df):
    st.header("Drill-down Analysis")
    
    st.subheader("Breakdown of Highest Annual Employment Gain")
    df_annual = df.groupby(df['date'].dt.year)['total_nonfarm'].sum().reset_index()
    df_annual['annual_gain'] = df_annual['total_nonfarm'].diff()
    df_annual.columns = ['year', 'total_employment', 'annual_gain']
    highest_gain_year = df_annual.loc[df_annual['annual_gain'].idxmax()]['year']
    
    st.write(f"The year with the highest annual employment gain was **{int(highest_gain_year)}**.")
    
    highest_gain_df = df[df['date'].dt.year == highest_gain_year].copy()
    highest_gain_df['month'] = highest_gain_df['date'].dt.strftime('%b')
    fig_drill = px.bar(highest_gain_df, x='month', y='total_nonfarm',
                       title=f"Monthly Employment Contributions in {int(highest_gain_year)}",
                       labels={'total_nonfarm': 'Total Employment (in thousands)', 'month': 'Month'},
                       color='total_nonfarm')
    st.plotly_chart(fig_drill)
    
    st.subheader("Sharpest Monthly Employment Drop")
    df['mom_drop'] = df['total_nonfarm'].diff()
    sharpest_drop_month = df.loc[df['mom_drop'].idxmin()]
    
    st.write(f"The sharpest drop in employment occurred in **{sharpest_drop_month['date'].strftime('%B %Y')}**.")
    st.write(f"The total payroll employment decreased by approximately **{sharpest_drop_month['mom_drop']:.2f} thousand** that month.")
    
    st.info("The available data is monthly. A weekly breakdown of this event is not possible with this dataset.")

# --- 4. Main App Structure ---
def main():
    add_custom_css()
    st.title("U.S. Non-Farm Payrolls OLAP Analysis")

    st.sidebar.title("Navigation")
    menu_selection = st.sidebar.radio(
        "Select an analysis type:",
        ["Slicing", "Dicing", "Roll-up", "Drill-Down"]
    )

    data = load_data()

    if data is not None:
        if menu_selection == "Slicing":
            create_slicing_charts(data.copy())
        elif menu_selection == "Dicing":
            create_dicing_charts(data.copy())
        elif menu_selection == "Roll-up":
            create_roll_up_charts(data.copy())
        elif menu_selection == "Drill-Down":
            create_drill_down_charts(data.copy())

if __name__ == "__main__":
    main()
