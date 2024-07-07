import psycopg2
import streamlit as st
import pandas as pd
import plotly.express as px

def get_database_connection():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5436",
            database="airflow-db",
            user="airflow",
            password="airflow"
        )
        return conn
    except psycopg2.Error as e:
        st.error(f"Error connecting to the database: {e}")
        return None

@st.cache(allow_output_mutation=True)
def fetch_data():
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT turbine_id, COUNT(*) as alert_count, alert_type
            FROM alerts
            GROUP BY turbine_id, alert_type
            ORDER BY turbine_id
        ''')
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    except psycopg2.Error as e:
        st.error(f"Error fetching data from the database: {e}")
        return pd.DataFrame()
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()

def main():
    data = fetch_data()

    if data.empty:
        st.write("No data available")
        return

    alert_types = data['alert_type'].unique()
    selected_alert_type = st.selectbox("Select Alert Type", alert_types)

    filtered_data = data[data['alert_type'] == selected_alert_type]

    if not filtered_data.empty:
        fig = px.bar(filtered_data, x='turbine_id', y='alert_count', title=f'Number of {selected_alert_type} Alerts per Turbine', labels={'alert_count': 'Number of Alerts'})
        st.plotly_chart(fig)
    else:
        st.write(f"No data available for the selected alert type: {selected_alert_type}")

if __name__ == "__main__":
    main()


#python -m streamlit run dashboard/app.py