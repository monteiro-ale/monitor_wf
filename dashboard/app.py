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

@st.cache_resource()
def fetch_data():
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        cursor = conn.cursor()
        cursor.execute('''SELECT turbine_id, alert_type, COUNT(*) as alert_count
                          FROM alerts
                          GROUP BY turbine_id, alert_type
                          ORDER BY turbine_id;
                       ''')
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

        return df

    except psycopg2.Error as e:
        st.error(f"Error fetching data from the database: {e}")
        return pd.DataFrame()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    data = fetch_data()

    if data.empty:
        st.write("No data available")
        return

    alert_types = data['alert_type'].unique()
    selected_alert_type = st.selectbox("Select Alert Type", alert_types)

    filtered_data = data[data['alert_type'] == selected_alert_type]

    # Garantir que todos os turbine_id de 1 a 10 apareçam no gráfico
    all_turbine_ids = pd.DataFrame({'turbine_id': range(1, 11)})
    filtered_data = all_turbine_ids.merge(filtered_data, on='turbine_id', how='left').fillna(0)

    # Concatenar "Turbina" com o turbine_id
    filtered_data['turbine_id'] = 'Turbina ' + filtered_data['turbine_id'].astype(str)

    if not filtered_data.empty:
        fig = px.bar(filtered_data, x='turbine_id', y='alert_count', title=f'Number of {selected_alert_type} Alerts per Turbine', labels={'alert_count': 'Number of Alerts'})
        st.plotly_chart(fig)
    else:
        st.write("No data available for the selected alert type")

if __name__ == "__main__":
    main()


#python -m streamlit run dashboard/app.py
#python watch_streamlit.py
#C:/Users/usuario/Desktop/monitor_wf