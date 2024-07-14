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
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def fetch_sensor_data():
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT turbine_id, temperature, timestamp
            FROM sensors
            ORDER BY timestamp;
        ''')
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    except psycopg2.Error as e:
        st.error(f"Error fetching data from the database: {e}")
        return pd.DataFrame()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def plot_alerts_per_turbine(data, selected_alert_type):
    filtered_data = data[data['alert_type'] == selected_alert_type]
       
    all_turbine_ids = range(1, 11)
    if not filtered_data.empty:
        filtered_data = filtered_data.set_index('turbine_id').reindex(all_turbine_ids, fill_value=0).reset_index()
        filtered_data['turbine_display'] = filtered_data['turbine_id'].apply(lambda x: f'Turbina {x}')
        fig = px.bar(filtered_data, x='turbine_display', y='alert_count', 
                     title=f'Number of {selected_alert_type} Alerts per Turbine', 
                     labels={'alert_count': 'Number of Alerts'})
        st.plotly_chart(fig)
    else:
        st.write("No data available for the selected alert type")

def plot_temperature_variation(data):
    fig = px.line(data, x='timestamp', y='temperature', color='turbine_id',
                  title='Variação da Temperatura ao Longo do Tempo',
                  labels={'timestamp': 'Período de Tempo', 'temperature': 'Temperatura'})

    fig.add_shape(
        type="line",
        x0=data['timestamp'].min(),
        y0=24,
        x1=data['timestamp'].max(),
        y1=24,
        line=dict(color="Red", width=2, dash="dash"),
        name="Temperatura Crítica"
    )

    fig.add_annotation(
        x=data['timestamp'].min(),
        y=24,
        xref="x",
        yref="y",
        text="Crítica (24°C)",
        showarrow=False,
        font=dict(color="Red"),
        align="left",
        xshift=-60,  
        yshift=10
    )

    st.plotly_chart(fig)


def plot_alert_distribution(data):
    fig = px.pie(data, names='alert_type', values='alert_count', title='Distribuição de Alertas por Tipo')
    st.plotly_chart(fig)

def plot_stacked_bar(data):
    data['turbine_display'] = data['turbine_id'].apply(lambda x: f'Turbina {x}')
    fig = px.bar(data, x='turbine_display', y='alert_count', color='alert_type', title='Tipos de Alertas por Turbina', labels={'alert_count': 'Número de Alertas'})
    st.plotly_chart(fig)

def main():
    st.title("Wind Turbine Monitoring Dashboard")
    
    data = fetch_data()
    if data.empty:
        st.write("No data available")
        return
    
    sensor_data = fetch_sensor_data()
    if sensor_data.empty:
        st.write("No sensor data available")
        return

    st.sidebar.header("Filter")
    alert_types = data['alert_type'].unique()
    selected_alert_type = st.sidebar.selectbox("Select Alert Type", alert_types)

    st.header("Alerts per Turbine")
    plot_alerts_per_turbine(data, selected_alert_type)

    st.header("Alert Distribution")
    plot_alert_distribution(data)

    st.header("Stacked Bar Chart of Alerts")
    plot_stacked_bar(data)
    
    st.header("Temperature Variation Over Time")
    plot_temperature_variation(sensor_data)

if __name__ == "__main__":
    main()


#python -m streamlit run dashboard/app.py