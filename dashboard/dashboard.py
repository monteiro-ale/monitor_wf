import psycopg2
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import re

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
            SELECT turbine_id, temperature, hydraulicpressure, powerfactor, timestamp
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

def fetch_maintenance_data():
    conn = get_database_connection()
    if conn is None:
        return None
    
    try:
        query = '''
            SELECT turbine_id, COUNT(*) as maintenance_count
            FROM maintenance
            GROUP BY turbine_id
            ORDER BY turbine_id
        '''
        df = pd.read_sql(query, conn)
        return df
    except psycopg2.Error as e:
        st.error(f"Error fetching maintenance data: {e}")
        return None
    finally:
        if conn is not None:
            conn.close()

def fetch_maintenance_by_type():
    conn = get_database_connection()
    if conn is None:
        return None

    try:
        query = '''
            SELECT maintenance_type, COUNT(*) as count
            FROM maintenance
            GROUP BY maintenance_type
            ORDER BY maintenance_type
        '''
        df = pd.read_sql(query, conn)
        return df
    except psycopg2.Error as e:
        st.error(f"Error fetching maintenance data: {e}")
        return None
    finally:
        if conn is not None:
            conn.close()

def fetch_turbine_locations():
    conn = get_database_connection()
    if conn is None:
        return None

    try:
        query = '''
            SELECT turbine_id, location
            FROM turbines
        '''
        df = pd.read_sql(query, conn)
        df[['latitude', 'longitude']] = df['location'].str.split(',', expand=True)
        df['latitude'] = df['latitude'].apply(convert_coordinates)
        df['longitude'] = df['longitude'].apply(convert_coordinates)
        df['turbine_display'] = df['turbine_id'].apply(lambda x: f'Turbina {x}')
        return df
    except psycopg2.Error as e:
        st.error(f"Error fetching turbine locations: {e}")
        return None
    finally:
        if conn is not None:
            conn.close()

def fetch_capacity_by_manufacturer():
    conn = get_database_connection()
    if conn is None:
        return None

    try:
        query = '''
            SELECT manufacturer, SUM(capacity) as total_capacity
            FROM turbines
            GROUP BY manufacturer
            ORDER BY manufacturer
        '''
        df = pd.read_sql(query, conn)
        return df
    except psycopg2.Error as e:
        st.error(f"Error fetching capacity data: {e}")
        return None
    finally:
        if conn is not None:
            conn.close()



def convert_coordinates(coord):
    """Convert coordinates from '37.7749° N' format to float"""
    coord = re.sub(r'[^\d.-]', '', coord)  # Remove all non-numeric, non-dot, non-hyphen characters
    if 'S' in coord or 'W' in coord:
        return -float(coord)
    return float(coord)

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

def plot_alert_distribution(data):
    fig = px.pie(data, names='alert_type', values='alert_count', title='Distribuição de Alertas por Tipo')
    st.plotly_chart(fig)

def plot_stacked_bar(data):
    alert_colors = {
        'High Temperature': '#FFABAB',
        'Low Hydraulic Pressure': '#0068C9',
        'Low Energy Efficiency': '#83C9FF', 
    }

    data['turbine_display'] = data['turbine_id'].apply(lambda x: f'Turbina {x}')
    
    # Gráfico de barras
    fig = px.bar(
        data,
        x='turbine_display',
        y='alert_count',
        color='alert_type',
        title='Tipos de Alertas por Turbina',
        labels={'alert_count': 'Número de Alertas'},
        color_discrete_map=alert_colors  # Aplicar mapeamento de cores
    )
    
    st.plotly_chart(fig)
    
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

def plot_pressure_variation(data):
    fig = px.line(data, x='timestamp', y='hydraulicpressure', color='turbine_id',
                  title='Variação da Pressão Hidráulica ao Longo do Tempo',
                  labels={'timestamp': 'Período de Tempo', 'hydraulicpressure': 'Pressão Hidr.'})

    fig.add_shape(
        type="line",
        x0=data['timestamp'].min(),
        y0=72,
        x1=data['timestamp'].max(),
        y1=72,
        line=dict(color="Red", width=2, dash="dash"),
        name="Pressão Hidráulica"
    )

    fig.add_annotation(
        x=data['timestamp'].min(),
        y=72,
        xref="x",
        yref="y",
        text="Pressão Baixa (72 bar)",
        showarrow=False,
        font=dict(color="Red"),
        align="left",
        xshift=-60,  
        yshift=10
    )

    st.plotly_chart(fig)

def plot_powerfactor_variation(data):
    fig = px.line(data, x='timestamp', y='powerfactor', color='turbine_id',
                  title='Eficiência Energética ao Longo do Tempo',
                  labels={'timestamp': 'Período de Tempo', 'powerfactor': 'Fator de Potência'})

    fig.add_shape(
        type="line",
        x0=data['timestamp'].min(),
        y0=0.8,
        x1=data['timestamp'].max(),
        y1=0.8,
        line=dict(color="Red", width=2, dash="dash"),
        name="Fator de Potência"
    )

    fig.add_annotation(
        x=data['timestamp'].min(),
        y=0.8,
        xref="x",
        yref="y",
        text="Baixa Potência (0.8 kW)",
        showarrow=False,
        font=dict(color="Red"),
        align="left",
        xshift=-60,  
        yshift=10
    )

    st.plotly_chart(fig)

def plot_maintenance_per_turbine(data):
    all_turbine_ids = range(1, 11)  # Lista de todos os IDs de turbinas esperados
    
    # Reindexar os dados para garantir que todas as turbinas estejam incluídas
    data = data.set_index('turbine_id').reindex(all_turbine_ids, fill_value=0).reset_index()
    data['turbine_display'] = data['turbine_id'].apply(lambda x: f'Turbina {x}')
    
    fig = px.bar(data, x='turbine_display', y='maintenance_count', 
                 title='Quantidade de Manutenções por Turbina', 
                 labels={'turbine_display': 'ID da Turbina', 'maintenance_count': 'Quantidade de Manutenções'})
    st.plotly_chart(fig)

def plot_maintenance_by_type(data):
    fig = px.bar(data, x='maintenance_type', y='count', 
                 title='Quantidade de Manutenções por Tipo', 
                 labels={'maintenance_type': 'Tipo de Manutenção', 'count': 'Quantidade'})
    st.plotly_chart(fig)

def plot_turbine_locations(data):
    fig = go.Figure(data=go.Scattergeo(
        lon=data['longitude'],
        lat=data['latitude'],
        text=data['turbine_display'],
        mode='markers',
        marker=dict(
            size=8,
            color='blue',
            symbol='circle'
        )
    ))

    fig.update_layout(
        title='Localização das Turbinas',
        geo=dict(
            scope='world',
            projection_type='equirectangular',
        )
    )

    st.plotly_chart(fig)

def plot_capacity_by_manufacturer(data):
    fig = px.bar(data, x='manufacturer', y='total_capacity', 
                 title='Capacidade Total por Fabricante', 
                 labels={'manufacturer': 'Fabricante', 'total_capacity': 'Capacidade Total (MW)'})
    st.plotly_chart(fig)

def main():
    st.title("Wind Farm Monitor Dashboard")
    
    data = fetch_data()
    if data.empty:
        st.write("No data available")
        return
    
    sensor_data = fetch_sensor_data()
    if sensor_data.empty:
        st.write("No sensor data available")
        return
    
    maintenance_data = fetch_maintenance_data()
    if maintenance_data.empty:
        st.write("Não há dados de manutenção disponíveis.")
        return
    
    maintenance_data_type = fetch_maintenance_by_type()
    if maintenance_data.empty:
        st.write("Não há dados de manutenção disponíveis.")
        return
    
    turbine_locations = fetch_turbine_locations()
    if turbine_locations.empty:
        st.write("Não há dados de localização das turbinas disponíveis.")
        return
    
    capacity_data = fetch_capacity_by_manufacturer()
    if capacity_data.empty:
        st.write("Não há dados de capacidade disponíveis.")
        return

    st.sidebar.header("Filter")
    alert_types = data['alert_type'].unique()
    selected_alert_type = st.sidebar.selectbox("Select Alert Type", alert_types)

    st.header("Alertas por Turbina")
    plot_alerts_per_turbine(data, selected_alert_type)

    st.header("Distribuição de alertas")
    plot_alert_distribution(data)

    st.header("Todos os Alertas")
    plot_stacked_bar(data)
    
    st.header("Variação de Temperatura ao longo do tempo")
    plot_temperature_variation(sensor_data)
    
    st.header("Variação de Pressão ao longo do tempo")
    plot_pressure_variation(sensor_data)
    
    st.header("Variação de Power Factor ao longo do tempo")
    plot_powerfactor_variation(sensor_data)
    
    st.header("Manutenções por Turbina")
    plot_maintenance_per_turbine(maintenance_data)
    
    st.header("Quantidade de Manutenções por Tipo")
    plot_maintenance_by_type(maintenance_data_type)
    
    st.header("Localização das Turbinas")
    plot_turbine_locations(turbine_locations)
    
    st.header("Capacidade total por fabricante")
    plot_capacity_by_manufacturer(capacity_data)

if __name__ == "__main__":
    main()


#python -m streamlit run dashboard/app.py