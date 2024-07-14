import streamlit as st
import psycopg2
import pandas as pd

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

# Função para listar os alertas
def list_alerts():
    conn = get_database_connection()
    if conn is None:
        return

    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT alert_id, turbine_id, TO_CHAR(alert_date, 'DD/MM/YYYY') as formatted_date, alert_type, resolved 
            FROM alerts
        ''')
        alerts_data = cursor.fetchall()

        if not alerts_data:
            st.write("No alerts found.")
            return

        for alert in alerts_data:
            alert_id, turbine_id, formatted_date, alert_type, resolved = alert

            if not resolved:
                button_label = f"Resolve Alert {alert_id}"
                with st.form(key=f"resolve_form_{alert_id}"):
                    st.write(f"### Maintenance Details for Alert {alert_id}")

                    st.markdown(f"**Alert ID:** {alert_id}")
                    st.markdown(f"**Turbine ID:** {turbine_id}")
                    st.markdown(f"**Timestamp:** {formatted_date}")
                    st.markdown(f"**Alert Type:** {alert_type}")
                    st.markdown(f"**Resolved:** {resolved}")
                    st.markdown("---")

                    maintenance_type = st.selectbox("Maintenance Type:", ["Scheduled", "Unscheduled"], key=f"maintenance_type_{alert_id}")
                    notes = st.text_area("Notes:", key=f"notes_{alert_id}")
                    submit_button = st.form_submit_button(label=button_label)

                    if submit_button:
                        # Atualizar o estado do alerta para resolvido na Alerts
                        cursor.execute('UPDATE alerts SET resolved = true WHERE alert_id = %s', (alert_id,))
                        conn.commit()
                        st.success(f"Alert {alert_id} resolved successfully!")

                        # Inserir dados na tabela maintenance
                        cursor.execute('''
                            INSERT INTO maintenance (turbine_id, maintenance_date, maintenance_type, notes)
                            VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
                        ''', (turbine_id, maintenance_type, notes))
                        conn.commit()
                        st.success("Maintenance details submitted successfully!")

            st.markdown("---")

    except psycopg2.Error as e:
        st.error(f"Error fetching or updating alerts: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    st.title("Alert Resolution Dashboard")
    st.header("Alerts List")
    st.markdown("\n")
    st.markdown("---")
    list_alerts()

if __name__ == "__main__":
    main()
