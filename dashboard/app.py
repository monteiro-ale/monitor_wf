import streamlit as st
import psycopg2

@st.cache(allow_output_mutation=True)
def get_database_connection():
    conn = psycopg2.connect(
        host="localhost",
				port="5436",
        database="airflow-db",
        user="airflow",
        password="airflow"
    )
    return conn

def fetch_data():
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM turbines;")
    data = cursor.fetchall()
    conn.close()
    return data

# Início
def main():
    st.title('Dashboard com Streamlit e PostgreSQL')

  
    data = fetch_data()


    st.write("Dados da Tabela:")
    st.write(data)

if __name__ == "__main__":
    main()

