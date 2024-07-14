import streamlit as st
from dashboard import main as dashboard_main
from resolve_alerts import main as resolve_alerts_main
st.set_page_config(page_title="Dashboard de Monitoramento", page_icon=":bar_chart:")
def main():
    st.sidebar.title("Navigation")
    pages = {
        "Dashboard": dashboard_main,
        "Resolve Alerts": resolve_alerts_main
    }
    selection = st.sidebar.radio("Go to", list(pages.keys()))
    page = pages[selection]
    page()

if __name__ == "__main__":
    main()
