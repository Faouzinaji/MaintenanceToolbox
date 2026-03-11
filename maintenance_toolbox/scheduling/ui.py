import streamlit as st


def render_scheduling_module(session):

    st.title("Scheduling")

    tab1, tab2 = st.tabs(["Mes plannings", "Créer un planning"])

    with tab1:

        st.info("Liste des plannings (placeholder)")

    with tab2:

        st.info("Wizard planning à intégrer avec ton scheduler")
