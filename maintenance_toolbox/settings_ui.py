import streamlit as st


def render_settings(session, user):
    st.title("Settings")

    tab1, tab2 = st.tabs(["Langue", "Mot de passe"])

    with tab1:
        with st.form("language_form"):
            language = st.selectbox(
                "Langue",
                options=["fr", "en", "nl"],
                index=["fr", "en", "nl"].index(user.language) if user.language in ["fr", "en", "nl"] else 0
            )
            submit_lang = st.form_submit_button("Enregistrer la langue", use_container_width=True)

        if submit_lang:
            try:
                user.language = language
                session.commit()
                st.success("Langue mise à jour.")
                st.rerun()
            except Exception as e:
                session.rollback()
                st.error(f"Erreur lors de la mise à jour de la langue : {e}")

    with tab2:
        with st.form("password_form"):
            current_password = st.text_input("Mot de passe actuel", type="password")
            new_password = st.text_input("Nouveau mot de passe", type="password")
            confirm_password = st.text_input("Confirmer le nouveau mot de passe", type="password")
            submit_pwd = st.form_submit_button("Changer le mot de passe", use_container_width=True)

        if submit_pwd:
            if not user.check_password(current_password):
                st.error("Mot de passe actuel incorrect.")
            elif len(new_password) < 6:
                st.error("Le nouveau mot de passe doit contenir au moins 6 caractères.")
            elif new_password != confirm_password:
                st.error("La confirmation du mot de passe ne correspond pas.")
            else:
                try:
                    user.set_password(new_password)
                    user.first_login = False
                    session.commit()
                    st.success("Mot de passe mis à jour.")
                except Exception as e:
                    session.rollback()
                    st.error(f"Erreur lors de la mise à jour du mot de passe : {e}")
