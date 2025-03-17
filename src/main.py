import streamlit as st
from frontend.Modules.dashboard import render_dashboard
from backend.file_handler import file_handler

# Set page config
st.set_page_config(page_title="Student Intake Analysis", page_icon="📊", layout="wide")

# Handle file upload in sidebar
with st.sidebar:
    st.title("Data Upload")
    file_handler()

# Sidebar feedback section
with st.sidebar:
    st.markdown("---")
    st.subheader("📝 Feedback")

    # Star feedback
    st.write("How would you rate this dashboard?")
    sentiment_mapping = ["Poor", "Fair", "Good", "Very Good", "Excellent"]
    selected = st.feedback("stars", key="dashboard_feedback")
    if selected is not None:
        st.success(
            f"Thank you! You rated the dashboard as '{sentiment_mapping[selected]}'"
        )

    # GitHub issues link
    st.markdown("""
    #### 🐛 Report Issues
    Found a bug or have a suggestion? Let us know!
    """)

    st.link_button(
        "Open an Issue on GitHub", "https://github.com/cedanl/data-1cijferho-py/issues"
    )

    st.markdown("---")


# Render dashboard
render_dashboard()
