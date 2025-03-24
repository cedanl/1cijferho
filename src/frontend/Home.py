import streamlit as st

# Main header and subtitle
st.title("🚀 1cijferho")
st.write("Transform complex DUO datasets into actionable insights in minutes, not months. ✨")

# Features
st.caption("📊 Instant visualization • ⏱️ Time-saving automation • 🔍 Intelligent error reduction • 👥 Enhanced accessibility • 🔄 Batch processing")

# Application Overview section - consolidated content
st.subheader("👁️ Overview")
st.write("""
Our application decodes and delimits all 1CHO files, enabling researchers to access educational 
data without technical expertise. We also provide pre-made visualizations and data sets 
based on 1CHO for immediate insights.
""")

st.subheader("📢 Get Involved")
st.write("We're constantly improving based on your feedback! Share your ideas by emailing us at a.sewnandan@hhs.nl or t.iwan@vu.nl, or submit a feature request:")

# Adding an inline button for GitHub issues
feature_request = st.button("Submit Feature Request", help="Opens our GitHub issues page")
if feature_request:
    st.markdown("[Feature Request](https://github.com/cedanl/1cijferho/issues)", unsafe_allow_html=True)
    # Note: In Streamlit, the button can't directly open a URL, so we display the link after clicking

# Divider before Demo section
st.divider()

# -----------------------------------------------------------------------------
# Demo Section
# -----------------------------------------------------------------------------

# Demo section
st.header("✨ Try Now (DEMO)")
uploaded_file = st.file_uploader("Upload your DUO data file", type=["csv", "xlsx", "txt"])

# Footer section
st.write("© 2025 CEDA | Bridging institutions, sharing solutions, advancing education.")