# Home.py
import streamlit as st

title = "Sample Page"
icon = ":material/home:"

st.title("Welcome to the home page")

st.markdown(
    """
    Streamlit is an open-source app framework built specifically for
    Machine Learning and Data Science projects.
    **👈 Select a demo from the sidebar** to see some examples
    of what Streamlit can do!
    ### Want to learn more?
    - Check out [streamlit.io](https://streamlit.io)
    - Jump into our [documentation](https://docs.streamlit.io)
    - Ask a question in our [community
        forums](https://discuss.streamlit.io)
    ### See more complex demos
    - Use a neural net to [analyze the Udacity Self-driving Car Image
        Dataset](https://github.com/streamlit/demo-self-driving)
    - Explore a [New York City rideshare dataset](https://github.com/streamlit/demo-uber-nyc-pickups)
"""
)

# Display or edit DataFrame if available
if st.session_state.df is not None:
    st.session_state.df = st.data_editor(st.session_state.df, key="main_data_editor")
else:
    st.write("No DataFrame available. Please upload a file.")
