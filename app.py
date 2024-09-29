import streamlit as st
from pipeline_improved import pipeline

st.title("File Processer")

if st.button('Process'):
    with st.spinner('Processing...'):
        logs = pipeline()
        # Shows logs on Streamlit
        for log in logs:
            st.write(log)