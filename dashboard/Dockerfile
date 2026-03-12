FROM python:3.10-slim
WORKDIR /app
RUN pip install streamlit pandas plotly watchdog
COPY app.py .
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
