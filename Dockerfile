FROM bitnami/spark:latest

# Instalacja bibliotek Pythona
RUN pip install requests kafka-python findspark matplotlib seaborn plotly pandas numpy