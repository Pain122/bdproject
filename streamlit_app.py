import streamlit as st
import pandas as pd
import numpy as np

def rmse(predictions, targets):
    return np.sqrt(np.mean((predictions-targets)**2))

incidents = pd.read_csv("data/police.csv")
q1 = pd.read_csv("output/q1.csv")
q2 = pd.read_csv("output/q2.csv")
q3 = pd.read_csv("output/q3.csv")
q4 = pd.read_csv("output/q4.csv")
q5 = pd.read_csv("output/q5.csv")
lr_lat = pd.read_csv("output/lr_lat_predictions.csv")
lr_lon = pd.read_csv("output/lr_lon_predictions.csv")
gbt_lat = pd.read_csv("output/gbt_lat_predictions.csv")
gbt_lon = pd.read_csv("output/gbt_lon_predictions.csv")
lr_lat_rmse = rmse(lr_lat['prediction'], lr_lat['latitude'])
lr_lon_rmse = rmse(lr_lon['prediction'], lr_lon['longitude'])
gbt_lat_rmse = rmse(gbt_lat['prediction'], gbt_lat['latitude'])
gbt_lon_rmse = rmse(gbt_lon['prediction'], gbt_lon['longitude'])

st.markdown('---')
st.title("Big Data Project **2023**")
st.markdown("""<style>body {
    background-color: #eee;
}

.fullScreenFrame > div {
    display: flex;
    justify-content: center;
}
</style>""", unsafe_allow_html=True)

#st.markdown("<p style='text-align: center; color: grey;'>Employees and Departments</p>", unsafe_allow_html=True)

st.markdown('---')
st.header('Descriptive Data Analysis')
st.subheader('Data Characteristics')
incidents_dda = pd.DataFrame(data = [["Incidents", incidents.shape[0], incidents.shape[1]]],columns = ["Tables", "Features", "Instances"])
st.write(incidents_dda)
st.markdown('`incidents` table')
st.write(incidents.describe())

st.subheader('Some samples from the data')
st.markdown('`emps` table')
st.write(incidents.head(5))
st.markdown('---')
st.header("Exploratory Data Analysis")
st.subheader('Q1')
st.text('Top 10 most occurring incidents')
st.bar_chart(q1.set_index('inc_category'))

st.subheader('Q2')
st.text('The number of incidents per year')
st.table(q2)
st.line_chart(q2.set_index('inc_year'), width=400)

st.subheader('Q3')
st.text('The average amount of incidents for each weekday')
st.line_chart(q3.set_index('inc_weekday'))

st.subheader('Q4')
st.text('Average response time in minutes by police districts')
st.bar_chart(q4.set_index('police_district'), width=400)

st.subheader('Q5')
st.text('Average number of incidents per hour of day')
st.line_chart(q5.set_index('hour'), width=400)

st.markdown('---')
st.header('Predictive Data Analytics')
st.markdown('I built a total of 4 models. Two for longitude, and two for latitude')
st.subheader('ML Model')
st.markdown('1. Linear Regression Models')
st.markdown('Settings of the model')
st.table(pd.DataFrame([['elasticNetParam', 0], ['elasticNetParam', 0.5], ['regParam', 0], ['regParam', 0.01]], columns = ['setting', 'value']))

st.markdown('2. GBT Regressor')
st.markdown('Settings of the model')
st.table(pd.DataFrame([['maxDepth', 2], ['maxDepth', 5], ['lossType','squared'], ['lossType','absolute']], columns = ['setting', 'value']))

st.subheader('Results')
st.text('RMSE Scores')
st.markdown('<center>Results table</center>', unsafe_allow_html = True)
st.table(pd.DataFrame([['lr_lat', lr_lat_rmse], ['lr_lon', lr_lon_rmse], ['gbt_lat', gbt_lat_rmse], ['gbt_lon',gbt_lon_rmse]], columns = ['Model', 'Score']))
