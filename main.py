import streamlit as st
from streamlit_folium import st_folium
from pymongo import MongoClient
import geopandas as gpd
import config
import json

# Add mongodb utils 
from scripts import db_utils as dbu

# Set parameters for client connection to mongodb
client = MongoClient(f"{dbu.mongo_uri}")
print("Connection Successul")
db = client['wildfire_db_2020_2023']
collection = db['wildFire_Collection_clean_Address']

def get_color(cfb_value):
    """Return color based on CFB value."""
    if cfb_value < 1:
        return 'green'
    elif 1 <= cfb_value < 3:
        return 'yellow'
    elif 3 <= cfb_value < 5:
        return 'orange'
    else:
        return 'red'

# Streamlit app
st.title('Geospatial Visualization')

selected_value = st.selectbox('Select Data', collection.distinct("rep_date"))

if selected_value:
    # Query MongoDB based on the selected value
    results = collection.find({"rep_date": selected_value})
    
    # Convert MongoDB cursor to list of dictionaries
    data = list(results)
    
    if not data:
        st.write("No data found for the selected value.")
    else:
        # Create a map object
        m = folium.Map(location=[45.5236, -122.6750], zoom_start=13)
        
        # Add markers for each data point
        for item in data:
            folium.CircleMarker(
                location=[item['lat'], item['lon']],
                radius=5,
                popup=f"CFB: {item['cfb']}",
                color=get_color(item['cfb']),
                fill=True,
                fill_color=get_color(item['cfb'])
            ).add_to(m)
        
        # Add a legend to the map
        legend_html = '''
        <div style="position: fixed; 
        bottom: 50px; left: 50px; width: 150px; height: 120px; 
        border:2px solid grey; z-index:9999; font-size:14px;
        background-color:white;
        ">&nbsp; CFB Value Legend <br>
        &nbsp; <i class="fa fa-circle" style="color:green"></i> < 1 <br>
        &nbsp; <i class="fa fa-circle" style="color:yellow"></i> 1 - 2.9<br>
        &nbsp; <i class="fa fa-circle" style="color:orange"></i> 3 - 4.9<br>
        &nbsp; <i class="fa fa-circle" style="color:red"></i> >= 5
        </div>
        '''
        m.get_root().html.add_child(st_folium.Element(legend_html))
        