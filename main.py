# %% Imports
import polars as pl
from polars import col as c
import plotly.express as px
import json
import streamlit as st


# %% Load the data (gathered and munged in the notebook)
total_pop_5y = pl.read_parquet('IdahoPopulation_2020-2022.parquet')

# %% Read the shapefiles
with open('zip_codes.geojson') as f:
    zip_geojson = json.load(f)

# %%
counties = {'16001': "Ada", '16045': "Gem", '16015': "Boise", '16027': 'Canyon'}
clean = (total_pop_5y
    # We just want the tracts
    .filter(c('GEO_ID').str.len_bytes() == 11+9)
    .select(
        'year',
        # Remove the useless part of the GEOID
        GEOID=c('GEO_ID').str.slice(9),
        # For matching the county names
        county=c('GEO_ID').str.slice(9, 5).replace(counties),
        pop='B01003_E001',
        err=c('B01003_M001').replace({-555555555: 1}),
    )
    # Get just the counties we want
    .filter(c('county').is_in(counties.values()))
)

# %%
# Calculate the growth
growth = (clean
    .group_by('GEOID')
    # 2022 comes last, because of how I stacked the dataframes
    .agg(growth=pl.last('pop') - pl.first('pop'))
)
clean = clean.join(growth, on='GEOID', how='left')

# %%
# Add the zip column
tract2zip = pl.read_csv('tract2zip.csv').cast(pl.String)
clean = clean.join(tract2zip, on='GEOID', how='left')

# %%
# We calculated the growth, then joined it back, so all the years should have the same growth data
fig = px.choropleth(clean.filter(c('year') == 2022),
    scope='usa',
    color='growth',
    featureidkey='properties.ZCTA5CE20',
    locations='zip',
    hover_data=('zip', 'growth', 'county', 'pop'),
    geojson=zip_geojson,
    title=f'Population Growth in West Idaho Zip Codes from 2020 - 2022',
    color_continuous_scale=px.colors.sequential.PuBu,
    # color_continuous_scale=px.colors.sequential.Blues,
)
fig.update_geos(
    fitbounds="locations",
    visible=False,
    showsubunits=True,
    subunitcolor='black',
)
fig.update_layout(
    autosize=False,
    width=800,
    height=600,
)
fig

# %%
