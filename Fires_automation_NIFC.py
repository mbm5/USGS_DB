from sqlalchemy import create_engine
import geopandas as gpd
import psycopg2
from sqlalchemy import *
# from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker
from postgis.psycopg import register
import numpy as np
import pandas as pd
import random
import string

# connect to database
user = input('User:')
password = input('Password:')
dbname = input('DB Name:')

engine_string = f"postgresql://{user}:{password}@localhost:5432/{dbname}"
try:
    engine = create_engine(engine_string, pool_pre_ping=True)
    db = psycopg2.connect(dbname=dbname, password=password, user=user, host='localhost')
    register(db)
    cursor = db.cursor()
    print('Successfully connected.')
except Exception as e:
    print('Error encountered.')
    print(e)

# create fire table in empty db with gacc table
create_fire_table = "CREATE TABLE IF NOT EXISTS fire\
    (fire_id VARCHAR(15) UNIQUE NOT NULL,\
        fire_name VARCHAR(250) NOT NULL,\
        final_size DECIMAL,\
        fire_type VARCHAR(50),\
        ignition_date DATE,\
        containment_date DATE,\
        agency VARCHAR(50),\
        poi VARCHAR(50),\
        fire_year VARCHAR(4),\
        complex_name VARCHAR(75),\
        IRWINID VARCHAR(100) UNIQUE,\
        gacc_id INT NOT NULL,\
        comments_original VARCHAR (200),\
        method VARCHAR (150),\
        comments_new VARCHAR (200),\
        source VARCHAR (50),\
        PRIMARY KEY (fire_id),\
        FOREIGN KEY (gacc_id) REFERENCES gacc(gacc_id) ON UPDATE CASCADE ON DELETE CASCADE);"

create_fire_geom =  "SELECT AddGeometryColumn('public', 'fire', 'geometry', 5070, 'GEOMETRY', 2);"

try:
    cursor.execute(create_fire_table)
    db.commit()
    cursor.execute(create_fire_geom)
    db.commit()
except Exception as e:
    cursor.execute("rollback")
    print('Error')
    print(e)

# preprocess GACC shapefile
def gacc_preprocess(gacc_file):
    gacc = gpd.read_file(gacc_file)
    gacc.rename(columns={'OBJECTID':'gacc_id', 'GACCName':'gacc_name', 'GACCLocati':'gacc_location'}, inplace=True)
    gacc = gacc[['gacc_name', 'gacc_location', 'geometry', 'gacc_id']]
    gacc = gacc.to_crs('EPSG:4326')
    return gacc

# function to create unique ID
def create_id(dataset, id_len):
    characters = string.ascii_letters + string.digits 
    x = [''.join(random.choice(characters) for x in range(id_len)) for _ in range(len(dataset))]   
    return x

# preprocess fire table input
def nifc_process(fire_file, gacc_file):
    gacc = gacc_preprocess(gacc_file)

    fires = gpd.read_file(fire_file)
    fires = fires.to_crs('EPSG:5070')
    gacc = gacc.to_crs(fires.crs)

    fires.rename(columns={
    'INCIDENT':'fire_name', 
    'FIRE_YEAR':'fire_year', 
    'POO_RESP_I':'poi', 
    'MAP_METHOD':'method',
    'AGENCY':'agency',
    'FEATURE_CA':'fire_type',
    'COMMENTS':'comments_original',
    'GIS_ACRES':'final_size',
    'SOURCE':'source',
    'IRWINID':'irwinid' }, inplace=True)

    fires_unique = fires.query('GEO_ID.is_unique & fire_type == "Wildfire Final Perimeter"') # ensure unique data upload
    fg = fires_unique.sjoin(gacc, how='left') # spatial join for gacc_id
    fg['fire_size'] = fg.area # recalculate fire size
    fg['fire_id'] = create_id(fg, 15) # create random fire_id


    # subset columns
    final_perim = fg[['gacc_id', 'fire_id', 'fire_type', 'fire_name', 'fire_year', 'final_size', 'poi', 'method',  'source', 'agency', 'comments_original', 'irwinid', 'geometry']].to_crs('EPSG:5070')
   
    return(final_perim)

# push to postgres DB
def db_upload(ds):
    print('Fires data preprocess completed.')

    ds.drop_duplicates(subset=['geometry'], keep= 'first', inplace=True)
    
    sql_geom = 'select geometry from fire;'
    cursor.execute(sql_geom)
    results_geom = cursor.fetchall()
    db_geom = pd.DataFrame(results_geom)

    sql_irwinid = 'select irwinid from fire;'
    cursor.execute(sql_irwinid)
    results_irwinid = cursor.fetchall()
    db_irwinid = pd.DataFrame(results_irwinid)

    if len(db_geom) >= 1 or db_irwinid >= 1:
        results_geom = db_geom[0].astype(str).str.slice(10,22)
        fires_geom = ds['geometry'].astype(str).str.slice(10,22)

        results_irwinid = db_irwinid[0]
        fires_irwinid = ds['irwinid']

        ds = ds[~fires_geom.isin(results_geom)] 
        ds = ds[~fires_irwinid.isin(results_irwinid)] 
        print('Data checked and processed for duplicates.')     
    else:
        print('Duplicate checking has been omitted.')

    try: 
        ds.to_postgis('fire', con=engine, if_exists='append')
        print('Database upload success.')
        print('Uploaded:', ds.shape[0], 'rows.')
        
    except Exception as e:
        cursor.execute("rollback")
        print('Error:')
        print(e)


fires_filepath = "C:/Users/Raina Monaghan/Desktop/Work/USGS/data/InterAgencyFirePerimeterHistory_All_Years_View.zip"
gacc_filepath = "C:/Users/Raina Monaghan/Desktop/Work/USGS/data/National_GACC_Boundaries/National_GACC_Current.shp"

db_upload(nifc_process(fires_filepath, gacc_filepath))