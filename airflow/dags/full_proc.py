from datetime import datetime
import pandas as pd
import re
from bson import ObjectId
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

CONN_ID_PG = 'postgres_default'
CONN_ID_MONGO = 'mongo_default'

mongo_hook = MongoHook(conn_id="mongo_default")
pg_hook = PostgresHook(postgres_conn_id="postgres_default")

drop_create_users_sql = """
drop table if exists users;

create table users (
    _id text primary key,
    name text,
    email text,
    password text
);
"""

drop_create_movies_sql = """
drop table if exists movies cascade;

create table movies (
    _id text primary key,
    title text,
    year varchar(10),
    released date,
    runtime float,
    genres text[],
    countries text[],
    languages text[],
    imdb_rating float,
    imdb_votes int,
    tomatoes_viewer_rating float,
    tomatoes_viewer_numReviews int,
    num_mflix_comments int
);

"""

drop_create_theaters_sql = """
drop table if exists theaters; 

create table theaters (
    _id text primary key,
    theaterId int,
    location_address_street1 text,
    location_address_city text,
    location_address_state text,
    location_address_zipcode text,
    location_geo_type text,
    location_geo_coordinates float[],
    location_address_street2 text
);
"""

drop_create_comments_sql = """
drop table if exists comments;

create table comments (
    _id text primary key,
    name text,
    email text,
    movie_id text,
    text text, 
    date timestamp
); 

"""

drop_create_view_best_m= """ 
create or replace view best_movies as

    select
        m.title,
        m.year,
        m.genres,
        m.runtime,
        m.imdb_rating,
        m.imdb_votes,
        m.tomatoes_viewer_rating,
        m.tomatoes_viewer_numReviews,
        count(c._id) as comments_count
    from movies m
    left join comments c
        on m._id = c.movie_id
    where m.imdb_rating is not NULL
    group by
        m.title,
        m.year,
        m.genres,
        m.runtime,
        m.imdb_rating,
        m.imdb_votes,
        m.tomatoes_viewer_rating,
        m.tomatoes_viewer_numReviews

    order by year desc, imdb_rating desc
    limit 15;
"""

drop_create_view_best_commentators = """
create or replace view best_commentators as 

    select
        c.name,
        c.email,
        array_agg(distinct m.title) as movies_commented,
        count(c._id) as total_comments,
        max(c.date) as last_comment

    from comments c inner join movies m
        on c.movie_id = m._id

    group by c.name, c.email
    order by total_comments desc
    limit 15;
"""

def _load_df_pg(df:pd.DataFrame, table_name:str):
    conn = pg_hook.get_conn()
    cols = ','.join(list(df.columns))
    
    df = df.copy()
    df = df.astype(object).where(pd.notnull(df), None)
    
    values = [tuple(x) for x in df.to_numpy()]
    
    sql = f""" 
    insert into {table_name} ({cols}) 
    values %s
    """
    
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    
    conn.commit()
    conn.close()
    
def proc_users():
    collection = mongo_hook.get_collection('users')
    
    docs = list(collection.find())
    df = pd.DataFrame(docs)
    df = df.drop('preferences', axis = 1)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, ObjectId) else x)
        
    _load_df_pg(df, 'users')
    
def proc_comments():
    collection = mongo_hook.get_collection('comments')
    
    docs = list(collection.find())
    df = pd.DataFrame(docs)

    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, ObjectId) else x)
    
    _load_df_pg(df, 'comments')

def proc_movies():
    collection = mongo_hook.get_collection('movies')
    
    docs = list(collection.find(
        {}, 
    {
        '_id': 1,
        'title': 1,
        'year': 1,
        'released': 1,
        'runtime': 1,
        'genres': 1,
        'countries': 1,
        'languages': 1,
        'imdb.rating': 1,
        'imdb.votes': 1,
        'tomatoes.viewer.rating': 1,
        'tomatoes.viewer.numReviews': 1,
        'num_mflix_comments': 1
    }
    ))
    df = pd.json_normalize(docs, sep = '_')
    df = df[["_id", "title", "year", "released", 
                    "runtime", "genres", "countries", 
                    "languages", "imdb_rating", "imdb_votes", 
                    "tomatoes_viewer_rating", "tomatoes_viewer_numReviews", "num_mflix_comments"]]

    df["year"] = df["year"].astype(str)
    df["year"] = df["year"].apply(lambda x: x.replace('è','') if not re.search(r'\d{4}.\d{4}', x) else x.replace('è','-'))
    df['imdb_rating'] = pd.to_numeric(df['imdb_rating'])
    df['imdb_votes'] = pd.to_numeric(df['imdb_votes']).astype("Int64")
    df['tomatoes_viewer_numReviews'] = pd.to_numeric(df['tomatoes_viewer_numReviews']).astype("Int64")

    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, ObjectId) else x)
    
    _load_df_pg(df, 'movies')
    
def proc_theaters():
    collection = mongo_hook.get_collection('theaters')
    
    docs = list(collection.find())
    df =  pd.json_normalize(docs, sep = '_')
    
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, ObjectId) else x)
    
    _load_df_pg(df, 'theaters')

with DAG(
    dag_id = 'process_data',
    start_date = datetime(2025,1,1),
    schedule=None,
    catchup=False) as dag:
    
    drop_create_users = PostgresOperator(
        task_id='drop_create_users',
        postgres_conn_id=CONN_ID_PG,
        sql=drop_create_users_sql
    )
    
    drop_create_movies = PostgresOperator(
        task_id='drop_create_movies',
        postgres_conn_id=CONN_ID_PG,
        sql=drop_create_movies_sql
    )
    
    drop_create_theaters = PostgresOperator(
        task_id='drop_create_theaters',
        postgres_conn_id=CONN_ID_PG,
        sql=drop_create_theaters_sql
    )
    
    drop_create_comments = PostgresOperator(
        task_id='drop_create_comments',
        postgres_conn_id=CONN_ID_PG,
        sql=drop_create_comments_sql
    )
    
    fill_users = PythonOperator(
        task_id='fill_users',
        python_callable=proc_users
    )
    
    fill_comments = PythonOperator(
        task_id='fill_comments',
        python_callable=proc_comments
    )
    
    
    fill_movies = PythonOperator(
        task_id='fill_movies',
        python_callable=proc_movies
    )
    
    
    fill_theaters = PythonOperator(
        task_id='fill_theaters',
        python_callable=proc_theaters 
    )
    
    view_best_mov = PostgresOperator(
        task_id='view_best_mov',
        postgres_conn_id=CONN_ID_PG,
        sql=drop_create_view_best_m
    )
    
    view_best_com = PostgresOperator(
        task_id='view_best_com',
        postgres_conn_id=CONN_ID_PG,
        sql=drop_create_view_best_commentators
    )
    
    
    drop_create_users >> drop_create_movies >> drop_create_theaters >> drop_create_comments >> fill_users >>  fill_movies >> fill_comments  >> fill_theaters >> view_best_mov >> view_best_com