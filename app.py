import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
import requests
import numpy as np
from streamlit_extras.colored_header import colored_header
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float,Boolean,select
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime
from streamlit_tags import st_tags_sidebar
Base = declarative_base()
engine = create_engine('sqlite:///db.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


class JobInfo(Base):
    __tablename__ = "JobsInfo"

    id = Column(Integer, primary_key=True)
    company = Column(String(253), unique=False, nullable=False)
    seen = Column(Boolean, unique=False, nullable=False)
    apply = Column(Boolean, unique=False, nullable=False)
    interview = Column(Boolean, unique=False, nullable=False)
    datetime_interview = Column(DateTime)
    datetime_apply = Column(DateTime)
    datetime_seen = Column(DateTime)
    datetime = Column(DateTime)


Base.metadata.create_all(engine)

st.set_page_config(
    page_title="CC3", page_icon="⬇", layout="wide"
)
@st.cache_data(ttl=820)
def get_data(_engine):
    #engine = create_engine('sqlite:///db.db')
    df_jobs = pd.read_sql("SELECT * FROM jobs where id is not null", con=_engine)
    df_info = pd.read_sql("SELECT * FROM JobsInfo", con=_engine)
    df_desc = pd.read_sql("SELECT id, description, apply_link FROM JobDescription", con=_engine)
    df_jobs.drop_duplicates(subset='id', keep="last", inplace=True)#
    df_info.rename(columns={"company":"companyInfo"}, inplace=True)
    
    df_jobs['id'] = df_jobs['id'].astype('int')
    ndf = pd.merge(df_jobs, df_info, how="left", left_on="id", right_on="id").merge(df_desc, how="left", on="id")
    
    #ndf['seen'] = ndf['seen'].astype('bool')
    ndf.fillna(0, inplace=True)
    print(ndf['seen'])
    return ndf#df_jobs, df_info
@st.cache_data(ttl=820)
def presets(_engine):
    df = pd.read_sql("SELECT * FROM SaveFilters", con=_engine)
    return df
#presets_values = presets(engine)
#presets_values
#select_presets = st.sidebar.selectbox("presets", presets_values['name'].to_list())
notkeyword = st_tags_sidebar(
    label='# not Keywords:',
    text='Press enter to add more',
    value=['Senior', 'Lead', 'Staff'],
    suggestions=['five', 'six', 'seven', 
    'eight', 'nine', 'three', 
    'eleven', 'ten', 'four'],
    maxtags = 16
)

keyword = st_tags_sidebar(
    label='# Enter title:',
    value=['Python', 'SQL', 'Engineer'],
    text='Press enter to add more',
    maxtags = 16
)

data = get_data(engine)
#data
not_company = st.sidebar.multiselect("not company", data['company'].unique().tolist()) 
posted_date = st.sidebar.date_input("posted", format="YYYY/MM/DD")
yoe = st.sidebar.multiselect("years of experience", data['YOE'].unique().tolist())
office = st.sidebar.multiselect("office", data['office'].unique().tolist())
salary = st.sidebar.multiselect("salary", data['salary'].unique().tolist())

seen = st.sidebar.toggle("seen?", key="dfff")
wdesc = st.sidebar.toggle("w/desc?", key="dfff2")
#data
col1, col2 = st.columns(2)
with col1:
    save_name = st.sidebar.text_input("save name")
with col2:
    save = st.sidebar.button("save")

if save and save_name:
    s_not_company = None
    if not_company:
        s_not_company = "|".join(not_company)
    
    s_posted_date = None
    if posted_date:
        s_posted_date = posted_date
    
    s_yoe = None
    if yoe:
        s_yoe = "|".join(yoe)
    #office
    s_office = None
    if office:
        s_office = "|".join(office)

    s_salary = None
    if salary:
        s_salary = "|".join(salary)

    #notkeyword
    s_notkeyword = None
    if notkeyword:
        s_notkeyword = "|".join(notkeyword)

    #notkeyword
    s_keyword = None
    if keyword:
        s_keyword = "|".join(keyword)
    save_data = {
        "name":save_name,
        "s_not_company":s_not_company,
        "s_posted_date":s_posted_date,
        "s_yoe":s_yoe,
        "s_office":s_office,
        "s_salary":s_salary,
        "s_notkeyword":s_notkeyword,
        "s_keyword":s_keyword
    }
    pd.DataFrame([save_data]).to_sql("SaveFilters", con=engine,  index=False, if_exists="append")

if keyword or not_company:
    if notkeyword:
        data = data[~data['title'].str.contains('|'.join(notkeyword))]
    data = data[data['title'].str.contains('|'.join(keyword))]
    data = data[~data["company"].isin(not_company)]
    data = data[data["seen"]==seen]
    if wdesc:
        data = data[data["apply_link"]!=0]


    
    #data
    #if seen == False:
    #    list_seen = jobInfo[jobInfo['seen']==seen]['id'].tolist()
    #    list_seen
    #    data = data[data["id"].isin(list_seen)]
    if office:
        data = data[data["office"].isin(office)]
    if yoe:
        data = data[data["YOE"].isin(yoe)]
    if posted_date:
        data['posted'] = pd.to_datetime(data['posted']).dt.date
        data = data[data['posted']>=posted_date]
    if salary:
        data = data[data['salary'].isin(salary)]
    #data
    
    st.metric("Jobs#", len(data), "1.2 °F")
    
    for _, row in data.iterrows():
        st.image(row['img'])
        colored_header(label=f"{row['company']} | {row['title']} ", description=f"{row['id']} | {row['posted']} | {row['office']} | {row['location']} | {row['salary']} | {row['YOE']} | {row['size']} ", color_name="violet-70")
        with st.expander("see description"):
            st.write(row['description'])
        col1, col2, col3, col4, col5 = st.columns(5)

        col1.markdown(f'<a href="https://builtin.com{row["link"]}" target="_blank">link</a>', unsafe_allow_html=True)
        cc = col2.toggle("seen?", value=row.seen, key=str(str(row['id'])+str(_)))
        if cc and row.seen==0:
            insert_job = JobInfo(id=row['id'], company=row['company'], seen=True, apply=False, interview=False,datetime_seen=datetime.datetime.now(), datetime=datetime.datetime.now())
            try:
                session.add(insert_job)
                session.commit()
            except:
                pass
        if row["apply_link"]:
            col3.markdown(f'<a href="{row["apply_link"]}" target="_blank">apply link</a>', unsafe_allow_html=True)
            #"lever builtin greenhouse"
            if "lever" in str(row["apply_link"]): #or "builtin" in str(row["apply_link"]) or "greenhouse" in str(row["apply_link"]):
                r = requests.get(row["apply_link"])
                soup = BeautifulSoup(r.content)
                form = soup.find('form')
                inputs = form.find_all('input')
                col4.metric("Fields#", len(inputs), "1.2 °F")

        
        True if row.seen == 1 else False
        
        