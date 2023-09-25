import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
import requests
import numpy as np
from streamlit_extras.colored_header import colored_header
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float,Boolean,select,Text,or_
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import datetime
from streamlit_tags import st_tags_sidebar

Base = declarative_base()
engine = create_engine('sqlite:///db.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

class Job(Base):
    __tablename__ = "Jobs"
    
    id = Column(Integer, primary_key=True)
    company = Column(String(250))
    title = Column(String(254))
    img = Column(Text)
    posted = Column(DateTime)
    office = Column(String(254))
    locations = Column(String(251))
    salary = Column(String(254))
    yoe = Column(String(254))
    size = Column(String(254))
    link = Column(Text)
    #def __init__(self, id):
    #    self.id = id
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

class JobDesc(Base):
    __tablename__ = "JobDescription"

    id = Column(Integer, primary_key=True)
    description = Column(Text)
    apply_link = Column(String(254))
    datetime = Column(DateTime)

Base.metadata.create_all(engine)

st.set_page_config(
    page_title="CC3", page_icon="⬇", layout="wide"
)
companies = set([(r.company) for r in session.query(Job.company).all()])
company = st.sidebar.multiselect(label="company", options=(companies) )
not_company = st.sidebar.multiselect(label="not company", options=(companies) )
office = st.sidebar.multiselect("office", set([(r.office) for r in session.query(Job.office).all()]) )
posted_date = st.sidebar.date_input("posted", format="YYYY/MM/DD")

seen = st.sidebar.toggle("seen?", key="dfff")
wdesc = st.sidebar.toggle("w/desc?", key="dfff2")


data = session.query(Job,JobDesc,JobInfo).join(JobDesc, JobDesc.id == Job.id, isouter=True).join(JobInfo, Job.id==JobInfo.id, isouter=True).filter(or_(JobInfo.seen==seen, JobInfo.seen==None))#.filter(JobInfo.seen==None)##session.query(Job)#.join(JobDesc, JobDesc.id == Job.id, isouter=True)
data
if wdesc:
    data = data.filter(JobDesc.apply_link!=None)
    #data = data.join(JobDesc, Job.id == JobDesc.id, isouter=False)
    #data
if seen:
    data = data.filter(JobInfo.seen==True)
    #data = data.join(JobInfo, JobInfo.id == Job.id, isouter=False)
#for rs in data:
#    try:
#        st.write(rs.JobInfo.seen)
#    except:
#        pass
if company:
    data= data.filter(Job.company.in_( company ))
elif not_company:
    data= data.filter(Job.company.not_in( not_company ))
else:
    pass
    #data  = session.query(Job).all()

if office:
    data= data.filter(Job.office.in_( office ))
    #data = session.query(Job).filter(Job.office.not_in( office ))

if posted_date:
    data= data.filter(Job.posted>=posted_date)



#df = pd.DataFrame([(r.id, r.company, r.title, r.posted, r.office) for r in data], columns=['id', 'company', 'title', 'date', 'office'])
#df

st.sidebar.metric("Jobs#", data.count(), "1.2 °F")
st.sidebar.metric("Remote #", data.filter(Job.locations=="Remote").count() )
st.sidebar.metric("Hybrid #", data.filter(Job.locations=="Hybrid").count())

for r in data.limit(45):
    st.image(r.Job.img)
    colored_header(label=f"{r.Job.company} | {r.Job.title} ", description=f"{r.Job.id} | {r.Job.posted} | {r.Job.office} | {r.Job.locations} | {r.Job.salary} | {r.Job.yoe} | {r.Job.size} ", color_name="violet-70")
    col1, col2, col3, col4, col5 = st.columns(5)

    col1.markdown(f'<a href="https://builtin.com{r.Job.link}" target="_blank">link</a>', unsafe_allow_html=True)
    cc = col2.toggle("seen?", value=True if r.JobInfo else False, key=str(str(r.Job.id)))
    if cc:
       JobInfoQ = session.query(JobInfo).get(r.Job.id)
       JobInfoQ
       
       if JobInfoQ is None:
            insert_jobInfo = JobInfo(id=r.Job.id, company=r.Job.company, seen=True, apply=False, interview=False,datetime_seen=datetime.datetime.now(), datetime=datetime.datetime.now())
            session.add(insert_jobInfo)
            session.commit()
       
    
    
    with st.expander("see description"):
            #Product.query.filter_by(id=101).first()
            data_desc = session.query(JobDesc).filter(JobDesc.id==r.Job.id).first()
            st.write('data_desc.description')
            if data_desc:
                st.write(data_desc.description)
    if r.JobDesc:
        if r.JobDesc.apply_link:
                col3.markdown(f'<a href="{r.JobDesc.apply_link}" target="_blank">apply link</a>', unsafe_allow_html=True)