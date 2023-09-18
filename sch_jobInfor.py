import pandas as pd
import schedule
import time
import json
from datetime import date, datetime
from bs4 import BeautifulSoup
import requests
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float,Boolean,select,Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import random

Base = declarative_base()
engine = create_engine('sqlite:///db.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


class JobDesc(Base):
    __tablename__ = "JobDescription"

    id = Column(Integer, primary_key=True)
    description = Column(Text)
    apply_link = Column(String(254))
    datetime = Column(DateTime)

Base.metadata.create_all(engine)

def getData():
    engine = create_engine('sqlite:///db.db')
    all_jobs = pd.read_sql("select * from Jobs where id is not null", con=engine)
    all_jobs.drop_duplicates(subset='id', keep="last", inplace=True)
    all_jobs['id'] = all_jobs['id'].astype('int')

    all_info = pd.read_sql("select id, seen from JobsInfo", con=engine)
    all_desc = pd.read_sql("select id from JobDescription", con=engine)

    
    df = all_jobs[~all_jobs['id'].isin(all_info['id'])]
    df = all_jobs[~all_jobs['id'].isin(all_desc['id'])]
    df['posted'] = pd.to_datetime(df['posted'])
    df.sort_values(by="posted", inplace=True, ascending=False)
    print(df['posted'])
    return df#[["company", "id", "posted"]]

data = getData()
batch_data = np.array_split(data, 500)
for batch in batch_data:
    for _, row in batch.iterrows():
        print(row['link'])
        r = requests.get("https://builtin.com"+str(row['link']))
        
        if r.status_code!=200:
            print("break")
            break;
        
        soup = BeautifulSoup(r.content)
        desc = soup.find("div", class_="job-description").text
        link = soup.find("div", class_="apply-now-result")['data-path']
        insert = JobDesc(id=int(row['id']), description=desc, apply_link=link, datetime=datetime.now())
        session.add(insert)
        session.commit()
        time.sleep(495+random.choice([10,5,22,38,15,16,7,13,18]))
    
    time.sleep(3600)
#print(batch_data)