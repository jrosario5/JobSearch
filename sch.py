import pandas as pd
import schedule
import time
import json
#import sqlalchemy
from datetime import date, datetime
from bs4 import BeautifulSoup
import requests
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float,Boolean,select,Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

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
Base.metadata.create_all(engine)
#dev-engineering
tags = ["Python", "Airflow", "Data Engineer", "Software Engineer"]

def job():
    engine = create_engine('sqlite:///db.db')

    for tag in tags:
        url = f"https://www.builtin.com/jobs?search={tag}"
        r = requests.get(url)
        soup = BeautifulSoup(r.content,"html.parser")
        
        pages_seek = soup.find('a', class_='page-link border rounded fw-bold border-0')['href'].split("=")
        pages_max = None
        for s in pages_seek:
            if s.isnumeric():
                pages_max = s
        
        for i in range(1, int(pages_max)):
            if i!=1:
                r = requests.get(url+"&page="+str(i))
                soup = BeautifulSoup(r.content,"html.parser")


            data = soup.find_all("div", {"data-id": "job-card"})
            
            df = {
                    "id":[],
                    "company":[],
                    "title":[],
                    "img":[],
                    "posted":[],
                    "office":[],
                    "location":[],
                    "salary":[],
                    "YOE":[],
                    "size":[],
                    "link":[]
                }
            for record in data:
                job = Job()
                id = None
                posted = None
                size =None
                location = None
                yeo = None
                salary = None
                link = record.find('a', class_='btn-outline-primary')#['href']
                if link==None:
                    link = record.find('a', class_='hover-underline')['href']
                elif link!=None:
                    link = record.find('a', class_='btn-outline-primary')['href']
                for s in link.split("/"):
                    if s.isnumeric():
                        id = s
                #df['id'].append(id)
                job.id = id
                #df['company'].append(record.find('div', {'data-id':"company-title"}).span.text)
                job.company = str(record.find('div', {'data-id':"company-title"}).span.text)
                #df['title'].append(record.find('a', class_="hover-underline").text)
                job.title = str(record.find('a', class_="hover-underline").text)
                #df['img'].append(record.find('img', {'data-id':"company-img"})['src'])
                job.img = str(record.find('img', {'data-id':"company-img"})['src'])
                posted = record.find_all('span', class_="font-barlow text-gray-03")[0].text
                #An Hour Ago
                if "Hours" in posted:
                    posted = posted.split(" ")[0]
                    if posted.isnumeric():
                        posted = pd.Timestamp.now() - pd.Timedelta(posted+' hours')
                elif "An Hour" in posted:
                    posted = pd.Timestamp.now() - pd.Timedelta('1 hours')
                elif "Yesterday" in posted:
                    posted = pd.Timestamp.now() - pd.Timedelta('1 days')
                elif "Today" in posted:
                    posted = pd.Timestamp.now()
                elif "Days" in posted:
                    posted = posted.split(" ")[0]
                    if posted.isnumeric():
                        posted = pd.Timestamp.now() - pd.Timedelta(posted+' days')
                #df['posted'].append(posted)
                job.posted = posted
                #df['office'].append(record.find_all('span', class_="font-barlow text-gray-03")[1].text)
                job.office = str(record.find_all('span', class_="font-barlow text-gray-03")[1].text)
                
                info2 = record.find_all('span', class_="font-barlow text-gray-03")[2].text
                if info2 in ["Hybrid", "Remote", "In Office"]:
                    location = info2
                elif "Employees" in info2:
                    size = info2
                elif "Annually" in info2:
                    salary = info2
                elif "Experience" in info2:
                    yeo = info2

                info3 = record.find_all('span', class_="font-barlow text-gray-03")[3].text
                if info3 in ["Hybrid", "Remote", "In Office"]:
                    location = info3
                elif "Employees" in info3:
                    size = info3
                elif "Annually" in info3:
                    salary = info3
                elif "Experience" in info3:
                    yeo = info3

                try:
                    info4 = record.find_all('span', class_="font-barlow text-gray-03")[4].text
                except IndexError:
                    info4 = ""
                
                if info4 in ["Hybrid", "Remote", "In Office"]:
                    location = info4
                elif "Employees" in info4:
                    size = info4#df['size'].append(info4)
                elif "Annually" in info4:
                    salary = info4
                elif "Experience" in info4:
                    yeo = info4

                #df['YOE'].append(yeo)
                job.yoe = yeo
                #df['salary'].append(salary)
                job.salary = salary

                #df['location'].append(location)
                job.locations = location
                #df['size'].append(size)
                job.size = size
                #df['link'].append(link)
                job.link = link
                try:
                    session.add(job)
                    session.commit()
                except:
                    pass
                #job = Job(id=int(df['id'][i-1]), company=df['company'][i-1], title=df['title'][i-1], img=df['img'][i-1], posted=df['posted'][i-1], office=df['office'][i-1], locations=df["location"][i-1], salary=df["salary"][i-1], yoe=df["YOE"][i-1], size=df["size"][i-1], link=df["link"][i-1])
                #try:
                #    #session.add(job)
                #    #session.commit()
                #except:
                #    pass
        #df.to_sql()

#schedule.every(15).minutes.do(job)


#while True:
#    schedule.run_pending()
#    time.sleep(1)
job()