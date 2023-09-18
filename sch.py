import pandas as pd
import schedule
import time
import json
import sqlalchemy
from datetime import date, datetime
from bs4 import BeautifulSoup
import requests

#dev-engineering
tags = ["Python", "Airflow", "Data Engineer", "Software Engineer"]

def job():
    engine = sqlalchemy.create_engine('sqlite:///db.db')

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
                df['id'].append(id)
                df['company'].append(record.find('div', {'data-id':"company-title"}).span.text)
                df['title'].append(record.find('a', class_="hover-underline").text)
                df['img'].append(record.find('img', {'data-id':"company-img"})['src'])
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
                df['posted'].append(posted)
                df['office'].append(record.find_all('span', class_="font-barlow text-gray-03")[1].text)
                
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

                df['YOE'].append(yeo)
                df['salary'].append(salary)

                df['location'].append(location)
                df['size'].append(size)
                df['link'].append(link)
                """r = requests.get("https://www.builtin.com"+link)
                soup = BeautifulSoup(r.content,"html.parser")
                desc_data = soup.find("div", class_="job-description")
                text = []
                print(desc_data)
                if desc_data:
                    pa = desc_data.find_all('p')
                    for p in pa:
                        text.append(p.text)
                    li = desc_data.find_all('li')
                    for l in li:
                        text.append(l.text)
                    if pa!=None:
                        text = []
                print(text)
                df['description'].append(text)"""
                i+=1
            print()
            #print(pd.DataFrame(df))
            """
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
            """

            i=len(df['id'])
            
            
            df['location'] = df['location'] + [None]*(i - len(df['location']))
            
            
            df['salary'] = df['salary'] + [None]*(i - len(df['salary']))
            df['YOE'] = df['YOE'] + [None]*(i - len(df['YOE']))
            df['size'] = df['size'] + [None]*(i - len(df['size']))
            #(len(df['size']))

            print(len(df['id']))
            print(len(df['company']))
            print(len(df['title']))
            print(len(df['img']))
            print(len(df['posted']))
            print(len(df['office']))

            print(len(df['location']))
            print(len(df['salary']))
            print(len(df['YOE']))
            print(len(df['size']))
            print(len(df['link']))
            #print(df['YOE'])
            #print(len(df['description']))

            dfs = pd.DataFrame(df)
            print(dfs['YOE'])
            #df['Date'] = datetime.now()#date.today()
            #df.set_index('Date', inplace=True)
            #print(df)
            dfs.to_sql("Jobs", con=engine, index=True, if_exists="append")
        
        #df.to_sql()

#schedule.every(15).minutes.do(job)


#while True:
#    schedule.run_pending()
#    time.sleep(1)
job()