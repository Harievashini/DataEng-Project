import pandas as pd
import numpy as np
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import json
from datetime import date

url = "http://34.83.136.192:8000/getStopEvents/"
html = urlopen(url)
soup = BeautifulSoup(html, 'lxml')
text = soup.get_text()
#print(text)
h1=soup.find_all('h1')
#print(h1)
ii=str(h1)
clean=BeautifulSoup(ii, "lxml").get_text()
today = re.findall('\d{4}-\d{2}-\d{2}', clean)
temp_t = today[0]
#print(temp_t)
trip=[]
h3=soup.find_all('h3')

for i in h3:
  ii=str(i)
  clean=BeautifulSoup(ii, "lxml").get_text()
  numbers = re.findall('[0-9]+', clean)
  trip.append(int(numbers[0]))
  
#print(trip)
#HEADINGS
tab=soup.find_all('table')
#print(tab[0])
headings=['trip_id']
for i in tab:
  tr=i.find_all('tr')
  for h in tr[0]:
    hh=str(h)
    head=BeautifulSoup(hh, "lxml").get_text()
    if head !="":
      headings.append(head)
  break
headings.append('se_Date')
#print(headings)

leng=soup.find_all('tr')
#print(len(leng))
final=[]
eachrow={}
ct=0
for i in tab:
  tr=i.find_all('tr')
  for j in tr[1:]:
    
    td=j.find_all('td')
    eachrow[headings[0]]=trip[ct]
    #print(eachrow)
    for val in range(len(td)):
      v=str(td[val])
      vv=BeautifulSoup(v, "lxml").get_text()
      #print(vv)
      eachrow[headings[val+1]]=vv
    #print(eachrow)
    eachrow[headings[-1]]=temp_t
    final.append(eachrow)
    eachrow={}
  ct+=1
#print(final[-1])
#j = json.dumps(final)
dt= date.today()
today=dt.strftime("%Y-%m-%d")
filename="examples/clients/cloud/python/se_data/"+today+".json"

with open(filename,'w') as f:
  json.dump(final,f)
