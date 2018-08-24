from bs4 import BeautifulSoup
import requests
import numpy as np
import re
import json
from elasticsearch import Elasticsearch

h=0
OURL= 'https://health.usnews.com' # ORIGINAL URL OF THE WEBSITE 
URL = "https://health.usnews.com/doctors/city-index/new-jersey" # URL FOR NEW JERSEY

headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"} # HEADER FOR REQUEST 
req = requests.get(URL,headers=headers) #SENDING A REQUEST 
soup = BeautifulSoup(req.content, 'html5lib')

cities=[] #CITIES IN NEW JERSEY
citylinks=[]#LINK OF EACH CITY IN NEW JERSEY 

for ppp in soup.find_all('li',class_='List__ListItem-dl3d8e-1 kfaWAY'):
    url1=ppp.a['href']
    url2=OURL+url1
    cities.append(ppp.text)
    citylinks.append(url2)
    
ran=np.random.randint(0,len(cities)-1,3)#selecting 3 random cities

Rcities=[] #list to store the randomly selected city names 
Rcitylinks=[] #list to store the links of the randomly selected city links

   
for j in ran :
    Rcities.append(cities[j])
    Rcitylinks.append(citylinks[j])
    city_lists = {key:[] for key in Rcities}
    
Doclinks=[]# list to store the links of all the doctors 
Docnames=[]# list to store all the names of the doctors 
Specnames=[]
Speclinks=[]
for b in range(0,4):
    url3=Rcitylinks[b]   
    r1 = requests.get(url3,headers=headers).text
    soup1 = BeautifulSoup(r1, 'html5lib')
    
    for c in range(0,2) :
        sws=soup1.find_all('li',class_='List__ListItem-dl3d8e-1 kfaWAY')
        zzz=sws[c]
        Specnames.append(zzz.text)
        url4=zzz.a['href']
        url5=OURL+url4 
        Speclinks.append(url5)        
        r2 = requests.get(url5,headers=headers).text
        soup2 = BeautifulSoup(r2, 'html5lib')
         
        for d in range(0,2) :
            ded=soup2.find_all('li', class_ = 'block-normal block-loose-for-large-up')
            ddd=ded[d]
            url6=ddd.h3.a['href']
            url7=OURL+url6
            Doclinks.append(url7)
            src1=requests.get(url7,headers=headers).text
            soup3= BeautifulSoup(src1,'html5lib')
            
            doc_ref = soup3.find('div', class_ ='flex-row relative' ).h1.text
            doc_name = doc_ref.strip(' \t\n\r')[0:50]
            Docnames.append(doc_name)
            #Final_list = {key:[] for key in Docnames}
            Doc_overview=soup3.find('div', class_ ='block-normal clearfix' ).p.text
            
            Doc_YIPl=soup3.find_all('span', class_ ='text-large heading-normal-for-small-only right-for-medium-up')
            Doc_YIP=Doc_YIPl[1].text
            
            Doc_languagel=(soup3.find('span', class_ ='text-large heading-normal-for-small-only right-for-medium-up text-right showmore').text).strip('\n')
            Doc_language=Doc_languagel.replace('\n','')
            
            dadd=soup3.find_all('span',class_='text-strong')
            dadd1=(dadd[1].text).strip('\n')
            Doc_OL=dadd1.replace('\n','')
            
            try:
                daff=soup3.find('a',class_='heading-larger')
                Doc_Aff=daff.text
            except:
                 Doc_Aff='none'
            
            Doc_spec=zzz.text
            
            try:
                doc_subsp = soup3.find('p', class_ ='text-large block-tight').text
                   
            except:
                doc_subsp = ['no speciality found']
               
            
                
            doc_ed = soup3.find_all(class_ ="block-loosest")[5]
            doc_ed1 = doc_ed.find_all('li')
            doc_ed5 = ' '
            doc_ed6 = []
            for i in doc_ed1:
                doc_ed2 = str(i.text)
                doc_ed2 = re.findall('[a-zA-Z0-9]\S*',doc_ed2)
                doc_ed2 = doc_ed5.join(doc_ed2)
                doc_ed6.append(doc_ed2)
           
            doc_cert2 = []
            doc_cert1 = ' '  
            doc_ed3 = soup3.find_all(class_ ="block-loosest")[6]
            doc_ed14 = doc_ed3.find_all('li')
            for i in doc_ed14:
                doc_cert = str(i.text)
                doc_cert = re.findall('[a-zA-Z0-9]\S*',doc_cert)
                doc_cert = doc_cert1.join(doc_cert)
                doc_cert2.append(doc_cert)
            
            doc_profile = {
                'city':Rcities[b],
                'Full Name': doc_name,
                'Overview': Doc_overview,
                'Years in practice': Doc_YIP,
                'Language':Doc_language,
                'Office location':Doc_OL,
                'Hosptial Affiliation':Doc_Aff,
                'Speciality': Doc_spec,
                'Sub speciality': doc_subsp,
                'Education and Medical training':doc_ed6,
                'Certification and Licensure':doc_cert2,
                }
            print doc_profile
            
            es = Elasticsearch([{'host': 'localhost', 'port': 9200}])  
            x = json.dumps(doc_profile)
            resp=es.index(index='a',doc_type='a',id=h,body=json.loads(x))
            print resp['result']
            h+=1
            
                        
            
            
            
            
            
            
            
