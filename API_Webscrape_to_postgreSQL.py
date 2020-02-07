#!/usr/bin/env python
# coding: utf-8

# In[3]:


# 1) import dependencies
from splinter import Browser
from bs4 import BeautifulSoup as bs
import time
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from pprint import pprint
import datetime as DT
import requests
import json
import re


# In[4]:


# 2) Create a path to chrome driver: Make sure to change the path!
executable_path = {'executable_path': '/Users/Pariah/Downloads/chromedriver'}
# 3) load the path into browser 4) initialize the browser
browser = Browser('chrome', **executable_path, headless=True)


# # E of ETL Part 1: Data Extraction via API Call for Bestbuy

# In[5]:


url = 'https://api.bestbuy.com/v1/products((categoryPath.id=abcat0502000))?apiKey=BC9ll4HTWIkIUFAUJjZFoKq3&show=manufacturer,modelNumber,name,regularPrice,salePrice,details.name,details.value,shortDescription,upc,url&pageSize=100&page=1&format=json'

# for product list:
response = requests.get(url)
data = response.json()
data


# # T of ETL Part 1: Data Extraction via API Call for Bestbuy

# In[6]:


list_one = []
for x in range(1, 10):
    url = f'https://api.bestbuy.com/v1/products((categoryPath.id=abcat0502000))?apiKey=BC9ll4HTWIkIUFAUJjZFoKq3&show=manufacturer,modelNumber,upc,name,regularPrice,salePrice,url,details.name,details.value,shortDescription&pageSize=100&page={x}&format=json'
    response = requests.get(url)
    data = response.json()
    list_one.extend(data.get("products"))
    print(x)

    


# In[7]:


productlist = list_one

bestbuy_laptops = []
  
    
for product in productlist:
    title = product['name']
    
    regularprice = product['regularPrice']
    saleprice = product['salePrice']
    if saleprice:  # is not null:
        price = saleprice
    else: 
        price = regularprice
    
    link = product['url']
    upc = product['upc']
    model = product['modelNumber']
    brand = product['manufacturer']
       
    for info in product['details']:
                
        # RAM in GB:
        if info['name'] == 'System Memory (RAM)':
            ram = re.findall('\d+', info['value'])[0]

        # in inches:
        if info['name'] == 'Screen Size':
            raw_ss = info['value']
            raw_ss = raw_ss[0: raw_ss.find(' ')]
            screensize = raw_ss.strip()

        # HD in GB:
        if info['name'] == 'Hard Drive Capacity':
            hd = re.findall('\d+', info['value'])[0]
        
        # CPU:
        if info['name'] == 'Processor Model':
            cpu = info['value']
            
            
    dic = {
        'brand' : brand,
        'model': model,
        'screensize': screensize,
        'cpu': cpu,
        'ram': ram,
        'hd': hd,
        'upc': upc,
        'price': price,
        'title': title,
        'link': link
    }
    
    bestbuy_laptops.append(dic)


# In[8]:


bestbuy_df = {}
bestbuy_df = pd.DataFrame(bestbuy_laptops)
bestbuy_df = bestbuy_df[['brand', 'model', 'screensize', 'cpu', 'ram', 'hd', 'upc', 'price', 'link', 'title']]

# Add the load date:
now = DT.datetime.now()

bestbuy_df['lastUpdated'] = now.strftime("%Y-%m-%d %H:%M")

final_bestbuy_df = bestbuy_df.dropna(subset=['upc'])
final_bestbuy_df.head()


# # E of ETL Part 2: Data Extraction via Webscraping for FRY'S

# In[9]:


###############################################################
# EXTRACTION WITH TRANSFORMATION:
###############################################################

#FRYS:
###############################################################
# 5) initialize the page parameters and the first site visit:
###############################################################
next_page = "https://www.frys.com/search?cat=-73060&nearbyStoreName=false&pType=pDisplay&rows=20&resultpage=0&start=0&rows=20"
page_remaining = True
page_no = 0
increment = 20
start_no = 0
# 6) make a variable that holds the url we want to scrape data from
laptops = {}

frys_laptops = []

# 7) using the browser function, visit the webpage
browser.visit(next_page)
# 8) stop the scrape process for 1.25 seconds using the time function. This allows the webpage to load in full
time.sleep(1.25)

# 9) create the html variable that allows the browser to visit the HTML elements on the webpage
html = browser.html
# 10) initialize the BeautifulSoup module by telling it to parse html from the HTML elements on the webpage.
soup = bs(html, 'html.parser')


# 11) loop through until no products are found:
###############################################################
while page_remaining == True:
    
    # 12) Data retrieval logic: 
    ###############################################################
    try:
        mothers = soup.find_all('div', class_='togrid')
        for mother in mothers:
            title = mother.find('p', class_='productDescp').get_text().strip()
            
            raw_price = mother.find('label', class_='red_txt').get_text().strip()
            text_price = raw_price.replace("$", "")
            nocommas = text_price.replace(",", "")
            price = float(nocommas)
            
            partial_link = mother.find('p', class_='productDescp').find('a')['href'].strip()
            base_url = "https://www.frys.com/"
            link = base_url + partial_link
            
            results = mother.find_all('div', class_='prodModel')
            for result in results:
                new_list = result.find_all('p', class_='mar-btm')
                for p in new_list:
                    dataitem = p.text.replace(" ", "")
                    di = dataitem.replace('\n', "")
                    dataitem_split = di.split(":")
                    if dataitem_split[0] == "Brand":
                        brand = dataitem_split[1]
                    if dataitem_split[0] == "UPC":
                        upc = dataitem_split[1]
                    if dataitem_split[0] == "Model":
                        model = dataitem_split[1]
                        
            # 13) create a new dictionary that will hold the data that we scrape
            dic = {
                'title': title,
                'price': price,
                'link': link,
                'upc': upc,
                'model': model,
                'brand': brand
            }
            print(dic)
            frys_laptops.append(dic)
            
    except AttributeError:
        # errors out when there are 2 prices in red (price with rebate)
        print("error found. skipping...")
    
    
    # 14) then process the Next Page logic:
    ###############################################################
    page_no += 1
    start_no = start_no + increment
    next_page = f'https://www.frys.com/search?cat=-73060&nearbyStoreName=false&pType=pDisplay&rows=20&resultpage={page_no}&start={start_no}&rows=20'

    browser.visit(next_page)
    time.sleep(3)

    html = browser.html
    soup = bs(html, 'html.parser') 
    
    next_page_avail = soup.find_all('p')
    for p in next_page_avail:
        message = p.text
        if message == " No products were found that matched your search":
            page_remaining = False
        


# # Data Transformation: Fry's

# In[10]:


# create a dataframe from the list of dictionaries:
frys_df = pd.DataFrame(frys_laptops)
frys_df
# print(len(frys_df))


# In[11]:


frys_df['modified_title'] = frys_df['title'].str.replace('"', '",')
frys_df['modified_title'].head()


# In[12]:


# for easier parsing:

frownum1 = 0

for x in frys_df['modified_title']:
    new_title = x[x.find(' '):]
    new_title = new_title[new_title.find(' '):]
    new_title = new_title[new_title.find(' '):]
    new_title = new_title.replace('Refurbished ', '')
    new_title = new_title.replace('
', '')
    new_title = new_title.replace('-inch', '",')
    new_title = new_title.replace(' Measured Diagonal', '",')
    
    frys_df.loc[frownum1, 'modified_title'] = new_title
        
    frownum1 +=1

frys_df['modified_title']


# In[13]:


# UPC FIRST because the rest is such a mess:
frys_df['raw_specs'] = frys_df['upc'] + ', ' + frys_df['modified_title']
frys_df.head()


# In[14]:


frys_specs = []
frys_specs = frys_df['raw_specs'].str.split(', ', expand = True)
frys_specs.head()
frys_specs[160:]


# ## Screen Size

# In[15]:


# first found in col 1:
fraw_size = []
f_size = []

fraw_size1 = frys_specs[1]

for x in fraw_size1:
    if (x.find('"') > 0):
        # find the first instance of "
        fsize1 = x[0:x.find('"')]
        # find the last space before the "
        fsize2 = fsize1[fsize1.rfind(' ')+1:]
        try:
            firstchar = int(fsize2[0])
            f_size.append(float(fsize2))
        except:
            f_size.append('')
    else:
        f_size.append('')

# len(f_size)
# f_size


# In[16]:


# round 2: found in col 2:
f_size2 = []
frownum2 = 0

for x in f_size:
    try:
        if (x == '' or x is None):
            new_size = frys_specs.loc[frownum2, 2]
            new_size2 = new_size.lstrip()
            if (new_size2.find('"') > 0):
                try:
                    firstchar = int(new_size[0])
                    new_size = new_size[0:new_size.find('"')]
                    new_size = new_size[new_size.rfind(' ')+1:]
                    f_size2.append(float(new_size))
                except:
                    f_size2.append(x)
            else:
                f_size2.append(x)
        else:
            f_size2.append(x)
    
    except:
        f_size2.append('')
        
    frownum2 += 1
       
f_size2
    


# In[17]:


# round 3: lastly found in col 3:
frys_size = []
frownum2 = 0

for x in f_size2:
    try:
        if (x == '' or x is None):
            new_size = frys_specs.loc[frownum2, 3]
            new_size2 = new_size.lstrip()
            if (new_size2.find('"') > 0):
                try:
                    firstchar = int(new_size[0])
                    new_size = new_size[0:new_size.find('"')]
                    new_size = new_size[new_size.rfind(' ')+1:]
                    frys_size.append(float(new_size))
                except:
                    frys_size.append(x)
            else:
                frys_size.append(x)
        else:
            frys_size.append(x)
    
    except:
        frys_size.append('')
    
    frownum2 += 1
        
    

frys_size
    

frys_specs['screensize'] = frys_size

frys_specs


# ## CPU

# In[18]:


# cpu round 1: col 1:
fraw_cpu = []
fcpu = []

fraw_cpu = frys_specs[1]

for x in fraw_cpu:
    # remove 'processor':
    f_cpu = x.replace(' Processor', '')
    
    # get rid of the screensize:
    if f_cpu.find('"') > 0:
        f_cpu = f_cpu[0:f_cpu.rfind(' ')]
       
    
    
    if f_cpu.find('Intel') > 0:
        f_cpu = f_cpu[f_cpu.find('Intel'):]
        fcpu.append(f_cpu)

    elif f_cpu.find('AMD') > 0:
        f_cpu = f_cpu[f_cpu.find('AMD'):]
        fcpu.append(f_cpu)
    
    else:
        fcpu.append('')

fcpu
        


# In[19]:


# cpu round 2:
fcpu1 = []
frnum = 0

# for x from the first round:
for x in fcpu:
    if x == '':
        f_cpu1 = frys_specs.loc[frnum, 2]
        
        # if it says 'Processor' in it:
        if f_cpu1.find('Processor') > 0:
            # cpu info ends at the first instance of 'Processor':
            f_cpu1 = f_cpu1[0:f_cpu1.find(' Processor')]
            
            # cpu info should start after 'With'
            f_cpu1 = f_cpu1.replace('With', 'with')
            f_cpu1 = f_cpu1[f_cpu1.find('with'):]

            # clean the rest out:
            f_cpu2 = f_cpu1.replace('with', '')
            f_cpu2 = f_cpu2.replace('2GB Memor', '')
            f_cpu2 = f_cpu2.replace('4GB Memor', '')
            f_cpu2 = f_cpu2.replace('8GB Memor', '')  
            f_cpu2 = f_cpu2.replace('16GB Memor', '')
            f_cpu2 = f_cpu2[1:]

            if len(f_cpu2) > 2:
                fcpu1.append(f_cpu2)
            else:
                fcpu1.append('')
        
        # if 'Processor' is not part of its name:
        elif (f_cpu1.find('Intel') >= 0) or (f_cpu1.find('AMD') >= 0):
            
            # clean out junk:
            f_cpu2 = f_cpu1.replace('Full HD IPS Laptop 8GB 1TB HDD Windows 10 Home ', '')
            f_cpu2 = f_cpu2.replace(' Laptop with 4GB Memory', '')
            f_cpu2 = f_cpu2.replace('"', '')
            f_cpu2 = f_cpu2.replace(' 4GB Memory', '')
            f_cpu2 = f_cpu2.replace(' Halo Keyboard', '')
            f_cpu2 = f_cpu2.replace(' 8GB 512GB SSD Touchscreen Laptop - Grey', '')
            f_cpu2 = f_cpu2.replace('   - Aluminum', '')
            f_cpu2 = f_cpu2.replace('With', 'with')
            
            
            if f_cpu2.find('with') > 0:
                # then retrieve everything after as cpu:
                f_cpu2 = f_cpu2[f_cpu2.find('with')+5:]
                fcpu1.append(f_cpu2)
                
            elif f_cpu2.find('3rd Party Intel'):
                fcpu1.append(f_cpu2)
                
            elif f_cpu2.find('Intel') >= 0:
                f_cpu2 = f_cpu2[f_cpu2.find('Intel'):]
                fcpu1.append(f_cpu2)

            elif f_cpu2.find('AMD') >= 0:
                f_cpu2 = f_cpu2[f_cpu2.find('AMD'):]
                fcpu1.append(f_cpu2)
            
            else:
                fcpu1.append('')
        else:
            fcpu1.append('')

    else:  # x != '':
        fcpu1.append(x)

        
    frnum += 1
    


fcpu1


# In[20]:


# cpu round 3: look in col 3:
fraw_cpu2 = []
fcpu2 = []
frownum3 = 0


for x in fcpu1:
    if x == '':
        # check for nulls:
        if frys_specs.loc[frownum3, 3] is None:
            fcpu2.append('')

        else:    
            # pull the data from col 3:
            f_cpu3 = frys_specs.loc[frownum3, 3]

            # clean the junk out:
            f_cpu4 = f_cpu3.replace('1GB Memory', '')
            f_cpu4 = f_cpu3.replace('2GB Memor', '')
            f_cpu4 = f_cpu4.replace('4GB Memor', '')
            f_cpu4 = f_cpu4.replace('8GB Memor', '')  
            f_cpu4 = f_cpu4.replace('16GB Memor', '')
            f_cpu4 = f_cpu4.replace('320GB', '')
            f_cpu4 = f_cpu4.replace('256GB', '')
            f_cpu4 = f_cpu4.replace('16GB', '')
            f_cpu4 = f_cpu4.replace('4GB', '')
            f_cpu4 = f_cpu4.replace('1TB', '')
            f_cpu4 = f_cpu4.replace('Hard Drive', '')
            f_cpu4 = f_cpu4.replace('Windows 10', '')
            f_cpu4 = f_cpu4.replace('with', 'With')

            f_cpu4 = f_cpu4[f_cpu4.find('With')+4:]
            f_cpu4 = f_cpu4.replace(' Processor', '')
            f_cpu4 = f_cpu4.replace('y,', '')
            f_cpu4 = f_cpu4.replace('Storage', '')
            f_cpu4 = f_cpu4.replace('GB', '')
            f_cpu4 = f_cpu4.replace('"', '')

            if (f_cpu4.find('Intel') > 0) or (f_cpu4.find('AMD') > 0):
                fcpu2.append(f_cpu4[1:])
            else:
                fcpu2.append('')
                
    else:
        fcpu2.append(x)
    
    frownum3 += 1


fcpu2
    
    
 
    
    


# In[21]:


# cpu round 4: translations:
fcpu3 = []

for x in fcpu2:
    if (x.find('Pentium') >0):
        fcpu3.append('Intel Pentium')
    
    elif (x.find('Ci5') >0) or (x.find('Ci5-2520M') >0):
        fcpu3.append('Intel Core i5')
    
    elif (x.find('Celeron') >0) or (x.find('Celeorn') >0):
        fcpu3.append('Intel Celeron')
        
    elif (x.find('Ci3') >0):
        fcpu3.append('Intel Core i3')
        
    elif (x.find('Ci7') >0):
        fcpu3.append('Intel Core i7')    

    elif (x.find('Ryzen5') >0) or (x.find('Ryzen 5') >0):
        fcpu3.append('AMD Ryzen 5')

    elif (x.find('Ryzen3') >0) or (x.find('Ryzen 3') >0) or (x.find('Ryzen R3') >0):
        fcpu3.append('AMD Ryzen 3')
        
    elif (x.find('AMD A4') >0):
        fcpu3.append('AMD A4')

    elif (x.find('AMD A9') >0):
        fcpu3.append('AMD A9')

    elif (x.find('AMD A10') >0):
        fcpu3.append('AMD A10')
        
    elif (x.find('AMD R3') >0):
        fcpu3.append('AMD R3')
        
    else:
        fcpu3.append(x)
        
fcpu3   


fcpu4 = []

for x in fcpu3:
    if (x.find('-') > 0):
        fcpu4.append(x[0:x.find('-')])
    else:
        fcpu4.append(x)
        
fcpu4


frys_specs['cpu'] = fcpu4

frys_specs  


# ## RAM

# In[22]:


ram_string_arr  = frys_df['modified_title']

ram_num_arr = []

for x in ram_string_arr:
    raw_mem = x.replace('memory', 'Memory')
    raw_mem = raw_mem.replace(',', ' ')
    
    if raw_mem.find('Memory') >=0:
        #clean the data:
        ramNo = raw_mem[raw_mem.find('Memory')-5:raw_mem.find('Memory')-3]
        ramNo = ramNo.replace(' ','')
        ram_num_arr.append(int(ramNo))
    
    elif raw_mem.find('8 GB DDR4 SDRAM') >= 0:
        ramNo = 8
        ram_num_arr.append(int(ramNo))
    
    elif raw_mem.find('RAM') >= 0:
        #clean the data:
        ramNo = raw_mem[raw_mem.find('RAM')-4:raw_mem.find('RAM')-3]
        ram_num_arr.append(int(ramNo))
    
    elif raw_mem.find('Full HD IPS Laptop 8GB') >= 0:
        ramNo = 8
        ram_num_arr.append(int(ramNo))
    
    else:
        ram_num_arr.append('')

ram_num_arr

# ram_string_arr[237]
  
frys_specs['ram'] = ram_num_arr

frys_specs  


# ## HD

# In[23]:


raw_frys_hd_arr = frys_df['modified_title']

frys_hd_arr = []

for x in raw_frys_hd_arr:
    if x.find('Hard Drive') >= 0:
        # clean data:
        hd = x[x.find('Hard Drive')-6: x.find('Hard Drive')-1]
        hd = hd.replace(', ', ' ')
        hd = hd.replace(',', ' ')
        hd = hd.strip()
        
        if hd.find('TB') >= 0:
            hd = hd[hd.find('TB')-1:hd.find('TB')]
            hd = int(hd) * 1000  # to convert it to GB
            frys_hd_arr.append(int(hd))
        elif hd.find('GB') >= 0:
            hd = hd[0:hd.find('GB')]
            frys_hd_arr.append(int(hd))
    
    
    elif x.find('HD') >= 0:
         # clean data:
        hd = x[x.find('HD')-6:x.find('HD')+2]

        #clean data
        if hd.find('TB') >= 0:
            hd = hd[hd.find('TB')-1:hd.find('TB')]
            hd = int(hd) * 1000 # to convert to GB
            frys_hd_arr.append(int(hd))
        elif hd.find('GB') >=0:
            hd = hd[0:hd.find('GB')]
            frys_hd_arr.append(int(hd))

        else:
            frys_hd_arr.append('')
            
            

    elif x.find('SSD') >= 0:
         # clean data:
        hd = x[x.find('SSD')-6:x.find('SSD')+3]
        
        if hd.find('GB') >=0:
            hd = hd[0:hd.find('GB')]
            frys_hd_arr.append(int(hd))
        else:
            frys_hd_arr.append('')
                        
            

    elif x.find('Storage') >= 0:
         # clean data:
        hd = x[x.find('Storage')-6:x.find('Storage')-1]

        #clean data
        if hd.find('GB') >=0:
            hd = hd[0:hd.find('GB')]
            frys_hd_arr.append(int(hd))
        else:
            frys_hd_arr.append('')
                        
    
    else:
        frys_hd_arr.append('')

frys_hd_arr

  
frys_specs['hd'] = frys_hd_arr

frys_specs  


# ## Final Frys Table

# In[24]:


frys_specs.head()

final_frys_specs = pd.DataFrame(frys_specs[[0, 'screensize', 'cpu', 'ram', 'hd']])
final_frys_specs.rename(columns={0: 'upc'}, inplace = True)
final_frys_specs.head()


# In[25]:


frys_df.head()
shorter_frys_df = frys_df[['brand', 'link', 'model', 'price', 'title', 'upc']]
shorter_frys_df.head()


# In[26]:


frys_merged1_df = shorter_frys_df.merge(final_frys_specs, on='upc', how='left')
final_frys_df = frys_merged1_df[['brand', 'model', 'screensize', 'cpu', 'ram', 'hd', 'upc', 'price', 'link', 'title']]


# Add the load date:
now = DT.datetime.now()

final_frys_df['lastUpdated'] = now.strftime("%Y-%m-%d %H:%M")

final_frys_df.head()


# # Load: Loading both Frys and BestBuy data into PostgreSQL

# In[27]:


###############################################################
# LOAD: 
###############################################################


# change to your username:password@localhost:5432/laptop_db:
connection = "postgres:helloworld@localhost:5432/laptop_db"
engine = create_engine(f'postgresql://{connection}')

# add the FRYS dataframe to the table:
final_frys_df.to_sql(name="frys_laptops", con=engine, if_exists='replace', index=False)



# add the BEST BUY dataframe to the table:
final_bestbuy_df.to_sql(name="bestbuy_laptops", con=engine, if_exists='replace', index=False)


# combine the 2 tables:
final_frys_df['store'] = "Fry's"
final_bestbuy_df['store'] = "Bestbuy"

combo_df_arr = [final_bestbuy_df, final_frys_df]
final_laptops_df = pd.concat(combo_df_arr)
final_laptops_df['upc_store'] = final_laptops_df['upc'].str.cat(final_laptops_df['store'], sep = '_')
final_laptops_df = final_laptops_df.dropna(subset=['upc'])

final_laptops_df.head()


final_laptops_df.to_sql(name="both_laptops", con=engine, if_exists='replace', index=False)


with engine.connect() as con:
    try:
        con.execute('ALTER TABLE bestbuy_laptops ADD CONSTRAINT primary_key1 PRIMARY KEY(upc);')
    except:
        print('bb already done')
    try:
        con.execute('ALTER TABLE frys_laptops ADD CONSTRAINT primary_key2 PRIMARY KEY(upc);')
    except:
        print('frys already done')
    try:
        con.execute('ALTER TABLE both_laptops ADD CONSTRAINT primary_key3 PRIMARY KEY(upc_store);')
    except:
        print('both already done')
        
# VALIDATE TABLE BUILD:
engine.table_names()


# In[ ]:




