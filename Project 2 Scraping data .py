#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from matplotlib import pyplot as plt
get_ipython().system('pip install mysql-connector-python')
from sqlalchemy import create_engine


# In[2]:


# Set the base URL of the page you want to scrape
# The page number and other filters (if any) will be added dynamically in the loop

products = []

# Loop through 100 pages and extract product names and prices
for i in range(1,101):
    # Build the URL for the current page
    url = f'https://www.newegg.com/GPUs-Video-Graphics-Cards/SubCategory/ID-48/Page-{i}'
    # Send a GET request to the page
    page = requests.get(url)

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(page.content, "html.parser")

    # Find all product containers on the page
    product_containers = soup.find_all("div", class_="item-cell")

    # Extract relevant information from each product container and store it in a dictionary
    for container in product_containers:
     
        # get product id
        product_id = container.find('a', {'class': 'item-title'})['href'].split("/")[-1].split('?')[0]
        
        # get name
        name = container.find("a", class_="item-title").text.strip()
        
        # get brand
        brand = container.find("a", class_="item-title").text.strip().split()[0]
        
        # get price
        price_raw = container.find('li', {'class': 'price-current'}).text.strip()
        price_pattern = r'\$[\d,]+\.\d{2}'
        price_match = re.search(price_pattern, price_raw)
        if price_match:
            price = price_match.group(0)    
        
        # get shipping 
        shipping_raw = container.find('li', {'class': 'price-ship'}).text.strip()
        if  shipping_raw == 'Free Shipping' or shipping_raw == 'Special' :
            shipping = '0'
        else:
            shipping_pattern = r'\$[\d,]+\.\d{2}'
            shipping_match = re.search(shipping_pattern, shipping_raw)
            if shipping_match:
                shipping = shipping_match.group(0)
      
        # get total price
        price_float = float(price.replace('$', '').replace(',', ''))
        shipping_float = float(shipping.replace('$', '').replace(',', ''))
        total_price = price_float + shipping_float
     
        
        # get img_url
        img_url = container.find('a', {'class': 'item-img'})['href']
           
        # get rating
        rating_container = soup.find("a", class_="item-rating")
        if rating_container is not None:
            rating = rating_container.get("title").split()[2]
        else:
            rating = "N/A"
        
        # get number of rating
        rating_num_container = soup.find("span", class_="item-rating-num")
        if rating_num_container is not None:
            rating_num = rating_num_container.get_text(strip=True).strip("()")
        else:
            rating_num = "N/A"
            
        #get detail product
        item_features = soup.find('ul', class_='item-features')
        features = soup.find_all('li')

        for feature in features:
            strong_tag = feature.find('strong')
            if strong_tag:
                if strong_tag.text == 'Max Resolution:':
                    max_resolution = feature.text.replace('Max Resolution:', '').strip()
                elif strong_tag.text == 'DisplayPort:':
                    display_port = feature.text.replace('DisplayPort:', '').strip()
                elif strong_tag.text == 'HDMI:':
                    hdmi = feature.text.replace('HDMI:', '').strip()
                elif strong_tag.text == 'DirectX:':
                    directx = feature.text.replace('DirectX:', '').strip()
                elif strong_tag.text == 'Model #: ':
                    model = feature.text.replace('Model #: ', '').strip()
        # create a dictionary to store the data
        item = {'Product_id': product_id,'Product_name': name,'Brand':brand,'Price':price_float,'Shipping_fee':shipping_float,
                'Total_payment':total_price,'Img_url':img_url,'Rating':rating,'Rating num':rating_num,
                'Max_resolution': max_resolution,'Display_port': display_port,'HDMI':hdmi,'DirectX':directx,'Model':model
                
                
               }
        # add the dictionary to the list
        products.append(item)
          
    # Print a message after each page is processed to track the progress
df = pd.DataFrame(products, columns=['Product_id','Product_name','Brand', 'Price', 'Shipping_fee','Total_payment',
                                      'Img_url','Rating','Rating_num',
                                     'Max_resolution','Display_port','HDMI','DirectX','Model'
                                     ])
df

# handle viec xu ly loi


# In[3]:


df.to_csv('namnguyenver.csv', index=False)
# After all pages have been processed, print the final list of products
# Print confirmation message
print('Data exported to final_data1.csv')


# In[4]:


import mysql.connector

mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  password="Huong6291!"
)
print(mydb)


# In[5]:


# Create a new database
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE project2")


# In[6]:


# Create a new table
mycursor.execute("CREATE TABLE project2.homework (Product_id VARCHAR(100),Product_name VARCHAR(200),Brand VARCHAR(45), Price INT,Shipping_fee INT,Total_payment VARCHAR(45),Img_url VARCHAR(200),Rating VARCHAR(45),Rating_num VARCHAR(100),Max_resolution VARCHAR(100),Display_port VARCHAR(45),HDMI VARCHAR(100),DirectX VARCHAR(45),Model VARCHAR(45))")


# In[7]:


from sqlalchemy import create_engine
engine = create_engine("mysql+mysqldb://root:Huong6291!@127.0.0.1/project2") #fill details
df.to_sql('homework',engine,if_exists='append',index = False)


# In[8]:


# Các hãng đang cung cấp Card đồ họa, số lượng sản phẩm của mỗi hãng. 
product_count = df['Brand'].value_counts()
plt.bar(product_count.index, product_count.values)
plt.title('Number of products sold by brand')
plt.xlabel('Brand')
plt.ylabel('Number of products')
plt.show()


# In[9]:


## top 10 best brand best selling
product_count_10 = df['Brand'].value_counts().head(10)
plt.bar(product_count_10.index, product_count_10.values)
plt.title('Number of products sold by top 10 brand')
plt.xlabel('Brand')
plt.ylabel('Number of products')
plt.show()


# In[10]:


## Phân bố giá sản phẩm theo hãng
# get top 10 brands by sales count
top_brands = df['Brand'].value_counts().nlargest(10).index.tolist()
df_top_brands = df[df['Brand'].isin(top_brands)]
grouped = df_top_brands.groupby('Brand')
mean_prices = grouped.mean()['Price']
# plot bar chart
plt.bar(mean_prices.index, mean_prices.values)
plt.title("Average price by top 10 brands")
plt.xlabel("Brand")
plt.ylabel("Average price")
plt.show()


# In[11]:


# plot scatter chart
plt.scatter(df_top_brands['Brand'], df_top_brands['Price'])
plt.title('Price of products by brand (top 10 best selling brands)')
plt.xlabel('Brand')
plt.ylabel('Price')
plt.show()


# In[12]:


## Phân bố giá của các sản phẩm, mức giá phổ biến
product = df['Price']
product.plot(kind = 'hist')
plt.title('The distribution of price')
plt.xlabel('Price')


# In[13]:


# Biểu diễn mối liên hệ giữa giá sản phẩm và rating của người dùng
df.plot(kind = 'scatter',x='Price',y='Rating')
plt.show()

