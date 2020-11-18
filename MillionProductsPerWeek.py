from ebaysdk.shopping import Connection as shopping
from ebaysdk.finding import Connection as finding
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup 
import pandas
import requests
import time
import os

#insert your appid,devid,and certid

apiF_2 = finding(appid = "", devid = '', certid = '', config_file = None) #Ebay only allows 5000 API calls per day, but you can easily create multiple accounts
apiF = finding(appid = "", devid = '', certid = '', config_file = None)

SCRAPE_LIST = []
KEYWORDS = []
STORES = []
MIN_PRICE = 25.00
DATASAVER = 0
LIST_DF = [] 
INTERNET = True 
MAX_THREADS = 30 
MASTER_KEYS = ['UPC', 'URL', 'Price', 'Title', 'Condition', 'Feedback', 'Seller', 'Username', 'EAN', 'Timestamp', 'URLcheck']
CALLS = ['findItemsByKeywords','keywords']
API_CALL_COUNTER = 0

def initiateDataframe():
	rowsData = []
	x = pandas.DataFrame(rowsData,columns=['UPC', 'URL', 'Price', 'Condition', 'Feedback', 'Seller','Username', 'EAN', 'Timestamp', 'URLcheck'])
	x.to_csv('xX_MASTER_LIST_Xx.csv') #a simple pandas DataFrame is initiated. You could alter the name here

def urlExecutor(urls):
	global INTERNET
	print('urlExecutor')
	if len(urls) > 0:						
		print("Querying " + str(len(urls)) + " URLs for UPC codes")
		with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:    
			while INTERNET == False:
				try:
					print('bad internet')
					requests.get("http://google.com")
		        		INTERNET = True
				except:
					time.sleep(3)
					print('waited')

			executor.map(scrapeURL, urls)

def scrapeURL(url):
	global SCRAPE_LIST
	global INTERNET
	while INTERNET == False:
		try:
			print('bad internet')
			requests.get("http://google.com")
		        INTERNET = True
		except:
			time.sleep(3)
			print('waited')
	try:
		listing = requests.get(url)
		listingMush = BeautifulSoup(listing.content.decode('utf-8','ignore'), 'lxml')
		listingMushText = listingMush.get_text()
		if 'UPC' in listingMushText:
			start = listingMushText.index('UPC')
			potentialUPC = listingMushText[start+3:start+23].encode('utf-8').replace("\n", "").replace(":", "").strip() 
			if potentialUPC.isdigit():
				upc_col = potentialUPC
			else:
				upc_col = 'not a number'
		else:
			upc_col = 'not in text'

		if 'EAN' in listingMushText:
			start = listingMushText.index('EAN')
			potentialEAN = listingMushText[start+3:start+23].encode('utf-8').replace("\n", "").replace(":", "").strip()
			if potentialEAN.isdigit():
				ean_col = potentialEAN
			else:	
				ean_col = 'not a number' 
		else:
			ean_col = 'not in text'	

		row_data = [url,upc_col,ean_col,'yes']
		SCRAPE_LIST.append(row_data)
		print(SCRAPE_LIST.index(row_data))
		
		
	except:
		INTERNET = False
		print('URL unreached, possibly iffy connection')

def mapUPC(df):
	global SCRAPE_LIST
	print('mapping upc')
	SCRAPE_DF = pandas.DataFrame(SCRAPE_LIST,columns=['URL','UPC','EAN','URLcheck'])
	df2 = df.set_index('URL')
	df2.update(SCRAPE_DF.set_index('URL'))
	df = df2.reset_index().reindex(columns=df.columns)
	df = df.drop_duplicates(subset='URL', keep="first")
	cols = [c for c in df.columns if c.lower()[:4] != 'unna']
	df=df[cols]
	df.to_csv('xX_MASTER_LIST_Xx.csv')
	print('Successfully added UPCs and EANs to master and backup')



def upcExecutor(df):
	print('upcExecutor')
	upcs = df['UPC'].unique()					
	for i in range(0, len(upcs), 220):
		with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:	
			executor.map(getProductByUPC, upcs[i:i + 220])	



def getProductByUPC(upc):  
	print('upc search')
	rowsData = []
	try:
		productMush = BeautifulSoup(apiF.execute('findItemsByKeywords',{'keywords':upc,'outputSelector':['SellerInfo','StoreInfo','shippingServiceCost'],'itemFilter':{'name':'ListingType','value':'FixedPrice'}}).content, 'lxml')
		productsInfo = productMush.find_all('item')
		if len(productsInfo) > 0:
			for item in productsInfo:
				try:
					feedback = item.feedbackscore.string.encode("utf-8")
					try:
						shipping = item.shippingservicecost.string.encode("utf-8")
					except:
						shipping = 'unfound'
					try:
						seller = item.storename.string.encode("utf-8")
					except:
						seller = 'no store'
					user = item.sellerusername.string.encode("utf-8")
					price = item.convertedcurrentprice.string.encode("utf-8")		
					title = item.title.string.lower().encode("utf-8")
					url = item.viewitemurl.string.lower().encode("utf-8")
					pricePlus = price + shipping
					condition = item.conditionid.string.encode("utf-8")
					row = [upc, url, pricePlus, condition, title, feedback, seller, user, ' ', time.strftime("%Y%m%d%H%M"), 'no']
					rowsData.append(row)	
					print("Extracted product data in upcSearch")								
				except:
					pass
	except:
		print("No items found in getProductByUPC API return HTML, possible API call limit overrun")
		pass

	pgProductsDf = pandas.DataFrame(rowsData, columns=MASTER_KEYS)
	LIST_DF.append(pgProductsDf)			

def yoyoExecutor(keys):
	global CALLS #CALLS contains arguments that access the Ebay API
	global MIN_PRICE
	global apiF
	global apiF_2    
	for key in keys:			
		MIN_PRICE = 1.00 #for each keyword query, we reset the MIN_PRICE to $1, and then call the yoyo() function
		print(key)
		yoyo(CALLS[0],CALLS[1],key,apiF)
	for key in keys: #change this, I'm pretty sure I should have a counter to make sure I don't requery a keyword
		MIN_PRICE = 1.00 #but anyway, the point here is to just do the same thing but get extra API calls. You could rework this whole function
		print(key)	 #for basically unlimited API calls
		yoyo(CALLS[0],CALLS[1],key,apiF_2)
		
		
def yoyo(call,parameter,key,api):
	rowsData = [] #we clear our rows of data 
	global MIN_PRICE
	global DATASAVER
	global LIST_DF
	global API_CALL_COUNTER 
	totalentries = 0 									
	try:
		productMush = BeautifulSoup(api.execute(call,{parameter : key,'outputSelector':['SellerInfo','StoreInfo','shippingServiceCost'], 'sortOrder': 'PricePlusShippingLowest','itemFilter':[{'name':'ListingType','value':'FixedPrice'}, {'name' : 'MinPrice', 'value' : MIN_PRICE},{'name' : 'MaxPrice', 'value' : 150}]}).content, 'lxml')
		API_CALL_COUNTER += 1 #productMush is the soup of the html from an ebay url. 200 products are in that url. For more info above check the Ebay API
		productsInfo = productMush.find_all('item')
		totalentries = int(productMush.find('totalentries').text) 
		if len(productsInfo) > 0:
			for item in productsInfo: #there's lots of different ways to create listings on ebay, so below I've handled missing data with try and except
				try:
					try:
						feedback = item.feedbackscore.string.encode("utf-8")
					except:
						feedback = 'unfound'
						print('feedback unfound')
					try:
						shipping = item.shippingservicecost.string.encode("utf-8")
					except:
						shipping = 'unfound'
					try:
						seller = item.storename.string.encode("utf-8")
					except:
						seller = 'no store'
					try:
						user = item.sellerusername.string.encode("utf-8")
					except:
						user = 'unfound'
						print('user unfound')
					price = item.convertedcurrentprice.string.encode("utf-8")   					
					title = item.title.string.lower().encode("utf-8")
					url = item.viewitemurl.string.lower().encode("utf-8")
					try:
						if shipping == 'unfound':
							pricePlus = float(price)
							print(pricePlus)
						else:
							pricePlus = float(price) + float(shipping) #price alone doesnt mean much. We want price plus shipping
							print(pricePlus)
					except:
						print('heres the problem')
						pricePlus = price,shipping
					if pricePlus >= MIN_PRICE: #a very important line. This is how we paginate the thousands of results pages. We continually raise the MIN_PRICE
						MIN_PRICE = pricePlus + .5 #Raising the MIN_PRICE to the highest price product per page means no duplicates, plus we get all the listings
					try:
						condition = item.conditionid.string.encode("utf-8")
					except:
						condition = 'unfound'
					row = ['no upc', url, pricePlus, title, condition, feedback, seller, user, ' ', time.strftime("%Y%m%d%H%M"), ' ']  
					rowsData.append(row)    #now we're just adding a row of data to a list. This will be easy to turn into a DataFrame
					print("Extracted product data in yoyo")	                                             		
				except:
					pass
	except:
		print("No items found in yoyo, possible API call limit overrun")
		pass

	pgProductsDf = pandas.DataFrame(rowsData, columns=MASTER_KEYS) #now we create the DataFrame for this page of results
	LIST_DF.append(pgProductsDf)  #and then we add to a list of DataFrames, one DataFrame for each results page
	if (totalentries >= 200) and (API_CALL_COUNTER <= 5000): #if a page has less than 200 entries, its the last page of results. Also, we don't want to keep querying urls if we've hit the Ebay 5000 call limit
		yoyo(call,parameter,key,api) #but if we're not on the last page and we haven't hit the limit, we call yoyo() again except now the MIN_PRICE has been raised so its new results
	else:
		API_CALL_COUNTER = 0 #this will finish this function
			
def main():
	try:
		master = pandas.read_csv('xX_MASTER_LIST_Xx.csv') #important, you're gonna have to create a new directory if you want to do totally separate scrapes
		print("Successfully loaded dataframe") #the try and except here checks if you've already started a scrape, if not, it intitiates a DataFrame
	except:
		initiateDataframe()
		print("Initiated dataframe named 'xX_MASTER_LIST_Xx.csv'")
	print("Input a list of keywords separated by commas") #in the command line, you can type a list of queries separated by commas. For example, 'basketball shoes, iphone, cordless drill'
	#keyword_list = [str(x) for x in input().split(',')] #here you would add an input line
	global SCRAPE_LIST
	keyword_list = ['sacd','dvd audio','blu-ray audio'] #but the way I did it was I just altered this list here
	yoyoExecutor(keyword_list) #then yoyoexecutor is called. This function will run for days depending on various parameters
	if len(LIST_DF) != 0: #now we have a list of DataFrames. If you entered no keywords, you skip to the else below
		print('Adding new products to the master list')
		master = pandas.concat(LIST_DF) #simple concat
		master = master.drop_duplicates(subset='URL', keep="first") #there will be duplicates. Listings constantly change in price
		cols = [c for c in master.columns if c.lower()[:4] != 'unna'] #one of my issues with pandas shown here
		master=master[cols]
		master.to_csv('tempNOupcList.csv') #basically just a backup, useful for debugging
		print('Stored csv without upc codes, just in case')
	else:
		print('LIST_DF empty, loading master list')
		master = pandas.read_csv('xX_MASTER_LIST_Xx.csv') #read master list
		master = master.drop_duplicates(subset='URL', keep="first")
	list_url = master['URL'].loc[master['UPC'] == 'no upc'].unique() #gets all the listing page urls for which the API didn't return UPC codes, all of them
	urlExecutor(list_url) #now we go to get the UPC codes, as you can see, you can feel free to exit the program whenever you want. 
	mapUPC(master)
	
	

main()		
