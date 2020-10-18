from ebaysdk.shopping import Connection as shopping
from ebaysdk.finding import Connection as finding
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup 
import pandas
import requests
import time
import os

#insert your appid,devid,and certid

apiF_2 = finding(appid = "", devid = '', certid = '', config_file = None)
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
	x.to_csv('xX_MASTER_LIST_Xx.csv')

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
	global CALLS
	global MIN_PRICE
	global apiF
	global apiF_2    
	for key in keys:			
		MIN_PRICE = 1.00
		print(key)
		yoyo(CALLS[0],CALLS[1],key,apiF)
	for key in keys:
		MIN_PRICE = 1.00
		print(key)
		yoyo(CALLS[0],CALLS[1],key,apiF_2)
		
		
def yoyo(call,parameter,key,api):
	rowsData = []
	global MIN_PRICE
	global DATASAVER
	global LIST_DF
	global API_CALL_COUNTER 
	totalentries = 0										
	try:
		productMush = BeautifulSoup(api.execute(call,{parameter : key,'outputSelector':['SellerInfo','StoreInfo','shippingServiceCost'], 'sortOrder': 'PricePlusShippingLowest','itemFilter':[{'name':'ListingType','value':'FixedPrice'}, {'name' : 'MinPrice', 'value' : MIN_PRICE},{'name' : 'MaxPrice', 'value' : 150}]}).content, 'lxml')
		API_CALL_COUNTER += 1
		productsInfo = productMush.find_all('item')
		totalentries = int(productMush.find('totalentries').text)
		if len(productsInfo) > 0:
			for item in productsInfo:
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
							pricePlus = float(price) + float(shipping)
							print(pricePlus)
					except:
						print('heres the problem')
						pricePlus = price,shipping
					if pricePlus >= MIN_PRICE:
						MIN_PRICE = pricePlus + .5
					try:
						condition = item.conditionid.string.encode("utf-8")
					except:
						condition = 'unfound'
					row = ['no upc', url, pricePlus, title, condition, feedback, seller, user, ' ', time.strftime("%Y%m%d%H%M"), ' ']  
					rowsData.append(row)   
					print("Extracted product data in yoyo")	                                             		
				except:
					pass
	except:
		print("No items found in yoyo, possible API call limit overrun")
		pass

	pgProductsDf = pandas.DataFrame(rowsData, columns=MASTER_KEYS)
	LIST_DF.append(pgProductsDf)
	if (totalentries >= 200) and (API_CALL_COUNTER <= 5000):
		yoyo(call,parameter,key,api)
	else:
		API_CALL_COUNTER = 0	
			
def main():
	try:
		master = pandas.read_csv('xX_MASTER_LIST_Xx.csv')
		print("Successfully loaded dataframe")
	except:
		initiateDataframe()
		print("Initiated dataframe named 'xX_MASTER_LIST_Xx.csv'")
	print("Input a list of keywords separated by commas")
	#keyword_list = [str(x) for x in input().split(',')]
	global SCRAPE_LIST
	keyword_list = ['sacd','dvd audio','blu-ray audio']
	yoyoExecutor(keyword_list)
	if len(LIST_DF) != 0:
		print('Adding new products to the master list')
		master = pandas.concat(LIST_DF)
		master = master.drop_duplicates(subset='URL', keep="first")
		cols = [c for c in master.columns if c.lower()[:4] != 'unna']
		master=master[cols]
		master.to_csv('tempNOupcList.csv')
		print('Stored csv without upc codes, just in case')
	else:
		print('LIST_DF empty, loading master list')
		master = pandas.read_csv('xX_MASTER_LIST_Xx.csv')
		master = master.drop_duplicates(subset='URL', keep="first")
	list_url = master['URL'].loc[master['UPC'] == 'no upc'].unique()
	urlExecutor(list_url) 
	mapUPC(master)
	
	

main()		
