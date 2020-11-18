#################################xX_EBAY_Xx.py#############################################################################################################################################
#################################James Kelmenson###########################################################################################################################################
#################################Early 2020################################################################################################################################################
###########################################################################################################################################################################################
#add in title, add in pricePlus converted to one num, delete unnamed columns**********&&&&&&&&&&&&&&@@@@@@@@@@@$$$$$$$$$$$$$$$$$$ iiterate over total entries divided by 100
from ebaysdk.shopping import Connection as shopping
from ebaysdk.finding import Connection as finding
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import pandas
import requests
import time
import os

################################## ^^^^^^ IMPORTS ^^^^^^ #####################################################################################################################################

apiF_2 = finding(appid = "", devid = '', certid = '', config_file = None)
apiF = finding(appid = "", devid = '', certid = '', config_file = None)

################################## ^^^^^^ API KEY ^^^^^^ #####################################################################################################################################
LIST_STORES = ['World of Books USA']
SCRAPE_LIST = []
KEYWORDS = []
#KEYWORDS = ['bluetooth', 'monitor','keyboard', 'scanner', 'printer', 'mouse', 'video game', 'sacd', 'controller', 'console', 'nerf', 'kids toy', 'barbie', 'action figure', 'marvel','dc', 'star wars', 'sony', 'apple', 'bosch', 'guess', 'canon', 'samsung', 'razer', 'makita', 'ridgid', 'panasonic'] #that's 2763 api calls about
STORES = []
MIN_PRICE = 25.00
DATASAVER = 0
#KEYWORDS = ['dewalt']
#STORES = ['Car Parts Wholesale','Zoro Tools','vipoutlet','ForeverLux','directimports1899']
#KEYWORDS = ['tablet','speakers', 'sound', 'glasses', 'watch', 'part', 'rc', 'drone', 'remote', 'exercise', 'tech', 'accessory', 'sport', 'health', 'beauty', 'protein', 'pet', 'children']
#KEYWORDS = ['sacd', 'appliance', 'camera', 'toy', 'kitchen', 'bluetooth', 'game', 'dog', 'jacket', 'tv', 'exercise', 'video game', 'silverware', 'food'] #KEYWORDS holds a list of keywords such as sacd, dvd audio, blu ray audio, appliance, etc
UPC_URL_TUP = [] #Global UPC tuple holding web scraped upc codes associated with urls, housed outside functions in case of errors
EAN_URL_TUP = [] #Global UPC tuple holding web scraped upc codes associated with urls, housed outside functions in case of errors
LIST_DF = [] #Global list of dataframes; each dataframe contains a pg of 100 product listings. This list is cleared often
INTERNET = True #Boolean that will set to 'iffy' if for any reason a url cant be accessed
MAX_THREADS = 30 #MAX_THREADS assigns the number of workers to threadPoolExecutor
MASTER_KEYS = ['UPC', 'URL', 'Price', 'Title', 'Condition', 'Feedback', 'Seller', 'Username', 'EAN', 'Timestamp', 'URLcheck'] #Columns in xX_MASTER_LIST_Xx.csv

################################# ^^^^^^ GLOBAL VARIABLES AND LISTS ^^^^^^ ####################################################################################################################

def keyPgExecutor():
	keyPgTupList = []
	if len(KEYWORDS) > 0:
		for key in KEYWORDS:
			for pg in range(1,100):						#keyPgExecutor creates a list of tuples that can be mapped to eBay's Finding API
				tup = ((key,str(pg)))							
				keyPgTupList.append(tup)
		with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:			#asynchonous execution using ThreadPoolExecutor
			executor.map(keywordSearch,keyPgTupList)
	else:
		pass

################################# ^^^^^^ FIND ITEMS BY KEYWORDS THREADER ^^^^^^ ###############################################################################################################

def keywordSearch(keyPgTup): 
	rowsData = []
	key = keyPgTup[0]
	pg = keyPgTup[1]											#store, pg are entries in the mapped tuple list from storePgExecutor
	try:
		productMush = BeautifulSoup(apiF.execute('findItemsByKeywords',{'keywords':key,'outputSelector':['SellerInfo','StoreInfo','shippingServiceCost'],'itemFilter':[{'name':'ListingType','value':'FixedPrice'},{'name' : 'MinPrice', 'value' : 25.00}],'paginationInput':{'pageNumber':str(pg)}}).content, 'lxml')
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
					price = item.convertedcurrentprice.string.encode("utf-8")						#feedback through price are variable names assigned to html
					title = item.title.string.lower().encode("utf-8")
					url = item.viewitemurl.string.lower().encode("utf-8")
					pricePlus = price + shipping
					condition = item.conditionid.string.encode("utf-8")
					row = ['no upc', url, pricePlus, title, condition, feedback, seller, user, ' ', time.strftime("%Y%m%d%H%M"), ' ']  
					rowsData.append(row)	
					print("Extracted product data in keywordSearch")						#each row of data is one product, and is appended to rowsData
				except:
					pass
	except:
		print("No items found in keywordSearch API return HTML, possible API call limit overrun")
		pass

	pgProductsDf = pandas.DataFrame(rowsData, columns=MASTER_KEYS)
	LIST_DF.append(pgProductsDf)										#LIST_DF holds the dataframe objects of rowsData for 100 pages

################################# ^^^^^^ FIND ITEMS BY KEYWORDS ACCESS POINT ^^^^^^ #########################################################################################################

def storePgExecutor():
	storePgTupList = []
	if len(STORES) > 0:
		for store in STORES:
			for pg in range(1,101):
				tup = ((store,str(pg)))						#keyPgExecutor creates a list of tuples that can be mapped to eBay's Finding API
				storePgTupList.append(tup)
		with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:			#asynchonous execution using ThreadPoolExecutor
			executor.map(storeSearch,storePgTupList) 	
	else:
		pass			

################################# ^^^^^^ FIND ITEMS IN EBAY STORES THREADER ^^^^^^ ##########################################################################################################

def storeSearch(storePgTup): 
	rowsData = []
	store = storePgTup[0]									
	pg = storePgTup[1]											#store, pg are entries in the mapped tuple list from storePgExecutor
	try:
		productMush = BeautifulSoup(apiF.execute('findItemsIneBayStores',{'storeName' : store,'outputSelector':['SellerInfo','StoreInfo','shippingServiceCost'], 'itemFilter':[{'name':'ListingType','value':'FixedPrice'},{'name' : 'MinPrice', 'value' : 25.00}], 'paginationInput' : {'pageNumber': str(pg)}}).content, 'lxml')
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
					price = item.convertedcurrentprice.string.encode("utf-8")   					#feedback through price are variable names assigned to html
					title = item.title.string.lower().encode("utf-8")
					url = item.viewitemurl.string.lower().encode("utf-8")
					pricePlus = price + shipping
					condition = item.conditionid.string.encode("utf-8")
					row = ['no upc', url, pricePlus, title, condition, feedback, seller, user, ' ', time.strftime("%Y%m%d%H%M"), ' ']
					rowsData.append(row)   
					print("Extracted product data in storeSearch")	                                             		#each row of data is one product, and is appended to rowsData
				except:
					pass
	except:
		print("No items found in store Search API return HTML, possible API call limit overrun")
		pass

	pgProductsDf = pandas.DataFrame(rowsData, columns=MASTER_KEYS)
	LIST_DF.append(pgProductsDf)										#LIST_DF holds the dataframe objects of rowsData for 100 pages

################################# ^^^^^^ FIND ITEMS IN EBAY STORES ACCESS POINT ^^^^^^ #####################################################################################################
		
def urlExecutor(urls):
	global INTERNET
	print('urlExecutor')
	if len(urls) > 0:							#urlExecutor executes until download_url hits an exception
		print("Querying " + str(len(urls)) + " URLs for UPC codes")
		with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:    #asynchonous execution using ThreadPoolExecutor
			while INTERNET == False:
				try:
					print('bad internet')
					requests.get("http://google.com")
		        		INTERNET = True
				except:
					time.sleep(3)
					print('waited')

			executor.map(scrapeURL, urls)

################################# ^^^^^^ REQUESTS THREADER ^^^^^^ ##########################################################################################################################

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

################################# ^^^^^^ HTTPS ACCESS POINT ^^^^^^ #########################################################################################################################

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

################################# ^^^^^^ UPC TO DATABASE ACCESS POINT ^^^^^^ ##############################################################################################################

def upcExecutor(df):
	print('upcExecutor')	#this function takes all upcs, but really, it needs to take those upcs that havent been queried in awhile or that dont have a high count. Prob that havent been queried
	upcs = df['UPC'].unique()					
	for i in range(0, len(upcs), 220):
		with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:	
			executor.map(getProductByUPC, upcs[i:i + 220])	

################################# ^^^^^^ FIND ITEMS BY UPC CODE THREADER ^^^^^^ ###########################################################################################################

def getProductByUPC(upc):  # this function isnt really useful because api calls matter. So instead I should scrape various websites based on google searching the upc code in order to populate
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
					price = item.convertedcurrentprice.string.encode("utf-8")		#feedback through price are variable names assigned to html
					title = item.title.string.lower().encode("utf-8")
					url = item.viewitemurl.string.lower().encode("utf-8")
					pricePlus = price + shipping
					condition = item.conditionid.string.encode("utf-8")
					row = [upc, url, pricePlus, condition, title, feedback, seller, user, ' ', time.strftime("%Y%m%d%H%M"), 'no']
					rowsData.append(row)	
					print("Extracted product data in upcSearch")								#each row of data is one product, and is appended to rowsData
				except:
					pass
	except:
		print("No items found in getProductByUPC API return HTML, possible API call limit overrun")
		pass

	pgProductsDf = pandas.DataFrame(rowsData, columns=MASTER_KEYS)
	LIST_DF.append(pgProductsDf)										#LIST_DF holds the dataframe objects of rowsData for 100 pages
		
################################# ^^^^^^ FIND ITEMS BY UPC ACCESS POINT ^^^^^^ ############################################################################################################

def fairPrices(df_og):
	df_stats = df_stats.loc[(df_stats['UPC'].map(checkEntry)) | (df_stats['EAN'].map(checkEntry))]
	df_stats = df_stats.loc[df_stats['Price'].map(checkEntryPrice)]
	df_stats["Price"] = pandas.to_numeric(df_stats["Price"])
	meanPriceDF = pandas.DataFrame(df_stats.groupby(['UPC']).Price.mean().reset_index())
	meanPriceDF.rename(columns={'Price':'MEAN'},inplace=True)
	stdPriceDF = pandas.DataFrame(df_stats.groupby(['UPC']).Price.std().reset_index())
	stdPriceDF.rename(columns={'Price':'STD'},inplace=True)
	minPriceDF = pandas.DataFrame(df_stats.groupby(['UPC']).Price.min().reset_index())
	minPriceDF.rename(columns={'Price':'MIN'},inplace=True)
	countPriceDF = pandas.DataFrame(df_stats.groupby(['UPC']).Price.count().reset_index())
	countPriceDF.rename(columns={'Price':'COUNT'},inplace=True)
	listDF = [meanPriceDF,stdPriceDF,minPriceDF,countPriceDF] 
	df_stats = reduce(lambda left,right: pandas.merge(left, right, on=['UPC'], how='left'), listDF) #idea in this function is to calculate fair prices per UPC. This way we can search by UPC and if its below the fair price, buy the lsiting
	outlierTrusted(df_og, df_stats)

def getMoreExamples(df):
	df = df['UPC'].value_counts()[df['UPC'].value_counts()<50] #this doesnt work but the idea is to focus in on products where the pricing data sample size is low
	return df.index.tolist() #if a given UPC has 50 examples, we don't need more data. But if a UPC has only one row of data, that's kind of useless data
  
def checkEntry(entry): #a bunch of data cleaning methods around here
	return len(str(entry)) >= 8

def checkEntryPrice(entry):
	return type(entry) == float or type(entry) == int

def outlierTrusted(df_og,df_stats):
	list_leads = []
	for upc in df_og['UPC']:
		this_price = df_og['Price'].loc[df_og['UPC'] == upc]
		fair_price = df_stats['MEAN'].loc[df_stats['UPC'] == upc] #so basically we're just giving an example where the fair price is the average price for a given UPC in our dataframe
		if (this_price + 10 < fair_price) and (df_og['Feedback'].loc[(df_og['UPC'] == upc) & (df_og['Price'] == this_price)] > 5000): #we check for prices $10 lower than the average, and where the seller has 5000 feedback or greater
			list_leads.append(df_og['URL'].loc[(df_og['UPC'] == upc) & (df_og['Price'] == this_price)]) #if the criteria is met, that's a solid lead ready to be looked at by a person

def yoyoExecutor():
	CALLS = ['findItemsIneBayStores','storeName','findItemsByKeywords','keywords']
	#keys = ['canon','makita','nerf', 'kids toy', 'barbie', 'marvel','dc', 'star wars']
	#keys = ['ridgid', 'bosch','kitchen']
	#keys = ['action figure']
	#keys = ['mattel']
	#keys = ['controller','hasbro','headphones','dvd','blu ray audio','keurig','appliance','kitchen','vaccuum','clippers','shaver','monitor','tv','basketball shoes','drill','saw','printer','scanner','station']
	#keys = ['toaster','microwave','game','case','ear phones','sports','batting gloves','lamp','animal','pet','furniture']
	#keys = ['sacd','dvd audio', 'blu ray audio', 'model plane','cleats','boots','shoes','superman','batman','captain america', 'american girl', 'cabbage patch']
	#keys = ['cell phone','droid','radio','motor','robot','electric saw']
	#master = pandas.read_csv('xX_MASTER_LIST_Xx.csv')
	#keys = master['UPC'].unique()[70000:]
	df = pandas.read_csv('xX_MASTER_LIST_Xx.csv') #i have all the lines commented out above because this was how I kept track of what keywords I'd queried
	df = df['UPC'].value_counts()[df['UPC'].value_counts()<30]
	keys = df.index.tolist()
	print(len(keys))
	global MIN_PRICE
	global apiF
	global apiF_2    
	for key in keys[:5001]:	 #this i can do asynchronous			
		MIN_PRICE = 1.00
		print(key)
		yoyo(CALLS[2],CALLS[3],key,apiF)
	for key in keys[5001:100005]: #this little snippet is how i use additional API calls, past the 5000 limit. I simply swap into a new account and continue making calls where I left off
		MIN_PRICE = 1.00
		print(key)
		yoyo(CALLS[2],CALLS[3],key,apiF_2)

def yoyo(call,parameter,key,api):
	rowsData = []
	global MIN_PRICE
	global DATASAVER
	global LIST_DF
	totalentries = 0											#store, pg are entries in the mapped tuple list from storePgExecutor
	try:
		productMush = BeautifulSoup(api.execute(call,{parameter : key,'outputSelector':['SellerInfo','StoreInfo','shippingServiceCost'], 'sortOrder': 'PricePlusShippingLowest','itemFilter':[{'name':'ListingType','value':'FixedPrice'}, {'name' : 'MinPrice', 'value' : MIN_PRICE},{'name' : 'MaxPrice', 'value' : 150}]}).content, 'lxml')
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
					price = item.convertedcurrentprice.string.encode("utf-8")   					#feedback through price are variable names assigned to html
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
					if pricePlus >= MIN_PRICE: #important line here. Let's you call the yoyo() function again witht he same parameters, but now the Output Selector argument will limit results to higher prices
						MIN_PRICE = pricePlus + .5 #it will limit results to those results higher priced than the highest priced product from this page now
					try:
						condition = item.conditionid.string.encode("utf-8")
					except:
						condition = 'unfound'
					row = [key, url, pricePlus, title, condition, feedback, seller, user, ' ', time.strftime("%Y%m%d%H%M"), ' ']  #there are entries in upc col with no urlcheck
					#row = ['no upc', url, pricePlus, title, condition, feedback, seller, user, ' ', time.strftime("%Y%m%d%H%M"), ' ']
					rowsData.append(row)   
					print("Extracted product data in yoyo")	                                             		#each row of data is one product, and is appended to rowsData
				except:
					pass
	except:
		print("No items found in yoyo, possible API call limit overrun")
		pass

	pgProductsDf = pandas.DataFrame(rowsData, columns=MASTER_KEYS)
	LIST_DF.append(pgProductsDf)
	if (DATASAVER / 500) == 1: #every 500 pages, you will save your data. so you won't miss out on much if you close the terminal
		print('Adding new products to the master list')
		new_products_DF = pandas.concat(LIST_DF)
		master = pandas.read_csv('xX_MASTER_LIST_Xx.csv') #create copy of file object tagged zZ_MASTER_LIST_Zz.csv
		master = pandas.concat([master, new_products_DF])
		master = master.drop_duplicates(subset='URL', keep="first")
		cols = [c for c in master.columns if c.lower()[:4] != 'unna']
		master=master[cols]
		master.to_csv('xX_MASTER_LIST_Xx.csv')
		print('Stored 1000 products in master list')
		LIST_DF = [] 
		DATASAVER = 0
	else:
		DATASAVER += 1
	if totalentries >= 200: #then you check to see if the page was the last page. The last page for a given keyword has fewer than 200 entries always
		yoyo(call,parameter,key,api)		

		
def main():
	global SCRAPE_LIST
	yoyoExecutor()
	#keyPgExecutor()
	#storePgExecutor()
	if len(LIST_DF) != 0:
		print('Adding new products to the master list')
		new_products_DF = pandas.concat(LIST_DF)
		master = pandas.read_csv('xX_MASTER_LIST_Xx.csv') #create copy of file object tagged zZ_MASTER_LIST_Zz.csv
		master = pandas.concat([master, new_products_DF])
		master = master.drop_duplicates(subset='URL', keep="first")
		cols = [c for c in master.columns if c.lower()[:4] != 'unna']
		master=master[cols]
		master.to_csv('xX_MASTER_LIST_Xx.csv')
		master.to_csv(time.strftime("%Y%m%d%H%M%S") + 'z_BACKUP_z.csv')
		print('Stored csv without upc codes, just in case')
	else:
		print('LIST_DF empty, loading master list')
		master = pandas.read_csv('xX_MASTER_LIST_Xx.csv')
		master = master.drop_duplicates(subset='URL', keep="first")
	list_url = master['URL'].loc[master['UPC'] == 'no upc'].unique()
	urlExecutor(list_url) 
	mapUPC(master)
	#getMoreExamples(master)

	
	

main()


#this is basically all self commenting code. The process in main goes like this:
#there are various executor functions depending on your needs. You can search for samples of results by stores or keyword, or, with yoyoexecutor, get full results for keywords
#all will load data in LIST_DF and eventually into your master dataframe
#next, if you've scraped data, you add it to the master list. If you havent inputted keywords or stores, you check for listings that havent yet been scraped for additional dat
#mainly, the additional data is the UPC. UPC's are blocked by Ebay's API but you can get them with Beautiful Soup
#finally, the UPC's are mapped into the master list
#getMoreExamples is to narrow down your next scrape to just those UPC's for which you have very few examples









