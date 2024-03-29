# UPC-s-and-the-Ebay-API-for-a-Million-Dollar-Heist

Warning: this script is now broken for two reasons. One is that it used the old version of the Ebay API's. There's a link below to fix this. And two, it's a crawler, and like any crawler, falls victim to changes to a website's layout.

This was my first major foray into python. It's a parallelized web scraper with an auto saving database. I managed to make it go pretty fast, grabbing a combination of data using the Ebay API directly, as well as some basic html scraping for the UPC codes. These UPC codes are very valuable - there are databases online that can cost thousands of dollars over time but this is a pretty good way around that. I think I could build this database up to the same size as any other online database using code pretty similar to this. Below is a gif of the file where I saved all the product data:

![alt-text](https://github.com/kelmensonj/UPC-s-and-the-Ebay-API-for-a-Million-Dollar-Heist/blob/master/libre_upc.gif)

There are some dependencies for this script. For Ubuntu 21.04, I did:

```
pip3 install BeautifulSoup4
pip3 install pandas
```

Then, you would need to use this link to get the Ebay API's: https://github.com/timotheus/ebaysdk-python

A few thing this crawler did:

* Integrates Ebay Shopping and Finding API's with Beautiful Soup and the Requests library in order to get supplemental data
* Collects URL's, Prices, Listing Titles, Conditions, Sellers, Seller Feedback, Seller Usernames using the Ebay API's Ebay API's block access to UPC's and EAN's (universal product codes), this python script uses a threadpool executor in order to make over a million URL requests per week, and scrapes URL for UPC's and EAN's
* All information is autosaved to a csv file at a variable interval. The database is built using pandas and will update whatever information is found.
* You can update the database by querying keywords, UPC codes, sellers, etc. Anything you might type in within the Ebay.com search function can be queried.
* Built a database of well over 40,000 unique UPC codes each with data on at least 3 different listings. Successfully identified multiple underpriced Ebay listings using this information - the listings were bought and flipped for profit.

Here's an extended video if you're interested: https://www.youtube.com/watch?v=FU7LTY4gWtE&t=6s
