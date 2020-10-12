# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 23:16:53 2020

@author: arman
"""

#%%---Importing the necessary packages and modules---------
from urllib.request import Request, urlopen
import requests
from bs4 import BeautifulSoup
import re
import time        
import sys
from multiprocessing import Pool

#%%---Defining a function for recursively crawling webpages and collecting the necessary URLs---------
def crawlCollectUrl(url, regex_page = "(/en/apartments-for-sale/10257\?page=)\d+$", 
                    page_urls = set(), 
                    regex_apt = "(-bedroom/apartment-for-sale)", 
                    apt_urls = set(),
                    write_to_file: bool = False):
    print(url)
    page_urls.add(url)
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    bs = BeautifulSoup(webpage, 'html.parser')
    
    for link in bs.findAll('a', href = re.compile(regex_apt)):
        if 'href' in link.attrs:
            if link.attrs['href'] not in apt_urls:
                apt_urls.add(link['href'])
    
    for link in bs.select_one('nav#page_pagination').findAll('a', href = re.compile(regex_page)):
        if 'href' in link.attrs:
            if link.attrs['href'] not in page_urls:
                newPage = link.attrs['href']
                time.sleep(3)
                crawlCollectUrl(newPage, page_urls = page_urls, 
                                apt_urls = apt_urls, 
                                write_to_file = write_to_file)
    
    if write_to_file == True:
        
        page_urls_ = '\n'.join(page_urls)
        with open('../data/page_urls.txt','w') as f:
            f.write(page_urls_)
        
        apt_urls_ = '\n'.join(apt_urls)
        with open('../data/apt_urls.txt','w') as f:
            f.write(apt_urls_)
        
        return "URLs are extracted to the proper files."
    
    return page_urls, apt_urls

#%% Define scraping functions for collecting separate components
def scrapPrice(urlContent):
    ''' Function responsible for scraping the total price'''
    # Obtain the toal price of the apartment
    try:
        obj = urlContent.select_one('.col-12').select_one('.item-view-price').text.strip()
    except:
        obj = None
    return obj

def scrapListParams(urlContent):
    ''' Function responsible for scraping the apartment listing parameters'''
    # Obtain the toal price of the apartment
    try:
        obj = [i.text.strip() for i in urlContent.select('ul.item-view-list-params div.col-5 span')]
    except:
        obj = None
    return obj

def scrapViews(urlContent):
    ''' Function responsible for scraping the views'''
    try:
        obj = urlContent.select_one('.item-view-count').text.strip()
    except:
        obj = None
    return obj
    
def scrapListingDates(urlContent):
    ''' Function responsible for scraping the added adn edited dates'''
    try:
        obj = [i.text.replace('Added in','').replace('Edited in',''). strip() for i in urlContent.select('div.item-view-date div')]
    except:
        obj = None
    return obj

def scrapPriceParams(urlContent):
    ''' Function responsible for scraping the apartment price parameters'''
    # Obtain the toal price of the apartment
    try:
        obj = list(dict.fromkeys([i.text.strip() for i in urlContent.select('div.item-view-price-params span')]))
    except:
        obj = None
    return obj

def scrapFacilities(urlContent):
    ''' Function responsible for scraping the primary and additional facilities'''
    facilities = []
    additionalFacilities = []
    for item in urlContent.find_all('div', attrs={'class': 'facilities-item'}):
        facilityChecker = item.find('span', {"class":re.compile(r'item-view-facilities-icon')})
        additionalFacilityChecker = item.find('span', {"class":re.compile(r'item-view-additional-icon')})
        if facilityChecker != None:
            facilities.append(item.text)
        if additionalFacilityChecker != None:
            additionalFacilities.append(item.text)
    return facilities, additionalFacilities

def scrapLatitudeLongitude(urlContent):
    ''' Function responsible for scraping the geographical longitude and latitude'''
    try:
        obj = urlContent.select_one('div#yandex_map_item_view')
    except:
        obj = None
    return obj['data-lat'], obj['data-lng']

#%% Designing the main function
def scrapAptURL(url):
    
    # Print the input URL
    print(url)
    
    # Create an empty list for storing information
    aptInfo = []
    
    # Define initial values for the variables
    aptTotalPrice = '-'
    aptPricePerSqm, aptBath, aptBldgType, aptCeilingHeight, aptCondition = "-","-","-","-","-"
    aptAddedDate, aptEditedDate = "-","-"
    aptArea, aptNoOfRooms, AptFloor = "-","-","-"
    aptViews = "-"
    aptFacilities, aptAdditionalFacilities = "-","-"
    aptLat, aptLng = "-","-"

    # Extract the apartment ID
    aptID = url.split('/')[-1]
        
    # Extract the apartment street, district and region 
    aptStreet, aptDistrict, aptRegion  = url.split('/')[-4:-1]
    
    try:
        # Request the URL and parse it through an HTML parser
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read()
        page_content = BeautifulSoup(webpage, "html.parser")
    
        # Extract the total price of the apartment    
        if scrapPrice(page_content) is not None:
            aptTotalPrice = scrapPrice(page_content)
        
        # Extract the price  per square meter of the apartment
        if scrapListParams(page_content) is not None:
           aptPricePerSqm, aptBath, aptBldgType, aptCeilingHeight, aptCondition = scrapListParams(page_content)
        
        # Extract the added and the edited dates for the apartment
        if scrapListingDates(page_content) is not None:
            aptAddedDate, aptEditedDate = scrapListingDates(page_content)
        
        # Extract the area, number of rooms and the floors/storeys
        if scrapPriceParams(page_content) is not None:
            aptArea, aptNoOfRooms, AptFloor = scrapPriceParams(page_content)
        
        # Extract the apartment page views
        if scrapViews(page_content) is not None:
            aptViews = scrapViews(page_content)

        # Extract the information about primary and additional facilities
        if scrapFacilities(page_content) is not None:
            aptFacilities, aptAdditionalFacilities = scrapFacilities(page_content)
        
        # Extract the information about the geographical latiitude & longitude of apartment
        if scrapLatitudeLongitude(page_content) is not None:
            aptLat, aptLng = scrapLatitudeLongitude(page_content)
        
        # Append the results to the list
        aptInfo.extend([aptID, aptTotalPrice,aptPricePerSqm, 
                        aptBath, aptBldgType, aptCeilingHeight,
                        aptArea, aptNoOfRooms, AptFloor,
                        aptCondition, aptStreet, aptDistrict, aptRegion,
                        aptFacilities, aptAdditionalFacilities,
                        aptViews, aptAddedDate, aptEditedDate,
                        aptLat, aptLng,url])
    except Exception as ex:
        print(str(ex))
        faultyUrls = open('../data/faultyURLs.txt', 'a+')
        faultyUrls.write(url)
        faultyUrls.write('\n')
    finally:
        if len(aptInfo) > 0:
            # Open a file for writing the data
            f = open('../data/scrapResult.txt', 'a+')
            f.write(';;'.join([str(i) for i in aptInfo]))
            f.write('\n')
            f.close()
        else:
            return None

#%% Import the apartment links and distinguish the unique ones
if __name__ == "__main__":
    
    # Running the crawlCollectUrl
    crawlCollectUrl("https://myrealty.am/en/apartments-for-sale/10257?page=1", write_to_file = True)

    # Print out the arguments passed via command line
    print('Argument(s) passed: {}'.format(str(sys.argv)))
    
    # Import the apartment links and distinguish the unique ones
    lines = list(set([i.strip("\n") for i in open("../data/apt_urls.txt", "r").readlines()]))
    
    # Sort the list
    lines.sort()

    # Use command line arguments for running small-size experiments
    if len(sys.argv) == 2:
        lines = lines[0:int(sys.argv[1])]
    elif len(sys.argv)==3:
        lines = lines[int(sys.argv[1]):int(sys.argv[2])]

    # Initiate a multiprocessing pool
    p = Pool(5)  # Pool tells how many at a time
    records = p.map(scrapAptURL, lines)
    p.terminate()
    p.join()