from bs4 import BeautifulSoup,SoupStrainer
import requests
from lxml import etree
import math
import pandas as pd

"""
NOTE: File name without csv
For Example : new_car_url_a.csv
Then your input file Name : new_car_url_a
"""

# Define your file name here (Read note before define file name)
file_name = "new_car_url_d"

country_list = ["D","A","B","E","F","I","L","NL"]
brand_list = ["audi","bmw","ford","mercedes-benz","opel","volkswagen","renault","9ff","abarth","ac","acm","acura","aiways","aixam","alba-mobility","alfa-romeo","alpina","alpine","amphicar","angelelli-automobili","ariel-motor","artega","aspark","aspid","aston-martin","aurus","austin","austin-healey","autobianchi","baic","bedford","bellier","bentley","boldmen","bolloré","borgward","brilliance","bristol","brute","bugatti","buick","byd","cadillac","caravans-wohnm","carver","casalini","caterham","cenntro","changhe","chatenet","chery","chevrolet","chrysler","cirelli","citroen","cityel","corvette","cupra","dacia","daewoo","daf","daihatsu","daimler","dallara","dangel","de-la-chapelle","de-tomaso","delorean","devinci-cars","dfsk","dodge","donkervoort","dr-automobiles","ds-automobiles","dutton","e.go","econelo","edran","elaris","embuggy","emc","emc","estrima","evetta","evo","ferrari","fiat","fisker","forthing","foton","gac-gonow","galloper","gappy","gaz","gem","gemballa","genesis","giana","gillet","giotti-victoria","gmc","goupil","great-wall","grecav","gta","gwm","haima","hamann","haval","hiphi","holden","honda","hongqi","hongqi","hummer","hurtan","hyundai","ineos","infiniti","innocenti","iso-rivolta","isuzu","iveco","izh","jac","jaguar","jeep","jensen","karma","kg-mobility","kia","koenigsegg","ktm","lada","lamborghini","lancia","land-rover","ldv","levc","lexus","lifan","ligier","lincoln","linzda","lorinser","lotus","lucid","m-ero","mahindra","man","mansory","martin","martin-motors","maserati","matra","maxus","maybach","mazda","mclaren","mega","melex","mercury","mg","micro","microcar","militem","minari","minauto","mini","mitsubishi","mitsuoka","morgan","moskvich","mp-lafer","mpm-motors","nio","nissan","nsu","oldsmobile","oldtimer","omoda","ora","pagani","panther-westwinds","peugeot","pgo","piaggio","plymouth","polestar","pontiac","porsche","proton","puch","ram","regis","reliant","rolls-royce","rover","ruf","saab","santana","seat","segway","selvo","seres","sevic","sgs","shelby","shuanghuan","silence","singer","skoda","skywell","smart","speedart","sportequipe","spyker","ssangyong","streetscooter","studebaker","subaru","suzuki","talbot","tasso","tata","tazzari-ev","techart","tesla","togg","town-life","toyota","trabant","trailer-anhänger","triumph","trucks-lkw","tvr","uaz","vanden-plas","vanderhall","vaz","vem","vinfast","volvo","wartburg","weltmeister","wenckstern","westfield","wey","wiesmann","xbus","xev","xpeng","zastava","zaz","zeekr","zhidou","zotye","others"]

registeryr_list = ["1900-1930","1931-1960","1961-1990","1991-1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","2023","2024"]
price_range_list = ["500-1000","1000-1500","1500-2000","2000-2500","2500-3000","3000-4000","4000-5000","5000-6000","6000-7000","7000-8000","8000-9000","9000-10000","10000-12500","12500-15000","15000-17500","17500-20000","20000-25000","25000-30000","30000-40000"]
mileage_range_list = ["0-2500","2500-5000","5000-10000","10000-20000","20000-30000","30000-40000","40000-50000","50000-60000","60000-70000","70000-80000","80000-90000","90000-100000","100000-125000","125000-150000","150000-175000","175000-200000"]
color_list = ["1","2","3","4","5","6","7","10","11","12","13","14","15","16"]

def getCarUrl(url,car_count):
    print(car_count)
    if car_count > 400:
        car_count = 400
    page_iteration = math.ceil(int(car_count)/20)
    for page in range(1,page_iteration+1):
        url_page = url + '&page='+str(page)
        response  = requests.get(url_page)
        if response.status_code == 200:
            html_content = response.text
        else:
            print("Failed to retrieve the webpage")
            exit
        soup = BeautifulSoup(response.text,'lxml',parse_only=SoupStrainer("a")) 
        # print(soup)
        car_urls = [link.get("href") for link in soup.find_all("a") if "/offers/" in str(link.get("href"))]
        for car_url  in car_urls:
            df = pd.DataFrame({'car_url':['https://www.autoscout24.com'+car_url],'page_url':[url_page]})
            df.to_csv(file_name+".csv",mode="a",index=False,header=False)
        print(url_page)

def getCarCount(country,brand,registeryr,price_range,mileage_range,color,is_last):
    domain_url = 'https://www.autoscout24.com/lst'
    if brand != '':
        domain_url = domain_url+'/'+brand+'?'
    else:
        domain_url = domain_url+'?'

    if country != '':
        domain_url = domain_url+'atype=C&cy='+country
    
    if registeryr != '':
        if str(registeryr).__contains__("-"):
            start_yr = str(registeryr).split("-")[0]
            end_yr = str(registeryr).split("-")[1]
            domain_url = domain_url+'&fregfrom='+start_yr+'&fregto='+end_yr
        else:
            domain_url = domain_url+'&fregfrom='+registeryr+'&fregto='+registeryr
    
    if price_range != '':
        pricefrom = str(price_range).split('-')[0]
        priceto = str(price_range).split('-')[1]
        domain_url = domain_url+'&pricefrom='+pricefrom+'&priceto='+priceto

    if mileage_range != '':
        kmfrom = str(mileage_range).split('-')[0]
        kmto = str(mileage_range).split('-')[1]
        if kmfrom == '0':
            domain_url = domain_url+'&kmto='+kmto
        else:
            domain_url = domain_url+'&kmfrom='+kmfrom+'&kmto='+kmto
    if color != '':
        domain_url = domain_url+'&bcol='+color
    try:
        response  = requests.get(domain_url)
        if response.status_code == 200:
            html_content = response.text
        else:
            print("Failed to retrieve the webpage")
            exit
        soup = BeautifulSoup(response.content, "html.parser") 
        dom = etree.HTML(str(soup)) 
        car_count = dom.xpath('/html/body/div[1]/div[3]/div/div/div/div[5]/header/div/div/h1/span/span[1]')[0].text
    except Exception as e:
        print(e)
        car_count = 500

    car_count = int(str(car_count).replace(',',''))
    if (car_count > 0 and car_count < 401) or (car_count > 0 and is_last == 'YES'):
        getCarUrl(url=domain_url,car_count=car_count)
    return car_count
    
i=0
for country in country_list:
    if getCarCount(country,brand='',registeryr='',price_range='',mileage_range='',color='',is_last='') > 400:
        for brand in brand_list:
            if getCarCount(country,brand,registeryr='',price_range='',mileage_range='',color='',is_last='') > 400:
                for registeryr in registeryr_list:
                    if getCarCount(country,brand,registeryr,price_range='',mileage_range='',color='',is_last='') > 400:
                        for price_range in price_range_list:
                            if getCarCount(country,brand,registeryr,price_range,mileage_range='',color='',is_last='') > 400:
                                for mileage_range in mileage_range_list:
                                    if getCarCount(country,brand,registeryr,price_range,mileage_range=mileage_range,color='',is_last='') > 400:
                                        for color in color_list:
                                            # print(country,"->",brand,'->',registeryr,'->',price_range,'->',mileage_range,'->',color,'->',getCarCount(country,brand,registeryr,price_range,mileage_range,color,is_last='YES'))
                                            getCarCount(country,brand,registeryr,price_range,mileage_range,color,is_last='YES')
    break

    
