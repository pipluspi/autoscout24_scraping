from bs4 import BeautifulSoup,SoupStrainer
import requests
from lxml import etree
import math
import pandas as pd
import shutil
import os
import json
import concurrent.futures
import glob


class autoscout:
    def __init__(self,file_name,country_list,brand_list):
        self.file_name = file_name
        self.country_list = country_list
        self.brand_list = brand_list
    # file_name = "test"
    # # D = Germany , A = Austria, B = Belgium,  E = Spain, F = France, I = Italy, L = Luxembourg, NL = Netherlands
    # country_list = ["D","A","B","E","F","I","L","NL"]
    # brand_list = ["audi","bmw","ford","mercedes-benz","opel","volkswagen","renault","9ff","abarth","ac","acm","acura","aiways","aixam","alba-mobility","alfa-romeo","alpina","alpine","amphicar","angelelli-automobili","ariel-motor","artega","aspark","aspid","aston-martin","aurus","austin","austin-healey","autobianchi","baic","bedford","bellier","bentley","boldmen","bolloré","borgward","brilliance","bristol","brute","bugatti","buick","byd","cadillac","caravans-wohnm","carver","casalini","caterham","cenntro","changhe","chatenet","chery","chevrolet","chrysler","cirelli","citroen","cityel","corvette","cupra","dacia","daewoo","daf","daihatsu","daimler","dallara","dangel","de-la-chapelle","de-tomaso","delorean","devinci-cars","dfsk","dodge","donkervoort","dr-automobiles","ds-automobiles","dutton","e.go","econelo","edran","elaris","embuggy","emc","emc","estrima","evetta","evo","ferrari","fiat","fisker","forthing","foton","gac-gonow","galloper","gappy","gaz","gem","gemballa","genesis","giana","gillet","giotti-victoria","gmc","goupil","great-wall","grecav","gta","gwm","haima","hamann","haval","hiphi","holden","honda","hongqi","hongqi","hummer","hurtan","hyundai","ineos","infiniti","innocenti","iso-rivolta","isuzu","iveco","izh","jac","jaguar","jeep","jensen","karma","kg-mobility","kia","koenigsegg","ktm","lada","lamborghini","lancia","land-rover","ldv","levc","lexus","lifan","ligier","lincoln","linzda","lorinser","lotus","lucid","m-ero","mahindra","man","mansory","martin","martin-motors","maserati","matra","maxus","maybach","mazda","mclaren","mega","melex","mercury","mg","micro","microcar","militem","minari","minauto","mini","mitsubishi","mitsuoka","morgan","moskvich","mp-lafer","mpm-motors","nio","nissan","nsu","oldsmobile","oldtimer","omoda","ora","pagani","panther-westwinds","peugeot","pgo","piaggio","plymouth","polestar","pontiac","porsche","proton","puch","ram","regis","reliant","rolls-royce","rover","ruf","saab","santana","seat","segway","selvo","seres","sevic","sgs","shelby","shuanghuan","silence","singer","skoda","skywell","smart","speedart","sportequipe","spyker","ssangyong","streetscooter","studebaker","subaru","suzuki","talbot","tasso","tata","tazzari-ev","techart","tesla","togg","town-life","toyota","trabant","trailer-anhänger","triumph","trucks-lkw","tvr","uaz","vanden-plas","vanderhall","vaz","vem","vinfast","volvo","wartburg","weltmeister","wenckstern","westfield","wey","wiesmann","xbus","xev","xpeng","zastava","zaz","zeekr","zhidou","zotye","others"]
    # write if only want scrap country L as below
    # country_list = ["L"]
    # write brands you want to scrap as below
    # brand_list = ["9ff","abarth","ac"]
    registeryr_list = ["1900-1930","1931-1960","1961-1990","1991-1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","2023","2024"]
    price_range_list = ["500-1000","1000-1500","1500-2000","2000-2500","2500-3000","3000-4000","4000-5000","5000-6000","6000-7000","7000-8000","8000-9000","9000-10000","10000-12500","12500-15000","15000-17500","17500-20000","20000-25000","25000-30000","30000-40000"]
    mileage_range_list = ["0-2500","2500-5000","5000-10000","10000-20000","20000-30000","30000-40000","40000-50000","50000-60000","60000-70000","70000-80000","80000-90000","90000-100000","100000-125000","125000-150000","150000-175000","175000-200000"]
    color_list = ["1","2","3","4","5","6","7","10","11","12","13","14","15","16"]


    def cleanup(self):
        if os.path.exists(self.file_name+'.csv'):          
            os.remove(self.file_name+'.csv')
        if os.path.exists('chunk_url/'+ self.file_name+'/'):          
            shutil.rmtree('chunk_url/'+ self.file_name+'/')
        if os.path.exists('current_file/'+self.file_name+'.csv'):          
            os.remove('current_file/'+self.file_name+'.csv')
        if os.path.exists('inprogress/'+ self.file_name + '/'):          
            shutil.rmtree('inprogress/'+ self.file_name + '/')
        if os.path.exists('completed/'+ self.file_name + '/'):          
            shutil.rmtree('completed/'+ self.file_name + '/')
        if os.path.exists('scraped_data/'+ self.file_name + '/'):          
            shutil.rmtree('scraped_data/'+ self.file_name + '/')
        if os.path.exists('final_result/'+self.file_name+'.csv'):          
            os.remove('final_result/'+self.file_name+'.csv')

    def getCarUrl(self,url,car_count):
        # print(car_count)
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
                df.to_csv(self.file_name+".csv",mode="a",index=False,header=False)
            # print(url_page)

    def getCarCount(self,country,brand,registeryr,price_range,mileage_range,color,is_last):
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
            self.getCarUrl(url=domain_url,car_count=car_count)
        return car_count

    def step_1(self):
        i=0
        for country in self.country_list:
            if self.getCarCount(country,brand='',registeryr='',price_range='',mileage_range='',color='',is_last='') > 400:
                for brand in self.brand_list:
                    if self.getCarCount(country,brand,registeryr='',price_range='',mileage_range='',color='',is_last='') > 400:
                        for registeryr in self.registeryr_list:
                            if self.getCarCount(country,brand,registeryr,price_range='',mileage_range='',color='',is_last='') > 400:
                                for price_range in self.price_range_list:
                                    if self.getCarCount(country,brand,registeryr,price_range,mileage_range='',color='',is_last='') > 400:
                                        for mileage_range in self.mileage_range_list:
                                            if self.getCarCount(country,brand,registeryr,price_range,mileage_range=mileage_range,color='',is_last='') > 400:
                                                for color in self.color_list:
                                                    # print(country,"->",brand,'->',registeryr,'->',price_range,'->',mileage_range,'->',color,'->',getCarCount(country,brand,registeryr,price_range,mileage_range,color,is_last='YES'))
                                                    self.getCarCount(country,brand,registeryr,price_range,mileage_range,color,is_last='YES')
        print("STEP - 1 COMPLETED")
    def step_2(self):
        chunk_size = 1000
        if not os.path.exists('current_file/'):
            os.makedirs('current_file/')
        file_save = 'current_file/'+self.file_name+'.csv'

        df = pd.read_csv(self.file_name + '.csv',header=None)

        # df = df.drop_duplicates()
        df.rename(columns={0:'car_url', 1:'page_url'}, inplace=True)

        df = df['car_url']
        df.to_csv(file_save, header=False, index=False)

        chunk_csv = pd.read_csv(file_save)
        # print(len(chunk_csv))
        chunk_csv = chunk_csv.drop_duplicates()
        # print(len(chunk_csv))
        chunk_iterator = pd.read_csv(file_save, chunksize=chunk_size)
        if os.path.exists(f'chunk_url/{self.file_name}/'):
            # print("exits")
            shutil.rmtree(f'chunk_url/{self.file_name}/')
            
        os.makedirs(f'chunk_url/{self.file_name}/')
        for i, chunk in enumerate(chunk_iterator):
            chunk.to_csv(f'chunk_url/{self.file_name}/{i}_chunk.csv', index=False)
            # print(f'Chunk {i} saved.')
        print("STEP - 2 COMPLETED")
    


    def fetch_and_parse(self,car_url, input_file_name):
        url = car_url[0]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'authority': 'dos.sunbiz.org',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7'
        }
        
        response_status = requests.get(url, headers=headers)
        if response_status.status_code == 200:
            response = requests.get(url, headers=headers).text
            soupHtml = BeautifulSoup(response, 'html.parser')
            vehicle_data = {}

            country_city = soupHtml.find('a', {'class': 'scr-link LocationWithPin_locationItem__tK1m5'})
            if country_city is not None:
                country_city = soupHtml.find('a', {'class': 'scr-link LocationWithPin_locationItem__tK1m5'}).text
                vehicle_data['country_city'] = country_city

            vehicle_overview_section = soupHtml.find("div", {"class": "VehicleOverview_containerMoreThanFourItems__691k2"})
            if vehicle_overview_section is not None:
                list_verhicle_overview = vehicle_overview_section.find_all("div", {"class": "VehicleOverview_itemContainer__XSLWi"})
                for vehicle in list_verhicle_overview:
                    key = vehicle.find('div', {'class': 'VehicleOverview_itemTitle__S2_lb'}).text
                    value = vehicle.find('div', {'class': 'VehicleOverview_itemText__AI4dA'}).text
                    vehicle_data[key] = value

            manufacturer = soupHtml.find("span", {"class": "StageTitle_boldClassifiedInfo__sQb0l"})
            if manufacturer is not None:
                manufacturer = soupHtml.find("span", {"class": "StageTitle_boldClassifiedInfo__sQb0l"}).text
                vehicle_data['manufacturer'] = manufacturer
            model_name = soupHtml.find("span", {"class": "StageTitle_model__EbfjC StageTitle_boldClassifiedInfo__sQb0l"}).text
            if model_name is not None:
                vehicle_data['model'] = model_name
            price_vehicle = str(soupHtml.find("span", {"class": "PriceInfo_price__XU0aF"}).text).replace('€', '').replace('-', '').strip()
            if price_vehicle is not None:
                vehicle_data['price_vehicle'] = price_vehicle

            basic_data_section = soupHtml.find('section', {'id': 'basic-details-section'})
            if basic_data_section is not None:
                basic_data_key = basic_data_section.find_all('dt', {'class': 'DataGrid_defaultDtStyle__soJ6R'})
                basic_data_value = basic_data_section.find_all('dd', {'class': 'DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01'})

                for i in range(len(basic_data_key)):
                    vehicle_data[basic_data_key[i].text] = basic_data_value[i].text

            technical_data_section = soupHtml.find('section', {'id': 'technical-details-section'})
            if technical_data_section is not None:
                technical_data_div = technical_data_section.find('div', {'class': 'DetailsSection_childrenSection__aElbi'})
                technical_data_key = technical_data_div.find_all('dt', {'class': 'DataGrid_defaultDtStyle__soJ6R'})
                technical_data_value = technical_data_div.find_all('dd', {'class': 'DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01'})
                for j in range(len(technical_data_key)):
                    vehicle_data[technical_data_key[j].text] = technical_data_value[j].text

            vehicle_history_section = soupHtml.find('section', {'id': 'listing-history-section'})
            if vehicle_history_section is not None:
                vehicle_history_div = vehicle_history_section.find('div', {'class': 'DetailsSection_childrenSection__aElbi'})
                vehicle_history_key = vehicle_history_div.find_all('dt', {'class': 'DataGrid_defaultDtStyle__soJ6R'})
                vehicle_history_value = vehicle_history_div.find_all('dd', {'class': 'DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01'})
                for j in range(len(vehicle_history_key)):
                    vehicle_data[vehicle_history_key[j].text] = vehicle_history_value[j].text

            color_data_section = soupHtml.find('section', {'id': 'color-section'})
            if color_data_section is not None:
                color_data_div = color_data_section.find('div', {'class': 'DetailsSection_childrenSection__aElbi'})
                color_data_key = color_data_div.find_all('dt', {'class': 'DataGrid_defaultDtStyle__soJ6R'})
                color_data_value = color_data_div.find_all('dd', {'class': 'DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01'})
                for j in range(len(color_data_key)):
                    vehicle_data[color_data_key[j].text] = color_data_value[j].text

            zip_code_section = soupHtml.find('div', {'id': 'vendor-and-cta-section'})
            if zip_code_section is not None:
                zip_code_container_div = zip_code_section.find('div', {'class': 'Department_openingHoursContainer__1Pk01'})
                zip_code_div = zip_code_container_div.find('div', {'class': 'Department_departmentContainer__UZ97C'})
                zip_code_a_tag = zip_code_div.find('a', {'class': 'scr-link Department_link__xMUEe'})
                vehicle_data['zip_code'] = zip_code_a_tag.text
            vehicle_data['url'] = url

            main_json = {
                "PRICE": vehicle_data.get('price_vehicle', ''),
                "MANUFACTURER": vehicle_data.get('manufacturer', ''),
                "MODEL": vehicle_data.get('model', ''),
                "YEAR OF MANUFACTURE": vehicle_data.get('Production date', ''),
                "POWER (HP & KW)": vehicle_data.get('Power', ''),
                "FIRST REGISTRATION": vehicle_data.get('First registration', ''),
                "MILEAGE": vehicle_data.get('Mileage', ''),
                "BODY TYPE": vehicle_data.get('Body type', ''),
                "SEATS": vehicle_data.get('Seats', ''),
                "DOORS": vehicle_data.get('Doors', ''),
                "ENGINE SIZE": vehicle_data.get('Engine size', ''),
                "GEARS": vehicle_data.get('Gears', ''),
                "CYLINDERS": vehicle_data.get('Cylinders', ''),
                "DRIVETRAIN": vehicle_data.get('Drivetrain', ''),
                "FUEL TYPE": vehicle_data.get('Fuel type', ''),
                "WEIGHT": vehicle_data.get('Empty weight', ''),
                "GEARBOX": vehicle_data.get('Gearbox', ''),
                "SELLER": vehicle_data.get('Seller', ''),
                "COLOUR": vehicle_data.get('Colour', ''),
                "UPHOLSTERY COLOUR": vehicle_data.get('Upholstery colour', ''),
                "CITY & COUNTRY": vehicle_data.get('country_city', ''),
                "POSTCODE WITH CITY": vehicle_data.get('zip_code', ''),
                "URL": vehicle_data.get('url', '')
            }
            url = main_json['URL']
            input_file_name_var = input_file_name
            input_file_name = input_file_name_var.split('.')[0]
            url = str(url).replace('https://www.autoscout24.com/offers/','')
            if not os.path.exists('scraped_data/'+self.file_name+'/'+input_file_name+'/'):
                os.makedirs('scraped_data/'+self.file_name+'/'+input_file_name+'/')
            with open('scraped_data/'+self.file_name+'/'+input_file_name+'/'+url+'.json', 'w') as f:
                    json.dump(main_json, f)
            f.close()
        else:
            return {'message':'Status code is not 200'}
    
    def step_3(self):
        folder_path = 'chunk_url' + '/' + self.file_name 
        file_list = glob.glob(folder_path+'/*.csv')
        for idx,files in enumerate(file_list):
            input_file_name = file_list[idx].split('\\')[-1]
            if not os.path.exists('inprogress/'+ self.file_name):
                os.makedirs('inprogress/'+ self.file_name)
            shutil.move(folder_path+'/'+input_file_name,'inprogress/'+ self.file_name + '/'+input_file_name)
            df = pd.read_csv('inprogress/'+ self.file_name + '/'+input_file_name)
            df.drop_duplicates(inplace=True)
            with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
                future_to_url = {executor.submit(self.fetch_and_parse, car_url, input_file_name): car_url for car_url in df.values}
                for future in concurrent.futures.as_completed(future_to_url):
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f'Generated an exception: {exc}')
            if not os.path.exists('completed/'+ self.file_name + '/'+input_file_name+'/'):
                os.makedirs('completed/'+ self.file_name + '/'+input_file_name+'/')
            shutil.move('inprogress/'+ self.file_name + '/'+input_file_name,'completed/'+ self.file_name + '/'+input_file_name)
        print("STEP - 3 COMPLETED")
    
    def step_4(self):
        foler_path = 'scraped_data/' + self.file_name
        folder_list = os.listdir(foler_path)
        for folder in folder_list:
            main_df = pd.DataFrame()
            file_list = glob.glob(foler_path+'/'+folder+'/'+'*.json')
            for file in file_list:
                with open(file, 'r') as f:
                    data = json.load(f)
                df = pd.json_normalize(data)
                main_df = pd.concat([main_df, df], ignore_index=True)  
            if not os.path.exists('final_result/'):
                os.makedirs('final_result/')
            if os.path.exists('final_result/'+self.file_name+'.csv'):
                main_df.to_csv('final_result/'+self.file_name+'.csv', header=False, mode='a',index=False)
            else:
                main_df.to_csv('final_result/'+self.file_name+'.csv', header=True, index=False)
        print("STEP - 4 COMPLETED")
        print(f'Final scraping result is saved path is /final_result/{self.file_name}.csv')

if __name__ == "__main__":
    file_name = str(input("Enter File Name : ")).replace(" ","_").split(".")[0]
    # Sample example
    # D = Germany , A = Austria, B = Belgium,  E = Spain, F = France, I = Italy, L = Luxembourg, NL = Netherlands
    # country_list = ["D","A","B","E","F","I","L","NL"]
    # brand_list = ["audi","bmw","ford","mercedes-benz","opel","volkswagen","renault","9ff","abarth","ac","acm","acura","aiways","aixam","alba-mobility","alfa-romeo","alpina","alpine","amphicar","angelelli-automobili","ariel-motor","artega","aspark","aspid","aston-martin","aurus","austin","austin-healey","autobianchi","baic","bedford","bellier","bentley","boldmen","bolloré","borgward","brilliance","bristol","brute","bugatti","buick","byd","cadillac","caravans-wohnm","carver","casalini","caterham","cenntro","changhe","chatenet","chery","chevrolet","chrysler","cirelli","citroen","cityel","corvette","cupra","dacia","daewoo","daf","daihatsu","daimler","dallara","dangel","de-la-chapelle","de-tomaso","delorean","devinci-cars","dfsk","dodge","donkervoort","dr-automobiles","ds-automobiles","dutton","e.go","econelo","edran","elaris","embuggy","emc","emc","estrima","evetta","evo","ferrari","fiat","fisker","forthing","foton","gac-gonow","galloper","gappy","gaz","gem","gemballa","genesis","giana","gillet","giotti-victoria","gmc","goupil","great-wall","grecav","gta","gwm","haima","hamann","haval","hiphi","holden","honda","hongqi","hongqi","hummer","hurtan","hyundai","ineos","infiniti","innocenti","iso-rivolta","isuzu","iveco","izh","jac","jaguar","jeep","jensen","karma","kg-mobility","kia","koenigsegg","ktm","lada","lamborghini","lancia","land-rover","ldv","levc","lexus","lifan","ligier","lincoln","linzda","lorinser","lotus","lucid","m-ero","mahindra","man","mansory","martin","martin-motors","maserati","matra","maxus","maybach","mazda","mclaren","mega","melex","mercury","mg","micro","microcar","militem","minari","minauto","mini","mitsubishi","mitsuoka","morgan","moskvich","mp-lafer","mpm-motors","nio","nissan","nsu","oldsmobile","oldtimer","omoda","ora","pagani","panther-westwinds","peugeot","pgo","piaggio","plymouth","polestar","pontiac","porsche","proton","puch","ram","regis","reliant","rolls-royce","rover","ruf","saab","santana","seat","segway","selvo","seres","sevic","sgs","shelby","shuanghuan","silence","singer","skoda","skywell","smart","speedart","sportequipe","spyker","ssangyong","streetscooter","studebaker","subaru","suzuki","talbot","tasso","tata","tazzari-ev","techart","tesla","togg","town-life","toyota","trabant","trailer-anhänger","triumph","trucks-lkw","tvr","uaz","vanden-plas","vanderhall","vaz","vem","vinfast","volvo","wartburg","weltmeister","wenckstern","westfield","wey","wiesmann","xbus","xev","xpeng","zastava","zaz","zeekr","zhidou","zotye","others"]
    # D = Germany , A = Austria, B = Belgium,  E = Spain, F = France, I = Italy, L = Luxembourg, NL = Netherlands
    country_list = ["L"]
    brand_list = ["9ff","abarth","ac"]    
    print("File name will be used while saving data will be => "+file_name)
    print("Scraping data from autoscout24 for country => "+str(country_list)+" and brands => "+str(brand_list))
    
    autoscout_obj = autoscout(file_name,country_list,brand_list)
    autoscout_obj.cleanup()
    autoscout_obj.step_1()
    autoscout_obj.step_2()
    autoscout_obj.step_3()
    autoscout_obj.step_4()