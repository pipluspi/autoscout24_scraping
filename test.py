import requests
from bs4 import BeautifulSoup
import re
import xlsxwriter

def AdatokLekerese(soup, worksheet):
    sor = 0
    
    # Megpróbáljuk beolvasni a sor változót a fájlból
    try:
        with open(re.sub(r'$', '.txt', cartype), 'r') as file:
            sor = int(file.read())
    except FileNotFoundError:
        # Ha még nincs fájl, létrehozunk egyet és kiírjuk a kezdeti értéket
        with open(re.sub(r'$', '.txt', cartype), 'w') as file:
            file.write(str(sor))
    
    oldalszam=1
    kereses = True
    while kereses:
        regex = re.compile('ListItem_wrapper__TxHWu')
        cars = soup.find_all('div', class_ = regex)
        oldaldarab = 0
        for car in cars:
            name = car.find('h2').text
            while True:
                try:
                    # Eredeti lekérdezés
                    price = car.find('p',  class_ = 'Price_price__APlgs PriceAndSeals_current_price__ykUpx').text.strip('€ ').strip(',-').replace('.','').replace('','')
                    break  # Ha sikerült a lekérdezés, kilépünk a while ciklusból

                except Exception as e:
                    print("Hiba történt:", e)
                    print(car.find('p',  class_ = 'Price_price__APlgs PriceAndSeals_current_price__ykUpx'))
                    # Alternatív lekérdezés leasing autók esetére
                    price = 0
                    break
            km = car.find('span', class_ = 'VehicleDetailTable_item__4n35N').text.strip(' km').replace('.','')
            gear = car.find('span', class_ = 'VehicleDetailTable_item__4n35N').find_next("span").text
            year = car.find('span', class_ = 'VehicleDetailTable_item__4n35N').find_next("span").find_next("span").text.strip('*/')
            fuel_type = car.find('span', class_ = 'VehicleDetailTable_item__4n35N').find_next("span").find_next("span").find_next("span").text
            power = car.find('span', class_ = 'VehicleDetailTable_item__4n35N').find_next("span").find_next("span").find_next("span").find_next("span").text
            
            worksheet.write(sor+1, 0, sor+1)
            worksheet.write(sor+1, 1, name)
            worksheet.write(sor+1, 2, price)
            worksheet.write(sor+1, 3, km)
            worksheet.write(sor+1, 4, gear)
            worksheet.write(sor+1, 5, year)
            worksheet.write(sor+1, 6, fuel_type)
            worksheet.write(sor+1, 7, power)
            oldaldarab+=1
            sor+=1
        if oldaldarab == 0:
            kereses = False
        else:
            oldalszam+=1
        newurl = re.sub(r'page=\d+', f'page={oldalszam}', url)
        print(f'VÉGE AZ OLDALNAK! KÖVETKEZIK: {oldalszam}')
        response = requests.get(newurl)
        soup = BeautifulSoup(response.text, 'html.parser')
        print(newurl)
        print(oldaldarab)
        
    with open(re.sub(r'$', '.txt', cartype), 'w') as file:
        file.write(str(sor))

T=[["https://www.autoscout24.de/lst/tesla/model-3/re_1990?atype=C&cy=D&damaged_listing=exclude&desc=0&kmfrom=1&kmto=1000000&ocs_listing=include&page=1&powertype=kw&search_id=25twvslahd9&sort=standard&source=listpage_pagination&ustate=N%2CU","Tesla 3"],
   ["https://www.autoscout24.de/lst/tesla/model-s/re_1990?atype=C&cy=D&damaged_listing=exclude&desc=0&kmfrom=1&kmto=1000000&ocs_listing=include&page=1&powertype=kw&search_id=682vsr8v18&sort=standard&source=listpage_pagination&ustate=N%2CU","Tesla S"],
   ["https://www.autoscout24.de/lst/audi/a4/re_1990?atype=C&cy=D&damaged_listing=exclude&desc=0&kmfrom=1&kmto=1000000&ocs_listing=include&page=1&powertype=kw&search_id=2ekrfc2cmjn&sort=standard&source=listpage_pagination&ustate=N%2CU","Audi A4"],
   ["https://www.autoscout24.de/lst/mercedes-benz/c-klasse-(alle)/re_1990?atype=C&cy=D&damaged_listing=exclude&desc=0&kmfrom=1&kmto=1000000&ocs_listing=include&page=1&powertype=kw&search_id=2cdeixlcstz&sort=standard&source=listpage_pagination&ustate=N%2CU","Mercedes C"],
   ["https://www.autoscout24.de/lst/opel/corsa/re_1990?atype=C&cy=D&damaged_listing=exclude&desc=0&kmfrom=1&kmto=1000000&ocs_listing=include&page=1&powertype=kw&search_id=238xlvn3xtx&sort=standard&source=listpage_pagination&ustate=N%2CU","Opel Corsa"],
   ["https://www.autoscout24.de/lst/porsche/cayenne/re_1990?atype=C&cy=D&damaged_listing=exclude&desc=0&kmfrom=1&kmto=1000000&ocs_listing=include&page=1&powertype=kw&search_id=1aik7sizizp&sort=standard&source=listpage_pagination&ustate=N%2CU","Porsche Cayenne"],
   ["https://www.autoscout24.de/lst/porsche/911/re_1990?atype=C&cy=D&damaged_listing=exclude&desc=0&kmfrom=1&kmto=1000000&ocs_listing=include&page=1&powertype=kw&search_id=qs52pmkl4&sort=standard&source=listpage_pagination&ustate=N%2CU","Porsche 911"],
   ["https://www.autoscout24.de/lst/renault/clio/re_1990?atype=C&cy=D&damaged_listing=exclude&desc=0&kmfrom=1&kmto=1000000&ocs_listing=include&page=1&powertype=kw&search_id=1vmzq3w28az&sort=standard&source=listpage_pagination&ustate=N%2CU","Renault Clio"],
   ["https://www.autoscout24.de/lst/volkswagen/polo/re_1990?atype=C&cy=D&damaged_listing=exclude&desc=0&kmfrom=1&kmto=1000000&ocs_listing=include&page=1&powertype=kw&search_id=avat19i6f6&sort=standard&source=listpage_pagination&ustate=N%2CU","Volswagen Polo"]]

for x in T:
    url=x[0]
    cartype=x[1]
    
    workbook = xlsxwriter.Workbook(re.sub(r'$', '.xlsx', cartype))
    worksheet = workbook.add_worksheet('Alap adatok')
    worksheet.write(0, 0, 'Index')
    worksheet.write(0, 1, 'Név')
    worksheet.write(0, 2, 'Ár (Eur)')
    worksheet.write(0, 3, 'Km óra állása (Km)')
    worksheet.write(0, 4, 'Váltó típus')
    worksheet.write(0, 5, 'Évjárat')
    worksheet.write(0, 6, 'Üzemanyag típus')
    worksheet.write(0, 7, 'Teljesítmény (kW és LE)')

    evjarat=1990
    while evjarat<2025:
        fromkm = 1
        tokm = 2500
        url = re.sub(r're_\d+', f're_{evjarat}', url)
        url = re.sub(r'kmfrom=\d+', f'kmfrom=0', url)
        url = re.sub(r'kmto=\d+', f'kmto=1000000', url)
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        talalatok = (int)(soup.find('div', class_= 'ListHeaderExperiment_title_with_sort__Gj9w7').find_next("span").find_next("span").text.replace('.',''))
        print(talalatok)
        if talalatok == 0:
            evjarat = evjarat+1
            continue
        elif talalatok != 0 and talalatok <= 400:
            AdatokLekerese(soup, worksheet)
            evjarat = evjarat+1
            continue
        else:
            while fromkm<302500:
                url = re.sub(r'kmfrom=\d+', f'kmfrom={fromkm}', url)
                url = re.sub(r'kmto=\d+', f'kmto={tokm}', url)
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                AdatokLekerese(soup, worksheet)
                fromkm+=2500
                tokm+=2500
                if fromkm == 300001:
                    tokm=1000000
            evjarat = evjarat+1
            continue
            
    workbook.close()