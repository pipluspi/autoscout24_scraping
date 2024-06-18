import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import concurrent.futures

def fetch_and_parse(car_url):
    url = car_url[0]
    # print(url)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'authority': 'dos.sunbiz.org',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7'
    }
    
    response = requests.get(url, headers=headers).text
    soupHtml = BeautifulSoup(response, 'html.parser')
    vehicle_data = {}

    country_city = soupHtml.find('a', {'class': 'scr-link LocationWithPin_locationItem__tK1m5'}).text
    if country_city is not None:
        vehicle_data['country_city'] = country_city

    vehicle_overview_section = soupHtml.find("div", {"class": "VehicleOverview_containerMoreThanFourItems__691k2"})
    if vehicle_overview_section is not None:
        list_verhicle_overview = vehicle_overview_section.find_all("div", {"class": "VehicleOverview_itemContainer__XSLWi"})
        for vehicle in list_verhicle_overview:
            key = vehicle.find('div', {'class': 'VehicleOverview_itemTitle__S2_lb'}).text
            value = vehicle.find('div', {'class': 'VehicleOverview_itemText__AI4dA'}).text
            vehicle_data[key] = value

    manufacturer = soupHtml.find("span", {"class": "StageTitle_boldClassifiedInfo__sQb0l"}).text
    if manufacturer is not None:
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

    return pd.json_normalize(main_json)

def main():
    # while True:
        df = pd.read_csv('test/new_car_url_l.csv')
        df.drop_duplicates(inplace=True)
        print(len(df))
        # df = df.head(50)
        main_df = pd.DataFrame()
        print(df)
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_url = {executor.submit(fetch_and_parse, car_url): car_url for car_url in df.values}
            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    data = future.result()
                    main_df = pd.concat([main_df, data])
                    print(len(main_df))
                except Exception as exc:
                    print(f'Generated an exception: {exc}')

        if os.path.exists('result.csv'):
            main_df.to_csv('result.csv', header=False, mode='a', index=False)
        else:
            main_df.to_csv('result.csv', header=True, index=False)
        # break

if __name__ == "__main__":
    main()
