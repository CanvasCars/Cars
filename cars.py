from datetime import datetime
import pandas as pd

data_frame = pd.DataFrame(columns=[
    'Car_ID', 'Title', 'Region', 'Make', 'Model', 'Variant', 'Suburb', 'Province',
    'Price', 'ExpectedPaymentPerMonth', 'CarType', 'RegistrationYear', 'Mileage',
    'Transmission', 'FuelType', 'PriceRating', 'Dealer', 'LastUpdated', 'PreviousOwners',
    'ManufacturersColour', 'BodyType', 'ServiceHistory', 'WarrantyRemaining',
    'IntroductionDate', 'EndDate', 'ServiceIntervalDistance', 'EnginePosition', 'EngineDetail',
    'EngineCapacity', 'CylinderLayoutAndQuantity', 'FuelTypeEngine', 'FuelCapacity', 'FuelConsumption',
    'FuelRange', 'PowerMaximum', 'TorqueMaximum', 'Acceleration', 'MaximumSpeed', 'CO2Emissions',
    'Version', 'DealerUrl', 'Timestamp'
])    
def bot():       
 
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)
    import threading
    import requests
    from bs4 import BeautifulSoup
    import re
    import datetime
    from datetime import datetime
    from azure.storage.blob import BlobClient
    base_url = "https://www.cars.co.za" 

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.5'
    }

    #getting the last page from website
    base_url1 = "https://www.cars.co.za/usedcars/?sort=date_d&P=1"
    response = requests.get(base_url1,headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    href_elements = soup.find_all(href=lambda x: x and "/usedcars/?P=" in x)
    # Extract page numbers greater than 2000
    for element in href_elements:
        # Use regular expression to extract numeric part of href
        match = re.search(r"/usedcars/\?P=(\d+)", element['href'])
        if match:
            page_number = int(match.group(1))
            if page_number > 2000:
                last_page_number=page_number   


    # Lock to synchronize access to the thread_data list
    lock = threading.Lock()

    # List to store data from each thread
    thread_data = []

    def insert_data_into_db(data):
        
        global data_frame  # Use the global DataFrame
        data_series = pd.Series(data)  # Convert the dictionary to a Series
        data_frame = pd.concat([data_frame, data_series.to_frame().T], ignore_index=True) 


        #body_ = "{''}"
        #_url = 'https://prod-14.southafricanorth.logic.azure.com:443/workflows/5c26e8c9a8604e68a661cdcd8cd4b9db/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=EvVRrny6VDcTMYMNjkEn0t07gNRDEdEgtQiQlVuGXMo'
        #requests.post(url=_url, data=body_)       
        


    def scrape_and_store_data(base_url, start_page, last_page, thread_num):
        try:
            for page in range(start_page, last_page + 1):
                #scraping code
                response = requests.get(f"https://www.cars.co.za/usedcars/?sort=price_d&P={page}",headers=headers)
                                        #https://www.cars.co.za/usedcars/?P={page}
                home_page = BeautifulSoup(response.text, 'html.parser')
                # Find all links containing 'href="/for"'
                links = home_page.find_all(href=lambda href: href and '/for' in href)
 
                # Extract and print the links
                for link in links:
                    href = link.get('href')
                    if re.match(r'^/for-sale/', href):
                        found_link = base_url + href
                            
                        try:
                            # Extract the Car ID from the link using regular expressions
                            car_id_match = re.search(r'/(\d+)/', found_link)


                            if car_id_match:
                                car_id = car_id_match.group(1)

                                # Get the HTML content of the car listing page
                                res = requests.get(found_link, headers=headers,timeout=20)
                                
                                # Parse the cleaned HTML content
                                car_page = BeautifulSoup(res.text, 'html.parser')
                                # Append the parsed HTML content to a list
                                parsed_html_list = [car_page]

                                # Find the script element containing the JSON-like data
                                script_element = parsed_html_list[0].find('script', {'id': '__NEXT_DATA__'})

                                # Extract the JSON-like data as a string
                                json_data = script_element.string
                                # Parse the JSON-like data into a Python dictionary
                                import json
                                data_dict = json.loads(json_data)

                                Car_ID = data_dict['props']['pageProps']['vehicle']['id']
                                title = data_dict['props']['pageProps']['vehicle']['attributes']['title']
                                location = data_dict['props']['pageProps']['vehicle']['attributes']['agent_locality']

                                # Handle the case when location is None
                                if location is None:
                                    dealer_info_div = car_page.find('div', class_='text-medium')

                                    if dealer_info_div:
                                        p_elements = dealer_info_div.find_all('p')
                                        
                                        # Check each <p> element for location information
                                        for p in p_elements:
                                            text = p.get_text(strip=True)
                                            
                                            # You can add conditions to identify the location
                                            if text and ',' in text:
                                                location = text
                                                location = location.split(',')[0].strip()
                                                break
                                    else:
                                        location=None        


                                brand = data_dict['props']['pageProps']['vehicle']['attributes']['make']
                                model = data_dict['props']['pageProps']['vehicle']['attributes']['model']
                                model_index = title.find(model)
                                # Extract the variant based on the model index
                                variant = title[model_index + len(model):].strip()
                                suburb = None
                                price = data_dict['props']['pageProps']['vehicle']['attributes']['price']
                                expected_payment = None
                                car_type = data_dict['props']['pageProps']['vehicle']['attributes']['new_or_used']
                                registration_year = re.search(r'\d+', title)
                                registration_year = registration_year.group()
                                mileage = data_dict['props']['pageProps']['vehicle']['attributes']['mileage']
                                transmission = data_dict['props']['pageProps']['vehicle']['attributes']['transmission']
                                fuel_type = data_dict['props']['pageProps']['vehicle']['attributes']['fuel_type']
                                price_rating = None
                                dealer = data_dict['props']['pageProps']['vehicle']['attributes']['agent_name']
                                last_updated = data_dict['props']['pageProps']['vehicle']['attributes']['date_time']
                                previous_owners = None
                                manufacturers_colour = data_dict['props']['pageProps']['vehicle']['attributes']['colour']
                                body_type = data_dict['props']['pageProps']['vehicle']['attributes']['body_type']
                                service_history = None
                                warranty_remaining = None
                                introduction_date = data_dict['props']['pageProps']['vehicle']['attributes']['date']
                                end_date = None
                                service_interval_distance = None

                                engine_position = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Engine":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Engine Position / Location":
                                                engine_position = attr['value']
                                                break

                                engine_detail = None
                                engine_capacity = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Engine":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Engine Size":
                                                engine_capacity = attr['value']
                                                break

                                cylinder_layout = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Engine":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Cylinders":
                                                cylinder_layout = attr['value']
                                                break

                                fuel_type_engine = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Engine":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Fuel Type":
                                                fuel_type_engine = attr['value']
                                                break

                                fuel_capacity = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Economy":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Fuel tank capacity":
                                                fuel_capacity = attr['value']
                                                break

                                fuel_consumption = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Summary":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Average Fuel Economy":
                                                fuel_consumption = attr['value']
                                                break

                                fuel_range = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Economy":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Fuel range":
                                                fuel_range = attr['value']
                                                break

                                power_max = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Summary":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Power Maximum Total":
                                                power_max = attr['value']
                                                break

                                torque_max = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Engine":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Torque Max":
                                                torque_max = attr['value']
                                                break

                                acceleration = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Performance":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "0-100Kph":
                                                acceleration = attr['value']
                                                break

                                maximum_speed = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Performance":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Top speed":
                                                maximum_speed = attr['value']
                                                break

                                co2_emissions = None
                                for spec in data_dict['props']['pageProps']['vehicle']['attributes']['specs']:
                                    if spec['title'] == "Economy":
                                        for attr in spec['attrs']:
                                            if attr['label'] == "Co2":
                                                co2_emissions = attr['value']
                                                break

                                current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                province_name = data_dict['props']['pageProps']['vehicle']['attributes']['province']
                                # Define a dictionary to hold the data
                                dealer_link = car_page.find('a', class_='ClickableCard_card__EHFn3')
                                basedealer="https://www.cars.co.za"
                                if dealer_link:
                                    link_dealer = dealer_link
                                    DealerUrl=(basedealer + link_dealer['href'])

                                else:
                                    DealerUrl = None
                                    
                        except Exception as e:
                            print(f"Error in thread {thread_num}: {e}")

                        # Store the scraped data in the data dictionary
                        data = {
                                'Car_ID': car_id,
                                'Title': title,
                                'Region': location,
                                'Make': brand,
                                'Model': model,
                                'Variant': variant,
                                'Suburb': suburb,
                                'Province': province_name,
                                'Price': price,
                                'ExpectedPaymentPerMonth': expected_payment,
                                'CarType': car_type,
                                'RegistrationYear': registration_year,
                                'Mileage': mileage,
                                'Transmission': transmission,
                                'FuelType': fuel_type,
                                'PriceRating': price_rating,
                                'Dealer': dealer,
                                'LastUpdated': last_updated,
                                'PreviousOwners': previous_owners,
                                'ManufacturersColour': manufacturers_colour,
                                'BodyType': body_type,
                                'ServiceHistory': service_history,
                                'WarrantyRemaining': warranty_remaining,
                                'IntroductionDate': introduction_date,
                                'EndDate': end_date,
                                'ServiceIntervalDistance': service_interval_distance,
                                'EnginePosition': engine_position,
                                'EngineDetail': engine_detail,
                                'EngineCapacity': engine_capacity,
                                'CylinderLayoutAndQuantity': cylinder_layout,
                                'FuelTypeEngine': fuel_type_engine,
                                'FuelCapacity': fuel_capacity,
                                'FuelConsumption': fuel_consumption,
                                'FuelRange': fuel_range,
                                'PowerMaximum': power_max,
                                'TorqueMaximum': torque_max,
                                'Acceleration': acceleration,
                                'MaximumSpeed': maximum_speed,
                                'CO2Emissions': co2_emissions,
                                'Version': 1,
                                'DealerUrl':DealerUrl,
                                'Timestamp': current_datetime
                            }
                        # Acquire the lock before appending to the thread_data list
                        with lock:
                            thread_data.append(data)
        except Exception as e:
            print(f"Error in thread {thread_num}: {e}")

    if __name__ == '__main__':
        # Define the number of threads
        num_threads = 6

        # Define the base URL and other scraping parameters for each thread
        thread_params = [(1, 400), (389, 800), (798, 1200), (1198, 1600), (1598, 2000), (1998, 2400),(2398, 2800), (2798, 3200), (3198, 3600), (3598, last_page_number)]
        #thread_params = [
       #     (1, 620),
        #    (618, 1240),
        #    (1238, 1860),
        #    (1878, 2480),                
        #    (2478, 3100),
        #    (3098, last_page_number)
       # ]
        # Create thread objects
        threads = []

        for i, (start_page, last_page) in enumerate(thread_params):
            thread = threading.Thread(target=scrape_and_store_data, args=(base_url, start_page, last_page, i + 1))
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        # Insert data into the database from thread_data list
        for data in thread_data:
            insert_data_into_db(data)
            
        print(f"Total cars found: {len(data_frame)}")
        

        car_data_csv = data_frame.to_csv(encoding = "utf-8",index=False)
        sas_url= f"https://stautotrader.blob.core.windows.net/carscoza/Carscoza1.csv?sv=2021-10-04&st=2023-10-26T06%3A25%3A32Z&se=2030-10-27T06%3A25%3A00Z&sr=c&sp=racwdxltf&sig=sUrO3DJetYAJLEG8AFAV2zJ13xQPX4uL2XgJwzHFnw8%3D"

        client = BlobClient.from_blob_url(sas_url)
        client.upload_blob(car_data_csv, overwrite=True)

while True:

      bot()
      data_frame = pd.DataFrame(columns=[
      'Car_ID', 'Title', 'Region', 'Make', 'Model', 'Variant', 'Suburb', 'Province',
      'Price', 'ExpectedPaymentPerMonth', 'CarType', 'RegistrationYear', 'Mileage',
      'Transmission', 'FuelType', 'PriceRating', 'Dealer', 'LastUpdated', 'PreviousOwners',
      'ManufacturersColour', 'BodyType', 'ServiceHistory', 'WarrantyRemaining',
      'IntroductionDate', 'EndDate', 'ServiceIntervalDistance', 'EnginePosition', 'EngineDetail',
      'EngineCapacity', 'CylinderLayoutAndQuantity', 'FuelTypeEngine', 'FuelCapacity', 'FuelConsumption',
      'FuelRange', 'PowerMaximum', 'TorqueMaximum', 'Acceleration', 'MaximumSpeed', 'CO2Emissions',
      'Version', 'DealerUrl', 'Timestamp'
      ])
   
