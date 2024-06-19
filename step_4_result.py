import glob
import pandas as pd
import os
import json

"""
NOTE: File name without csv
For Example : new_car_url_a.csv
Then your input file Name : new_car_url_a
"""
# Define your file name here (Read note before define file name)
file_name = 'new_car_url_l'

foler_path = 'scraped_data/' + file_name

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
    if os.path.exists('final_result/'+file_name+'.csv'):
        main_df.to_csv('final_result/'+file_name+'.csv', header=False, mode='a',index=False)
    else:
        main_df.to_csv('final_result/'+file_name+'.csv', header=True, index=False)

print(f'File saved as {file_name}.csv in folder final_result')