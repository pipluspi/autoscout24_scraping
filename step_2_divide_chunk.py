import pandas as pd
import os
import shutil
"""
NOTE: File name without csv
For Example : new_car_url_a.csv
Then your input file Name : new_car_url_a
"""

# Define your file name here (Read note before define file name)
file_name = "new_car_url_l"


chunk_size = 1000
if not os.path.exists('current_file/'):
    os.makedirs('current_file/')
file_save = 'current_file/'+file_name+'.csv'

df = pd.read_csv(file_name + '.csv',header=None)

# df = df.drop_duplicates()
df.rename(columns={0:'car_url', 1:'page_url'}, inplace=True)

df = df['car_url']
df.to_csv(file_save, header=False, index=False)

chunk_csv = pd.read_csv(file_save)
print(len(chunk_csv))
chunk_csv = chunk_csv.drop_duplicates()
print(len(chunk_csv))
chunk_iterator = pd.read_csv(file_save, chunksize=chunk_size)
if os.path.exists(f'chunk_url/{file_name}/'):
    print("exits")
    shutil.rmtree(f'chunk_url/{file_name}/')
    
os.makedirs(f'chunk_url/{file_name}/')
for i, chunk in enumerate(chunk_iterator):
    chunk.to_csv(f'chunk_url/{file_name}/{i}_chunk.csv', index=False)
    print(f'Chunk {i} saved.')
