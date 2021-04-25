import pandas as pd
import datetime as d
import requests
import io
import os
import sys

def findfile(name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return 1
        else:
            return 0

def LKNR_convert(LKNR_input):
    if len(str(LKNR_input)) < 5:
        LKNR_output = "0" + str(LKNR_input)
    else:
        LKNR_output = str(LKNR_input)

    return LKNR_output

###### Set the storage location to store the RKI Archiv data to ####

set_path = None


###########################

if set_path == None:
    print(f'You need to set a storage location for the RKI-Archiv data in the <set_path> variable first!')
    sys.exit(0)

print('Download Starting...')
url = 'https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Kum_Tab.xlsx?__blob=publicationFile'
print(f'{url}')
r = requests.get(url)
print(f'File date: {r.headers["date"]}')

excel_in = set_path + 'Fallzahlen_Archiv.xlsx'
 
with open(excel_in,'wb') as output_file:
    output_file.write(r.content)
 
print(f'Download Completed.\n See {excel_in}')

Archiv_start_date = d.datetime(2020,11,18)
today = d.datetime.today()

data_found = 0
i = 0

excel_data_df = pd.read_excel(excel_in, skiprows=3, header=1, sheet_name = "LK_7-Tage-Inzidenz",
                              converters={'LKNR': LKNR_convert})


while i < int(len(excel_data_df.axes[1])) - 3:
    date = Archiv_start_date + d.timedelta(i)
    str_date = d.datetime.strftime(date, '%Y%m%d')
    str_xl_date = d.datetime.strftime(date, '%d.%m.%Y')
    
    if not str_date == d.datetime.strftime(today, '%Y%m%d'):
        if findfile(str_date + '.csv', set_path) == 0:
            print(f'Writing data for {str_date}')
            f_output = set_path + str_date + '.csv'
            f_RKI_Archiv = open (f_output, 'w', newline = '', encoding = 'utf-8')
            column_name = excel_data_df.keys()[3+i]
            csv_writer = pd.DataFrame.to_csv(excel_data_df,
                                             path_or_buf=f_RKI_Archiv,
                                             columns = ["LK", "LKNR", column_name],
                                             header = ["Landkreis", "LKNR", "Inzidenz"])
            f_RKI_Archiv.close
            data_found = 1

    i +=1
if data_found == 0:
    print('Did not find new data sets in RKI Archiv.')

input('Press key to close...')
        


    
    
