import csv
import datetime as d
import requests
import pandas as pd
import io
import sys
import time
from pathlib import Path

version = "0.7.2"

today = d.datetime.today()
str_today = d.datetime.strftime(today, '%Y%m%d')
found_lk = 0
lk = ''

###### Set the storage location to store the RKI Archiv data to ####

set_path = None # # <-- set the path where to store the 7 day incidence data to
set_path_ARCHIV = None # <-- set the path to your RKI Archiv data


###########################

if set_path_ARCHIV == None:
        print(f'You need to set a storage location for the RKI-Archiv data in the <set_path_ARCHIV> variable first!')
        sys.exit(0)

if set_path == None:
        print(f'You need to set a storage location to store the data to in the <set_path> variable first!')
        sys.exit(0)

store_path = set_path + str_today + '/'
path = Path(store_path)
path.mkdir(parents=True, exist_ok=True)

print(f'###### 7-DIC v{version} /7-Day-Incidence comparison/ based on Covid-19 RKI data  - created by EffWee #####')

#Define Landkreis Data
id_lk = input("Please type in a Landkreis ID (if available) - leave empty to specify Landkreis by name: ")
if id_lk =="":
        lk = input("Please type in a Landkreis name: ")

### Define a short cut for your home LK if needed
        if lk == "":
                lk = "Erlangen-Höchstadt"
                print(f'{lk} will be used for further processing.')

        if lk == "":
                print(f'You need to define a LK name.')
                input('Press key to exit...')
                sys.exit(0)

#Define Start Date for Evaluation
start_date = d.datetime(2020,3,1)
date_RKI_Archiv = d.datetime(2020,11,18)
end_RKI_Archiv = today


RKI_COVID19_url = 'https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv'
RKI_Landkreise_url = 'https://opendata.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0.csv'
RKI_Archiv_file = set_path_ARCHIV + d.datetime.strftime(date_RKI_Archiv, '%Y%m%d') + '.csv'

proc_start = d.datetime.today()
print(f'Start processing at {proc_start}')
if id_lk:
        print(f'Searching for {id_lk} in RKI database...')
if lk:
        print(f'Searching for {lk} in RKI database...')

f_input_RKI_LK = requests.get(RKI_Landkreise_url).content
csv_reader_RKI_LK = csv.DictReader(io.StringIO(f_input_RKI_LK.decode('utf-8')))
for row in csv_reader_RKI_LK:
        if row["GEN"] == lk or row["RS"] == id_lk:
                id_lk = row["RS"]
                lk_plus = row["county"]
                adm_unit = row["AdmUnitId"]
                lk_einwohner = int(row["EWZ"])
                update = row["last_update"]
                STI_Actual = float(row["cases7_per_100k"])
                found_lk += 1
                print(f'Found {lk_plus} with {id_lk} in RKI-database.\nActual seven-day incidence: {STI_Actual} (updated: {update})')

if found_lk == 0:
        print(f'Did not find {lk} in RKI database. Please check spelling!')
        input(f'Press key to end...')
        sys.exit(0)

if found_lk > 1:
        print(f'Found {found_lk} items with the same Landkreis name {lk}.')
        print(f'Use Landkreis ID instead')
        input(f'Press key to end...')
        sys.exit(0)
        

# Define Output Location & Files
LK_output_file = store_path + id_lk + '_' + str_today + '_RKI-Data_complete.csv'
STI_output_file = store_path + id_lk + '_' + str_today + '_7TI-Data_complete.csv'
STI_output_file_reduced = store_path + id_lk + '_' + str_today + '_7TI-Data_6-weeks.csv'

f_output_LK = open(LK_output_file, 'w', newline ='', encoding='utf-8')
fieldnames_LK = ['Landkreis', 'Meldedatum', 'AnzahlFall']
fieldnames_STI = ['Landkreis', 'Meldedatum', '7-Tage-Inzidenz (tagesaktuell)', '7-Tage-Inzidenz (korrigiert)', 'AnzahlFall']
csv_writer_LK = csv.DictWriter(f_output_LK, fieldnames = fieldnames_LK)

line_count = 0
day = 0
kum = 0
sti = 0.0
sti_null = 0.0
stf = 0
progress = 0

date_dict = {"2020/03/01" : 0}
end_date = today
pandemic = end_date - start_date
csv_writer_LK.writeheader()


print(f'Requesting data from {RKI_COVID19_url}')
f_input_RKI_header = requests.head(RKI_COVID19_url)
print(f'Data was last modified: {f_input_RKI_header.headers["Last-Modified"]}')
f_input_RKI = requests.get(RKI_COVID19_url).content
csv_reader_RKI = csv.DictReader(io.StringIO(f_input_RKI.decode('utf-8')))



print(f'Extracting data for {lk_plus} with ID {id_lk}. Population: {lk_einwohner}')

for row in csv_reader_RKI:
        if row["IdLandkreis"] == id_lk:
                csv_writer_LK.writerow({'Landkreis': row["Landkreis"], 'Meldedatum': row["Meldedatum"], 'AnzahlFall': row["AnzahlFall"]})
f_output_LK.close()
print(f'File {LK_output_file} created')
f_output_STI = open(STI_output_file, 'w', newline ='', encoding='utf-8')
f_output_STI_reduced = open(STI_output_file_reduced, 'w', newline='', encoding='utf-8')
csv_writer_STI = csv.DictWriter(f_output_STI, fieldnames = fieldnames_STI)
csv_writer_STI.writeheader()
csv_writer_STI_red = csv.DictWriter(f_output_STI_reduced, fieldnames = fieldnames_STI)
csv_writer_STI_red.writeheader()
print("\t|--10%|--20%|--30%|--40%|--50%|--60%|--70%|--80%|--90%|-100%|")
print("\t      |     |     |     |     |     |     |     |     |     |")   
print("\t|-----------------------------------------------------------|", end="", flush=True)
while day < pandemic.days:
        date = start_date + d.timedelta(day)
        date_RKI = date + d.timedelta(1)
        if (day / pandemic.days *100 > 10 and progress == 0):
                print("\r\t|######", end="", flush=True)
                progress = 1
        if (day / pandemic.days *100 > 20 and progress == 1):
                print("######", end="", flush=True)
                progress = 2
        if (day / pandemic.days *100 > 30 and progress == 2):
                print("######", end="", flush=True)
                progress = 3
        if (day / pandemic.days *100 > 40 and progress == 3):
                print("######", end="", flush=True)
                progress = 4
        if (day / pandemic.days *100 > 50 and progress == 4):
                print("######", end="", flush=True)
                progress = 5
        if (day / pandemic.days *100 > 60 and progress == 5):
                print("######", end="", flush=True)
                progress = 6
        if (day / pandemic.days *100 > 70 and progress == 6):
                print("######", end="", flush=True)
                progress = 7
        if (day / pandemic.days *100 > 80 and progress == 7):
                print("######", end="", flush=True)
                progress = 8
        if (day / pandemic.days *100 > 90 and progress == 8):
                print("######", end="", flush=True)
                progress = 9
        if (day / pandemic.days *100 > 99 and progress == 9):
                print("#####|")
                progress = 10

        f_input_LK = open(LK_output_file, encoding='utf-8')
        csv_reader_LK = csv.DictReader(f_input_LK)
        for row in csv_reader_LK:
                if (d.datetime.strptime(row["Meldedatum"], '%Y/%m/%d %H:%M:%S+00') == date):
                        af = int(row["AnzahlFall"])
                        kum = kum + af
                        line_count += 1
        date_dict[d.datetime.strftime(date, '%Y/%m/%d')] = kum
        ref_date = date - d.timedelta(7)
        if date - d.timedelta(7) < start_date:
                stf = kum
        else:
                stf = date_dict[d.datetime.strftime(date, '%Y/%m/%d')] - date_dict[d.datetime.strftime(ref_date, '%Y/%m/%d')]
        if d.datetime.strftime(date_RKI_Archiv, '%Y%m%d') == d.datetime.strftime(end_RKI_Archiv, '%Y%m%d'):
                date_RKI_Archiv = d.datetime(2020,4,1)

        if d.datetime.strftime(date_RKI, '%Y%m%d') == d.datetime.strftime(date_RKI_Archiv, '%Y%m%d'):
                if Path(RKI_Archiv_file).is_file():
                        f_input_RKI_Archiv = open(RKI_Archiv_file, encoding='utf-8')
                        csv_reader_RKI_Archiv = csv.DictReader(f_input_RKI_Archiv)
                        for row_i in csv_reader_RKI_Archiv:
                                if (row_i["LKNR"] == id_lk):
#                                if (row_i["Landkreis"] == lk_plus):
                                        sti_null = row_i["Inzidenz"]
                else:
                        print(f'Did not find an archived RKI seven-day-incidence value {date_RKI}. Skip date.')
                        sti_null = None
                date_RKI_Archiv = date_RKI_Archiv + d.timedelta(1)
                f_input_RKI_Archiv.close()
                RKI_Archiv_file = set_path_ARCHIV + d.datetime.strftime(date_RKI_Archiv, '%Y%m%d') + '.csv'

        elif d.datetime.strftime(date_RKI, '%Y%m%d') == d.datetime.strftime(today, '%Y%m%d'):
                sti_null = STI_Actual

        else:
                sti_null = None
        sti = stf/lk_einwohner*100000
        csv_writer_STI.writerow({'Landkreis': lk, 'Meldedatum': d.datetime.strftime(date_RKI, '%Y/%m/%d'), '7-Tage-Inzidenz (tagesaktuell)': sti_null, '7-Tage-Inzidenz (korrigiert)': sti, 'AnzahlFall': date_dict[d.datetime.strftime(date, '%Y/%m/%d')]})
        if date_RKI >= today - d.timedelta(42):
                csv_writer_STI_red.writerow({'Landkreis': lk, 'Meldedatum': d.datetime.strftime(date_RKI, '%Y/%m/%d'), '7-Tage-Inzidenz (tagesaktuell)': sti_null, '7-Tage-Inzidenz (korrigiert)': sti, 'AnzahlFall': date_dict[d.datetime.strftime(date, '%Y/%m/%d')]})
        day += 1
        f_input_LK.close()
f_output_STI.close()
f_output_STI_reduced.close()
proc_end = d.datetime.today()
proc_duration = proc_end - proc_start
print(f'End processing at {proc_end}')
print(f'Total processing time: {proc_duration}')
print(f'File {STI_output_file} created')
print(f'Processed {line_count} lines.')
print(f'Cumulated cases: {kum}')
print(f'Total days of pandemic: {day}')
input(f'Press key to end...')

