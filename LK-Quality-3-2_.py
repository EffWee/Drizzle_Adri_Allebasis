import csv
import datetime as d
import requests
import pandas as pd
import io
import sys
import time
from pathlib import Path

version = "0.3.2"
print(f'###### LK-Quality v{version} /Quality of Incidence Reporting/ based on Covid-19 RKI data  - created by EffWee #####')
today = d.date.today()
str_today = d.datetime.strftime(today, '%Y%m%d')
found_lk = 0
lk = ''
lk_dict = {}
lkq_dict = {}
af = 0
STI = 0.0

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

#Define Time Frame for Evaluation
print('Starting quality evaluation for each Landkreis in Germany considering the last 4 weeks...')
start_date = today - d.timedelta(36)


#str_start_date = input('Please type in start date for evaluation (e.g. 2021-03-01): ')
#if str_start_date == '':
#        str_start_date = '2021-04-01'
#start_date = d.datetime.strptime(str_start_date, '%Y-%m-%d') - d.timedelta(7)

#weeks = input('Please input the number of weeks to be considered for Quality Evaluation (Standard: 4): ')
#if weeks == '':
#       weeks = 4
#evaluation = (weeks + 1) * 7

day = 0
end_date = today
evaluation = end_date - start_date
progress = 0

#start_date = today - d.timedelta(weeks*7)

date_RKI_Archiv = start_date
end_RKI_Archiv = today

#Define Data location (Files & URL)
RKI_COVID19_url = 'https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv'
RKI_Landkreise_url = 'https://opendata.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0.csv'
RKI_Archiv_file = set_path_ARCHIV + d.datetime.strftime(date_RKI_Archiv, '%Y%m%d') + '.csv'

LKQ_output_file = store_path + str_today + '_LK-Quality_complete.csv'

proc_start = d.datetime.today()
print(f'Start processing at {proc_start}')
print(f'Collecting data from {RKI_Landkreise_url}...')
f_input_RKI_LK = requests.head(RKI_Landkreise_url)
print(f'Last update of data: {f_input_RKI_LK.headers["Last-Modified"]}')
f_input_RKI_LK = requests.get(RKI_Landkreise_url).content
csv_reader_RKI_LK = csv.DictReader(io.StringIO(f_input_RKI_LK.decode('utf-8')))


for row in csv_reader_RKI_LK:
        lk_dict = {
                "lk" : row["GEN"],
                "update" : row["last_update"],
                "STI_Actual" : float(row["cases7_per_100k"]),
                "lk_plus" : row["county"],
                "lk_einwohner" : int(row["EWZ"]),
                }
        lkq_dict.update({row["RS"] : lk_dict})
        found_lk += 1
print(f'Found {found_lk} items to process')

print(f'Requesting data from {RKI_COVID19_url}')
f_input_RKI = requests.head(RKI_COVID19_url)
print(f'Last update of data: {f_input_RKI.headers["Last-Modified"]}')
f_input_RKI = requests.get(RKI_COVID19_url).content

print(f'Start calculation of cumulated cases per day...')
proc_start_RKI_data = d.datetime.today()

print("\t|--10%|--20%|--30%|--40%|--50%|--60%|--70%|--80%|--90%|-100%|")
print("\t      |     |     |     |     |     |     |     |     |     |")   
print("\t|-----------------------------------------------------------|", end="", flush=True)

while day < evaluation.days:

        if (day / evaluation.days *100 > 10 and progress == 0):
                print("\r\t|######", end="", flush=True)
                progress = 1
        if (day / evaluation.days *100 > 20 and progress == 1):
                print("######", end="", flush=True)
                progress = 2
        if (day / evaluation.days *100 > 30 and progress == 2):
                print("######", end="", flush=True)
                progress = 3
        if (day / evaluation.days *100 > 40 and progress == 3):
                print("######", end="", flush=True)
                progress = 4
        if (day / evaluation.days *100 > 50 and progress == 4):
                print("######", end="", flush=True)
                progress = 5
        if (day / evaluation.days *100 > 60 and progress == 5):
                print("######", end="", flush=True)
                progress = 6
        if (day / evaluation.days *100 > 70 and progress == 6):
                print("######", end="", flush=True)
                progress = 7
        if (day / evaluation.days *100 > 80 and progress == 7):
                print("######", end="", flush=True)
                progress = 8
        if (day / evaluation.days *100 > 90 and progress == 8):
                print("######", end="", flush=True)
                progress = 9
        if (day / evaluation.days *100 > 99 and progress == 9):
                print("#####|")
                progress = 10
        
        csv_reader_RKI = csv.DictReader(io.StringIO(f_input_RKI.decode('utf-8')))
        date = start_date + d.timedelta(day)

        date_RKI = date + d.timedelta(1)
        str_date_RKI = d.datetime.strftime(date_RKI, "%Y-%m-%d")
        
        for row in csv_reader_RKI:
                Meldedatum = d.datetime.strptime(row["Meldedatum"], '%Y/%m/%d %H:%M:%S+00')
                if (d.datetime.strftime(Meldedatum, '%Y-%m-%d') == d.datetime.strftime(date, '%Y-%m-%d')):        
                        if str_date_RKI in lkq_dict[row["IdLandkreis"]]:
                                lkq_dict[row["IdLandkreis"]][str_date_RKI] += int(row["AnzahlFall"])
                        else:
                                lkq_dict[row["IdLandkreis"]].update({str_date_RKI : int(row["AnzahlFall"])})     
        day += 1

proc_end_RKI_data = d.datetime.today()
proc_RKI_data = proc_end_RKI_data - proc_start_RKI_data
print(f'RKI data processing took: {proc_RKI_data}')
print(f'Start calculating seven day incidences for all regions...')
proc_start_sti_data = d.datetime.today()
for Id in lkq_dict:

        lkq_dict[Id].update({"STI_cor" : {}})
        lkq_dict[Id].update({"STI" : {}})
        day = 0
        while day < evaluation.days - 7:
                af = 0
                date = start_date + d.timedelta(7+day)
                date_RKI = date + d.timedelta(1)
                str_date_RKI = d.datetime.strftime(date_RKI, '%Y-%m-%d')
                RKI_Archiv_file = set_path_ARCHIV + d.datetime.strftime(date_RKI, '%Y%m%d') + '.csv'
                dates_STI = [d.datetime.strftime(date_RKI, '%Y-%m-%d'),
                             d.datetime.strftime(date, '%Y-%m-%d'),
                             d.datetime.strftime(date - d.timedelta(1), '%Y-%m-%d'),
                             d.datetime.strftime(date - d.timedelta(2), '%Y-%m-%d'),
                             d.datetime.strftime(date - d.timedelta(3), '%Y-%m-%d'),
                             d.datetime.strftime(date - d.timedelta(4), '%Y-%m-%d'),
                             d.datetime.strftime(date - d.timedelta(5), '%Y-%m-%d')]

                for i in dates_STI:
                        if i in lkq_dict[Id]:
                                af += lkq_dict[Id][i]
                        else:
                                af += 0

                STI = af / lkq_dict[Id]["lk_einwohner"] * 100000
                lkq_dict[Id]["STI_cor"].update({str_date_RKI : STI})

                if Path(RKI_Archiv_file).is_file():
                        f_input_RKI_Archiv = open(RKI_Archiv_file, encoding='utf-8')
                        csv_reader_RKI_Archiv = csv.DictReader(f_input_RKI_Archiv)
                        for row_i in csv_reader_RKI_Archiv:
                                if (row_i["LKNR"] == Id):
                                        lkq_dict[Id]["STI"].update({str_date_RKI : float(row_i["Inzidenz"])})
                                
                if d.datetime.strftime(date_RKI, '%Y%m%d') == d.datetime.strftime(today, '%Y%m%d'):
                        lkq_dict[Id]["STI"].update({str_date_RKI : float(lkq_dict[Id]["STI_Actual"])}) 
                day +=1
        
proc_end_sti_data = d.datetime.today()
proc_end_sti_data = d.datetime.today()
proc_sti_data = proc_end_sti_data - proc_start_sti_data
print(f'STI data processing took: {proc_sti_data}')
Id = 0
print(f'Calculating Quality indicators...')
f_output_LKQ = open(LKQ_output_file, 'w', newline ='', encoding='utf-8')
fieldnames_LKQ = ['ID-Landkreis',
                  'Landkreis',
                  'Letzte Aktualisierung',
                  '7-Tage-Inzidenz (tagesaktuell)',
                  'Meldequalität MQ',
                  'Mittelwert MQ_quer']
csv_writer_LKQ = csv.DictWriter(f_output_LKQ, fieldnames = fieldnames_LKQ)
csv_writer_LKQ.writeheader()

for Id in lkq_dict:
        lkq_dict[Id].update({"MQ" : {}})
        day = 1
        while day < evaluation.days - 7:
                Err = 0
                date = start_date + d.timedelta(7+day)
                date_RKI = date + d.timedelta(1)
                str_date_RKI = d.datetime.strftime(date_RKI, '%Y-%m-%d')
                str_date = d.datetime.strftime(date, '%Y-%m-%d')
                
                if str_date_RKI in lkq_dict[Id]["STI"] and \
                   str_date in lkq_dict[Id]["STI"] and \
                   str_date_RKI in lkq_dict[Id]["STI_cor"] and \
                   str_date in lkq_dict[Id]["STI_cor"]:
                        
                        
                        Area_STI = (lkq_dict[Id]["STI"][str_date_RKI] + lkq_dict[Id]["STI"][str_date]) / 2
                        Area_STI_cor = (float(lkq_dict[Id]["STI_cor"][str_date_RKI]) + float(lkq_dict[Id]["STI_cor"][str_date])) / 2
                        delta_Area = Area_STI_cor - Area_STI
                        lkq_dict[Id]["MQ"].update({str_date_RKI : delta_Area})
                        

                elif str_date_RKI not in lkq_dict[Id]["STI"]:
                        
                        print(f'On {str_date_RKI}: Did not find STI-data for {lkq_dict[Id]["lk_plus"]} with ID: {Id}')
##                        Err = 1
                        

                elif str_date not in lkq_dict[Id]["STI"]:
                        
                        print(f'On {str_date}: Did not find STI-data for {lkq_dict[Id]["lk_plus"]} with ID: {Id}')
##                        Err = 1
                        

                elif str_date_RKI not in lkq_dict[Id]["STI_cor"]:
                        
                        print(f'On {str_date_RKI}: Did not find STI_cor-data for {lkq_dict[Id]["lk_plus"]} with ID: {Id}')
##                        Err = 1

                elif str_date not in lkq_dict[Id]["STI_cor"]:
                        
                        print(f'On {str_date}: Did not find STI_cor-data for {lkq_dict[Id]["lk_plus"]} with ID: {Id}')
##                        Err = 1
                day += 1
                
        MQ_summe = 0.0
        MQ_quer = 0.0
        count = 0.00000001
        for n in lkq_dict[Id]["MQ"]:
                MQ_summe += lkq_dict[Id]["MQ"][n]
                count += 1
        MQ_quer = MQ_summe / count

        csv_writer_LKQ.writerow({'ID-Landkreis': Id,
                                 'Landkreis': lkq_dict[Id]["lk_plus"],
                                 'Letzte Aktualisierung': lkq_dict[Id]["update"],
                                 '7-Tage-Inzidenz (tagesaktuell)': round(lkq_dict[Id]["STI_Actual"],8),
                                 'Meldequalität MQ': round(MQ_summe,8),
                                 'Mittelwert MQ_quer': round(MQ_quer,8)})

        
f_output_LKQ.close()


proc_end = d.datetime.today()
proc_duration = proc_end - proc_start
print(f'End processing at {proc_end}')
print(f'Total processing time: {proc_duration}')
print(f'Check file {LKQ_output_file} for results.')
input(f'Press key to end...')

