# Drizzle_Adri_Allebasis

Included are Python Scripts to evaluate RKI data.

### RKI-Archiv-6-3.py
Downloads xlsx file from RKI server including archived seven day incidence values for all german counties and creates daily csv-files to be used in further scripts.

### STI_Routine_URL-7-2.py
Calculates the seven day incidence based on most recent RKI data over the complete pandemic time frame and compares to "official" seven day incidence based on RKI archive. 
Runs only if RKI-Archive-x-x.py is run first.

### LK-Quality-3-2.py
Calculates a Quality indicator for the seven day incidence in all counties in germany based on the last 4 weeks. THe indicator is basically the area which is spun between the two curves of the seven day incidence based on RKI Archiv and based on up-to-date case count. [to be explained better soon]

