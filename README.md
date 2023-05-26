# QC Property Assessment Roll Parser

Parse the QC Property Roll XML and associated shapefiles and store them in a PostgreSQL database.

## Data

Download the and unzip the XML data for the 2022 roll here:

https://www.donneesquebec.ca/recherche/dataset/roles-d-evaluation-fonciere-du-quebec/resource/93548503-59b0-4721-b1a8-df98f447aa5c

Note: depending on changes to the data structure, the code will probably work with roll data from other years, but was only tested on the 2022 data.

Download the shapefile data here: 

https://www.donneesquebec.ca/recherche/dataset/roles-d-evaluation-fonciere-du-quebec/resource/6e34aecd-6914-4791-8aff-452e554b9990

We use this data to extract latitude/longitude for each evaluation unit in the roll.

## 0. Configure your database and setup the environment

Setup and install a PostgreSQL database. 

Create a `.env` file following the format laid out in `.env.example` and containing your database information.

Setup a virtual environment for the project (optional)

Install dependencies:
```
pip install -r requirements.txt
```

## 1. Parse the XMLs

First parse the XMLs using `parse_xmls.py`. This script runs partitions the XML files between `NUM_WORKERS` parallel processes and processes them, writing out the data to the database.
```
$ python parse_xmls.py -h
usage: parse_xmls.py [-h] [-n NUM_WORKERS] input-folder

positional arguments:
  input-folder          Path to folder containing the roll XML files.

optional arguments:
  -h, --help            show this help message and exit
  -n NUM_WORKERS, --num-workers NUM_WORKERS
                        Number of parallel workers. Defaults to one less than the number of CPUs on the machine.
```

Note: This takes around 3.5 hours using 6 parallel processes on a 6 Core AMD Ryzen 5 4500U 2.375 GHz laptop.


## 2. Parse the SHP files

Now that the database is setup and filled, use `parse_shp.py` to parse the shp files to extract the latitude and longitude values for each unit, and update the database with these values. 

Note: this WILL NOT WORK if run before, as the table won't exist.

```
$ python parse_shp.py -h
usage: parse_shp.py [-h] input_file

positional arguments:
  input_file  Path to the rol_unite_p.shp file

optional arguments:
  -h, --help  show this help message and exit
```


## Future Work
See if doing a first run and gathering the number of units in each file and distributing the XMLs such that each process has approximately the same number of units leads to faster execution time.
Currently, with a random split of XMLs, one process can end up lasting much longer than others if it took a long time procesing some of the very large files.