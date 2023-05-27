# QC Property Assessment Roll Parser

Parse the QC Property Roll XML and associated shapefiles and store them in a PostgreSQL database.

## Data

### Property Roll XML data

Download and unzip [the XML data for the 2022 roll](https://www.donneesquebec.ca/recherche/dataset/roles-d-evaluation-fonciere-du-quebec/resource/93548503-59b0-4721-b1a8-df98f447aa5c). There is one XML file per QC municipality.

Note: depending on changes to the XML structure, this code will probably work with data from other years, but was only tested on the 2022 data.

Download and unzip the [GIS shapefile data](https://www.donneesquebec.ca/recherche/dataset/roles-d-evaluation-fonciere-du-quebec/resource/6e34aecd-6914-4791-8aff-452e554b9990). We use this to extract latitude/longitude for each evaluation unit in the roll.


There is a lot of documentation on the same website (unfortunately only in French it seems), notably:
- XML field inventory [(link)](https://www.donneesquebec.ca/recherche/dataset/061c8cb7-ca4e-45be-a990-61fce7e7d2dc/resource/427a72a7-f34c-495b-aa23-9de71a84a066/download/repertoire-des-renseignements-prescrits-du-role-devaluation-fonciere-version-2.5.pdf)
- XML field descriptions, with definition of various categorical variables [(link)](https://www.donneesquebec.ca/recherche/dataset/061c8cb7-ca4e-45be-a990-61fce7e7d2dc/resource/6f2599be-e49d-4b9a-8702-b12ad0f56141/download/gui_donneesrolesformatouvert_vf20220627.pdf)
- Full Manual for the QC property roll - this document is extensive and mostly irrelevant, but contains some information that could be intersting. [(link)](https://www.mamh.gouv.qc.ca/fileadmin/publications/evaluation_fonciere/manuel_evaluation_fonciere/2022/MEFQ_2022.pdf).


## 0. Configure your database and setup the environment

Setup and install a [PostgreSQL](https://www.postgresql.org/) database. This code may work with other SQL databases, but was only tested with Postgres.

Create a `.env` file following the format laid out in `.env.example` and containing your database information. I've assumed localhost:5432, so you will need to add a host and port value to this file (or hardcode them) if your setup is different. 

Setup a virtual environment for the project (optional):
```
python -m venv .venv
.venv\Scripts\activate      # on Windows
source .venv/bin/activate   # on UNIX
```

Install the project dependencies:
```
pip install -r requirements.txt
```

## 1. Parse the XMLs

First parse the XMLs using `parse_xmls.py`. This script partitions the XML files between `NUM_WORKERS` parallel processes and processes them, writing out to the database. We keep most fields, resolve some of them to human-readable values using the maps in `utils\qc_roll_mapping.py` and concatenate some to form the full address or the provincial ID, for example.

If you want to drop certain fields, or add new ones, you will need to modify the table creation statement, the table insert statement and the parsing code.

```
$ python parse_xmls.py -h
usage: parse_xmls.py [-h] [-n NUM_WORKERS] [-t] xml_folder

positional arguments:
  xml_folder            Path to folder containing the roll XML files.

optional arguments:
  -h, --help            show this help message and exit
  -n NUM_WORKERS, --num-workers NUM_WORKERS
                        Number of parallel workers. Defaults to one less than the number of CPUs on the machine.
  -t, --test            Run in testing mode on a few XMLs
```

Note: This takes around 1.5 hours using 6 parallel processes on a 6 Core AMD Ryzen 5 4500U 2.375 GHz laptop.


## 2. Parse the SHP files

Now that the database is setup and filled, use `parse_shp.py` to parse the shapefiles to extract the latitude and longitude values for each unit, and update the database with these values. 

Note: this WILL NOT WORK if run before parsing the XMLs, as the table won't exist.

```
$ python parse_shp.py -h
usage: parse_shp.py [-h] input_file

positional arguments:
  input_file  Path to the rol_unite_p.shp file

optional arguments:
  -h, --help  show this help message and exit
```

This script uses a single process and took about 15min on my laptop.


## Future Work
See if doing a first run and gathering the number of units in each file and distributing the XMLs such that each process has approximately the same number of units leads to faster execution time.
Currently, with a random split of XMLs, one process can end up lasting much longer than others if it took a long time procesing some of the very large files.