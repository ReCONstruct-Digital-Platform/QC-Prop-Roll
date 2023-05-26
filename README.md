# QC Property Assessment Roll Parser

Parse the QC Property Roll XML and store them in a PostgreSQL database.

## Data

Download the and unzip the XML data for the 2022 roll here:
https://www.donneesquebec.ca/recherche/dataset/roles-d-evaluation-fonciere-du-quebec/resource/93548503-59b0-4721-b1a8-df98f447aa5c

Note: depending on changes to the data structure, the code will probably work with roll data from other years, but was only tested on the 2022 data.

Download the GIS data here: https://www.donneesquebec.ca/recherche/dataset/roles-d-evaluation-fonciere-du-quebec/resource/6e34aecd-6914-4791-8aff-452e554b9990

We use this data to extract latitude/longitude for each evaluation unit in the roll.