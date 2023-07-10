# SirenSpark

## Todo


### Engine 

* ~~Test avec attribute creator~~
* ~~Test avec attribute mapper~~
* ~~Fonctionnement avec une structure JSON~~
* ~~Attente des opérations dans une structure JSON~~
* ~~Joiner~~
* ~~Évite boucles~~
* ~~GDAL~~
* ~~Shapefiles~~


* Tableaux
* Test avec intersector
* Test avec json_reader
* Opérations dans evaluator
* python caller
* inline querier

### IHM

* Init project


## Readers 

Chaque reader retourne un DF ainsi qu'un dictionnaire types qui contient les colonnes avec leur type ainsi que la clé permettant de passer à l'étape suivante. 

Les types peuvent être :
  * boolean
  * string
  * binary
  * date
  * timestamp
  * double
  * integer
  * decimal
  * float
  * short
  * long
  * geometry


## Usage


1. Install pip dependencies

`sudo apt-get install default-jdk`
`sudo apt-get install libpq-dev`
`sudo apt-get install gdal-bin`

`pip install -r requirements.txt`

2. Run a project

`cd engine`
`python main.py SirenSpark/engine/samples/postgres_reader.json`