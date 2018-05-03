# Dependencies 

Debian/Ubuntu
```sh
$ sudo apt-get install build-essential libboost-python-dev libexpat1-dev zlib1g-dev libbz2-dev
$ python3 -m venv venv
$ venv/bin/pip install -r requirements
```

# Running
```
$ venv/bin/python get-postcodes --osm-data <url of pbf file to parse>
```

File pointed by URL will be automatically downloaded and stored in a file named data.pbf or any other name
defined by --local-data.

Postcodes with centroids will be printed on the output.