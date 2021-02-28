#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
features = []
with open('query5c.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[1:]:
        row = line.strip('\n').split("\t")
        
        # Uncomment these lines
        lat = row[0]
        lon = row[1]
        speed = row[2]

        #print(lat,lon,speed)

        # skip the rows where speed is missing
        if speed is None or speed == "":
            continue
        
        speed = int(float(speed))
        try:
            latitude, longitude = map(float, (lat, lon))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("data.geojson", "w") as f:
    f.write('%s' % collection)
