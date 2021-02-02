# toulouse-meteo-data

This repo contains two collectors fetching data on a public API on a recurrent interval.
When new data is collected, it is sent to GCP via PubSub.

* meteo_collector.py fetch data from https://data.toulouse-metropole.fr
* atmo_collector.py fetch data from https://data-atmo-occitanie.opendata.arcgis.com/

To start the collect, use this command : 
```
python <programm_name> \
    --wait_interval=<sleep time between each API call> \
    [--topic_name=<name of the PubSub topic>]
```

For exemple :
```
python meteo_collector.py \
    --wait_interval=300
```
This command start the collector which checks every 5min for new weather data.