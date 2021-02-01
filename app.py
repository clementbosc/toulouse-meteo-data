import argparse
import json
import logging
import re
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any, List

import requests
from google.cloud import pubsub_v1, bigquery
from google.cloud.pubsub_v1 import PublisherClient
from requests import Response, Session


# Imports the Cloud Logging client library
import google.cloud.logging

# Instantiates a client
client = google.cloud.logging.Client()

# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
client.get_default_handler()
client.setup_logging()

logging.basicConfig(level=logging.DEBUG)


class Collector:

    def __init__(self, topic_name, wait_interval):
        self.session: Session = requests.Session()
        self.root_endpoint: str = 'https://data.toulouse-metropole.fr'
        self.topic_name = topic_name
        self.wait_interval = wait_interval
        self.load_config()
        self.load_last_data_params()

    def start(self):
        # infinite loop
        while True:

            # for each station meteo
            for station in self.stations:
                # fetch new data gt last fetched data
                data = self.get_data(station, self.get_last_date(station))
                if len(data) > 0:
                    last_data_date: str = \
                        sorted(
                            filter(
                                lambda i: datetime.strptime(i['heure_de_paris'], '%Y-%m-%dT%H:%M:%S%z').replace(
                                    tzinfo=timezone.utc) <= datetime.now(timezone.utc), data),
                            key=lambda i: datetime.strptime(i['heure_de_paris'], '%Y-%m-%dT%H:%M:%S%z'),
                            reverse=True)[0]['heure_de_paris']
                    self.set_last_date(last_data_date, station)
                    for row in data:
                        self.push_to_pubsub(self.clean_row(row, station))

            time.sleep(self.wait_interval)

    def get_data(self, slug, start_date, max_rows=100):

        url: str = f"{self.root_endpoint}/api/v2/catalog/datasets/{slug}/records?where=(heure_de_paris>'" \
                   f"{urllib.parse.quote_plus(start_date)}' AND heure_de_paris<'" \
                   f"{urllib.parse.quote_plus(datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S%z'))}')&rows={max_rows}"
        loop: bool = True
        data: List[Any] = []

        while loop:
            logging.debug(f'Fetching {url}')
            r: Response = self.session.get(url)
            if r.status_code != 200:
                logging.warning(f'HTTP {r.status_code} : {r.content}')

            content: Any = r.json()
            if 'total_count' in content and content.get('total_count') > 0:
                next_candidates: List[str] = [l['href'] for l in content.get('links') if l['rel'] == 'next']
                url = next_candidates[0] if len(next_candidates) > 0 else None
                loop = url is not None
                data.extend([record.get('record').get('fields') for record in content.get('records')])
            else:
                loop = False

        return data

    def load_config(self):
        # todo load the yaml file in vars
        self.stations = ['21-station-meteo-cugnaux-general-de-gaulle',
                         '37-station-meteo-toulouse-universite-paul-sabatier']

        if not hasattr(self, 'bq_client') or self.bq_client is None:
            self.bq_client: bigquery.Client = bigquery.Client()

        query = 'select distinct slug from meteo.stations'
        query_job = self.bq_client.query(query)
        self.stations = [r['slug'] for r in query_job]

    def load_last_data_params(self, using_bq=True):
        default_last_data: str = '2019-01-01T00:00:00'

        self.last_data = {}
        for station in self.stations:
            if station not in self.last_data:
                self.last_data[station] = default_last_data

        if using_bq:
            if not hasattr(self, 'bq_client') or self.bq_client is None:
                self.bq_client: bigquery.Client = bigquery.Client()

            query = 'select max(heure_de_paris) as last_date, station from `meteo.observations` group by station'
            query_job = self.bq_client.query(query)

            for row in query_job:
                if row['station'] in self.last_data:
                    self.last_data[row['station']] = row['last_date'].strftime('%Y-%m-%dT%H:%M:%S%z')

    def get_last_date(self, station):
        return self.last_data[station]

    def set_last_date(self, last_data_date, station):
        self.last_data.update({station: last_data_date})

    def clean_row(self, row, station):
        date_regex = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'

        heure_de_paris = row.get('heure_de_paris') if row.get('heure_de_paris', None) is not None and re.match(
            date_regex, row.get('heure_de_paris', None)) else None

        if not heure_de_paris:
            logging.warning('Missing or invalid field heure_de_paris !')

        new_row = {
            "id": row.get('id', None),  # 84,
            "data": row.get('data', None),  # "a8a4b8f2f2f2f2f2f2f2f2f3",
            "numero_de_message": row.get('numero_de_message', None),  # 71,
            # "false_message": row.get('false_message', None), #"267125272015603",
            "station": station,

            "heure_de_paris": heure_de_paris,  # "2030-05-04T17:30:00+00:00",

            # meteo attributes
            "force_rafale_max": row.get('force_rafale_max', None),  # 60,
            "pluie_intensite_max": round(row.get('pluie_intensite_max'), 9) if row.get('pluie_intensite_max',
                                                                                       None) is not None else None,
            # 2.2,
            "pression_a_decoder": row.get('pression_a_decoder', None),  # 151,
            "type_de_station": row.get('type_de_station', None),  # "sous station",
            "force_du_vecteur_de_vent_max": row.get('force_du_vecteur_de_vent_max', None),  # 60,
            "temperature_en_degre_c": round(row.get('temperature_en_degre_c'), 9) if row.get('temperature_en_degre_c',
                                                                                       None) is not None else None,  # 25.12,
            "direction_du_vecteur_vent_moyen": row.get('direction_du_vecteur_vent_moyen', None),  # 94,
            "direction_du_vecteur_moyen_de_vent": row.get('direction_du_vecteur_moyen_de_vent', None),  # 47,
            "pluie": round(row.get('pluie'), 9) if row.get('pluie', None) is not None else None,  # 9.4,
            "temperature_partie_entiere": row.get('temperature_partie_entiere', None),  # 75,
            "pluie_en_augets": row.get('pluie_en_augets', None),  # 47,
            "type_station": row.get('type_station', None),  # 51,
            "force_moyenne_du_vecteur_vent": row.get('force_moyenne_du_vecteur_vent', None),  # 23,
            "direction_du_vecteur_de_vent_max_en_degres": row.get('direction_du_vecteur_de_vent_max_en_degres', None),
            # 202.5,
            "temperature_decodee": row.get('temperature_decodee', None),  # 25,
            "pluie_la_plus_intense_durant_1_min": row.get('pluie_la_plus_intense_durant_1_min', None),  # 11,
            "pression": row.get('pression', None),  # 105100,
            "temperature_partie_decimale": row.get('temperature_partie_decimale', None),  # 12,
            "direction_du_vecteur_de_vent_max": row.get('direction_du_vecteur_de_vent_max', None),  # 9,
            "humidite": row.get('humidite', None),  # 94,
            "force_moyenne_du_vecteur_de_vent": row.get('force_moyenne_du_vecteur_de_vent', None)  # 23,
        }
        return new_row

    def push_to_pubsub(self, row):
        if not hasattr(self, 'pubsub_publisher') or self.pubsub_publisher is None:
            self.pubsub_publisher: PublisherClient = pubsub_v1.PublisherClient()

        self.pubsub_publisher.publish(self.topic_name, json.dumps(row).encode("utf-8"))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic_name", type=str, default="projects/toulouse-meteo-data/topics/meteo-observations")
    parser.add_argument("--wait_interval", type=int, help='Wait interval in seconds', required=True)

    args = parser.parse_args()
    c = Collector(args.topic_name, args.wait_interval)
    c.start()
