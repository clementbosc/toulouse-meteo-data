import argparse
import itertools
import json
import logging
import re
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any, List

import pytz
import requests
from google.cloud import pubsub_v1, bigquery
from google.cloud.bigquery import QueryJob
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
        self.topic_name = topic_name
        self.wait_interval = wait_interval

        self.load_last_data_params()

    def test(self, row):
        # lambda x: x['date_debut'] > self.last_data
        print(row['date_debut'], self.last_data)
        return row['date_debut'] > self.last_data

    def start(self):
        # infinite loop
        while True:
            data = self.get_data()
            print(len(data))

            if len(data) > 0:
                last_data_date_candidates = list(sorted(data, key=lambda i: i['date_debut'], reverse=True))
                last_data_date = last_data_date_candidates[0]['date_debut']
                self.set_last_date(last_data_date)

                for row in data:
                    row.update({'date_debut': row['date_debut'].strftime('%Y-%m-%d %H:%M:%S'),
                                'date_fin': row['date_fin'].strftime('%Y-%m-%d %H:%M:%S')})
                    self.push_to_pubsub(row)

            time.sleep(self.wait_interval)

    def get_data(self):
        res_per_page = 1000

        all_data = []

        # 1. first get total_results
        url = "https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/ArcGIS/rest/services/Mesure_horaire_(30j)_Region_Occitanie_Polluants_Reglementaires_1/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryPoint&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=true&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token="
        r: Response = self.session.get(url)
        total_count = r.json().get('count')

        loop: bool = True
        i = 0
        while loop:
            url = f"https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/ArcGIS/rest/services/Mesure_horaire_(30j)_Region_Occitanie_Polluants_Reglementaires_1/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryPoint&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=date_debut+desc&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset={i*res_per_page}&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token="
            r: Response = self.session.get(url)
            if r.status_code != 200:
                logging.warning(f'HTTP {r.status_code} : {r.content}')

            content: Any = r.json()
            data = list(map(self.clean_row, content.get('features')))

            if data[-1].get('date_debut') < self.last_data:
                loop = False

            all_data.extend(data)
            i += 1

        return list(filter(lambda x: x['date_debut'] > self.last_data, all_data))

        # 2. for each offset part
        # for i in range(0, (total_count // 1000) + 1):
        #     url = f"https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/ArcGIS/rest/services/Mesure_horaire_(30j)_Region_Occitanie_Polluants_Reglementaires_1/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryPoint&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset={i*res_per_page}&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token="
#
        #     logging.debug(f'Fetching {url}')
        #     r: Response = self.session.get(url)
        #     if r.status_code != 200:
        #         logging.warning(f'HTTP {r.status_code} : {r.content}')
#
        #     content: Any = r.json()
        #     all_data.extend(content.get('features'))
        # return all_data

    def load_last_data_params(self, using_bq=True):
        self.last_data = None
        if using_bq:
            if not hasattr(self, 'bq_client') or self.bq_client is None:
                self.bq_client: bigquery.Client = bigquery.Client()

            query = 'select max(date_debut) as last_date from `atmo_pollution.observations`'
            query_job: QueryJob = self.bq_client.query(query)

            for row in query_job:
                self.last_data = row['last_date']

        if not self.last_data:
            default_last_data: datetime = datetime.strptime('2019-01-01T00:00:00', '%Y-%m-%dT%H:%M:%S')
            default_last_data = default_last_data.replace(tzinfo=timezone.utc)
            self.last_data: datetime = default_last_data

    def set_last_date(self, last_data_date: datetime):
        self.last_data: datetime = last_data_date

    def clean_row(self, row):
        attributes = row.get('attributes')

        date_debut = datetime.utcfromtimestamp(attributes.get('date_debut', None) // 1000)
        date_debut = datetime(
            year=date_debut.year,
            month=date_debut.month,
            day=date_debut.day,
            hour=date_debut.hour,
            minute=date_debut.minute,
            second=date_debut.second,
            tzinfo=pytz.UTC)
        date_fin = datetime.utcfromtimestamp(attributes.get('date_fin', None) // 1000)
        date_fin = datetime(
            year=date_fin.year,
            month=date_fin.month,
            day=date_fin.day,
            hour=date_fin.hour,
            minute=date_fin.minute,
            second=date_fin.second,
            tzinfo=pytz.UTC)

        new_row = {
            "nom_dept": attributes.get('nom_dept', None),
            "nom_com": attributes.get('nom_com', None),
            "insee_com": attributes.get('insee_com', None),
            "nom_station": attributes.get('nom_station', None),
            "code_station": attributes.get('code_station', None),
            "typologie": attributes.get('typologie', None),
            "influence": attributes.get('influence', None),
            "nom_poll": attributes.get('nom_poll', None),
            "id_poll_ue": attributes.get('id_poll_ue', None),
            "valeur": attributes.get('valeur', None),
            "unite": attributes.get('unite', None),
            "metrique": attributes.get('metrique', None),
            # "date_debut_tsp": attributes.get('date_debut', None),
            # "date_fin_tsp": attributes.get('date_fin', None),
            # "date_debut": datetime.utcfromtimestamp(attributes.get('date_debut', None) // 1000),
            # "date_fin": datetime.utcfromtimestamp(attributes.get('date_fin', None) // 1000),
            "date_debut": date_debut,
            "date_fin": date_fin,
            "statut_valid": attributes.get('statut_valid', None),
            "x_l93": attributes.get('x_l93', None),
            "y_l93": attributes.get('y_l93', None),
            "geometry": {
                "x": None, # round(row.get('geometry', {}).get('x', None), 9) if row.get('geometry', {}).get('x', None) is not None else None,
                "y": None, # round(row.get('geometry', {}).get('y', None), 9) if row.get('geometry', {}).get('y', None) is not None else None
            }
        }
        return new_row

    def push_to_pubsub(self, row):
        if not hasattr(self, 'pubsub_publisher') or self.pubsub_publisher is None:
            self.pubsub_publisher: PublisherClient = pubsub_v1.PublisherClient()

        self.pubsub_publisher.publish(self.topic_name, json.dumps(row).encode("utf-8"))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic_name", type=str, default="projects/toulouse-meteo-data/topics/air-observations")
    parser.add_argument("--wait_interval", type=int, help='Wait interval in seconds', required=True)
    args = parser.parse_args()
    c = Collector(args.topic_name, args.wait_interval)
    c.start()
