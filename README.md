# toulouse-meteo-data


```
id: INTEGER,
data: STRING,
numero_de_message: INTEGER,
station: STRING,
heure_de_paris: TIMESTAMP,
force_rafale_max: INTEGER,
pluie_intensite_max: NUMERIC,
pression_a_decoder: INTEGER,
type_de_station: STRING,
force_du_vecteur_de_vent_max: INTEGER,
temperature_en_degre_c: NUMERIC,
direction_du_vecteur_vent_moyen: INTEGER,
direction_du_vecteur_moyen_de_vent: INTEGER,
pluie: NUMERIC,
temperature_partie_entiere: INTEGER,
pluie_en_augets: INTEGER,
type_station: INTEGER,
force_moyenne_du_vecteur_vent: INTEGER,
direction_du_vecteur_de_vent_max_en_degres: NUMERIC,
temperature_decodee: INTEGER,
pluie_la_plus_intense_durant_1_min: INTEGER,
pression: INTEGER,
temperature_partie_decimale: INTEGER,
direction_du_vecteur_de_vent_max: INTEGER,
humidite: INTEGER,
force_moyenne_du_vecteur_de_vent: INTEGER,
direction_du_vecteur_de_rafale_de_vent_max: INTEGER
```



## unknown fields : 
* direction_du_vecteur_de_rafale_de_vent_max : Angle entre la direction de laquelle vient vent  par rapport a une référence cardinale qui est ici le nord
* temperature: Température moyenne des 15 dernières minutes