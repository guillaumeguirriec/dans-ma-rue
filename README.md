# Dans Ma Rue

L'objectif de cet exerice est d'analyser les données de l'application mobile `DansMaRue - Paris` avec `Elasticsearch`.

`DansMaRue - Paris` est une application mobile permettant de transmettre les anomalies de la voie publique à Paris.
* Play Store : https://play.google.com/store/apps/details?id=fr.paris.android.signalement&hl=fr
* App Store : https://apps.apple.com/fr/app/dansmarue-paris/id662045577

## Pré-requis

* [Elasticsearch](https://www.elastic.co/fr/downloads/elasticsearch) (version 7.5.x)
* [Kibana](https://www.elastic.co/fr/downloads/kibana) (version 7.5.x)
* [Node](https://nodejs.org/en/download/) (version 10.x)

## Dataset

Les données sont disponibles sur la plateforme Open Data de Paris : https://opendata.paris.fr/explore/dataset/dans-ma-rue

La version du dataset utilisée est celle du vendredi 31 janvier 2020, dont une copie se trouve au format `zip` dans le dossier `dataset`.

## Import des données dans Elasticsearch

Complétez le script `import.js` afin d'insérer dans Elasticsearch l'ensemble des anomalies depuis le CSV `dataset/dans-ma-rue.csv`.

Voici le format de document conseillé : 

```json
{
    "@timestamp" : "2019-06-24",
    "object_id" : "788771",
    "annee_declaration" : "2019",
    "mois_declaration" : "6",
    "type" : "Propreté",
    "sous_type" : "Déchets et/ou salissures divers",
    "code_postal" : "75002",
    "ville" : "Paris 2",
    "arrondissement" : "2",
    "prefixe" : "A",
    "intervenant" : "DPE",
    "conseil_de_quartier" : "GAILLON - VIVIENNE",
    "location" : "48.8661199955,2.34107070196"
}
```

## Analyse des données

Complétez le code des `services` afin de répondre aux besoins suivants : 

### Compter le nombre d'anomalies entre deux dates

Voici un exemple pour les anomalies entre le 1er janvier 2016 (inclus) et le 1er janvier 2018 (exclu).

La requête `curl -X GET http://localhost:3000/_count?from=2016-01-01&to=2018-01-01`

doit produire le résultat suivant : 

```json
{
  "count": 162384
}
```


### Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné

Voici un exemple pour les anomalies autour de la Tour Eiffel dans un rayon de 500 mètres

La requête `curl -X GET http://localhost:3000/around/_count?lat=48.858053&lon=2.294289&radius=500m`

doit produire le résultat suivant : 

```json
{
  "count": 2052
}
```

### Compter le nombre d'anomalies par arrondissement

La requête `curl -X GET http://localhost:3000/stats/_byArrondissement`

doit produire le résultat suivant : 

```json
[
   {
      "arrondissement":"18",
      "count":104627
   },
   {
      "arrondissement":"15",
      "count":79574
   },
   {
      "arrondissement":"11",
      "count":77671
   },
   {
      "arrondissement":"10",
      "count":69880
   },
   {
      "arrondissement":"17",
      "count":65323
   },
   {
      "arrondissement":"19",
      "count":64233
   },
   {
      "arrondissement":"20",
      "count":62320
   },
   {
      "arrondissement":"12",
      "count":49544
   },
   {
      "arrondissement":"16",
      "count":46063
   },
   {
      "arrondissement":"13",
      "count":42754
   },
   {
      "arrondissement":"14",
      "count":30938
   },
   {
      "arrondissement":"9",
      "count":27736
   },
   {
      "arrondissement":"3",
      "count":20936
   },
   {
      "arrondissement":"5",
      "count":20176
   },
   {
      "arrondissement":"4",
      "count":19351
   },
   {
      "arrondissement":"8",
      "count":15961
   },
   {
      "arrondissement":"7",
      "count":15763
   },
   {
      "arrondissement":"6",
      "count":15256
   },
   {
      "arrondissement":"2",
      "count":14604
   },
   {
      "arrondissement":"1",
      "count":13391
   }
]
```


### Trouver le top 5 des types et sous types d'anomalies

La requête `curl -X GET http://localhost:3000/stats/_byType`

doit produire le résultat suivant : 

```json
[
   {
      "type":"Objets abandonnés",
      "count":308504,
      "sous_types":[
         {
            "sous_type":"Autres objets encombrants abandonnés",
            "count":259849
         },
         {
            "sous_type":"Gravats ou déchets de chantier",
            "count":28371
         },
         {
            "sous_type":"Sacs de déchets ménagers",
            "count":12149
         },
         {
            "sous_type":"Cartons",
            "count":3561
         },
         {
            "sous_type":"Objets infestés de punaises de lit",
            "count":1593
         }
      ]
   },
   {
      "type":"Graffitis, tags, affiches et autocollants",
      "count":200329,
      "sous_types":[
         {
            "sous_type":"Graffitis sur mur, façade sur rue, pont",
            "count":156591
         },
         {
            "sous_type":"Affiches ou autocollants sur mur, façade sur rue, pont",
            "count":11999
         },
         {
            "sous_type":"Affiches, autocollants ou graffitis sur autres supports:Autre mobilier urbain",
            "count":9073
         },
         {
            "sous_type":"Affiches, autocollants ou graffitis sur autres supports:Panneau routier ou signalétique",
            "count":4884
         },
         {
            "sous_type":"Affiches, autocollants ou graffitis sur autres supports:[] Mobilier électrique Enedis",
            "count":4027
         }
      ]
   },
   {
      "type":"Propreté",
      "count":133260,
      "sous_types":[
         {
            "sous_type":"Déchets et/ou salissures divers",
            "count":84244
         },
         {
            "sous_type":"Épanchement d'urine",
            "count":12614
         },
         {
            "sous_type":"Propreté des équipements de collecte des déchets:Corbeille de rue sale ou débordante",
            "count":9914
         },
         {
            "sous_type":"Déjections canines",
            "count":9431
         },
         {
            "sous_type":"Propreté du sol:Déchets et/ou salissures en pied d'arbre, dans une jardinière ou un espace végétalisé",
            "count":2774
         }
      ]
   },
   {
      "type":"Autos, motos, vélos...",
      "count":95989,
      "sous_types":[
         {
            "sous_type":"Automobile ou autre véhicule motorisé en stationnement gênant",
            "count":55291
         },
         {
            "sous_type":"Deux-roues motorisé en stationnement gênant",
            "count":21304
         },
         {
            "sous_type":"Épave de vélo",
            "count":9359
         },
         {
            "sous_type":"Épave d'automobile ou d'un autre véhicule motorisé",
            "count":6005
         },
         {
            "sous_type":"Épave de Vélib'",
            "count":2260
         }
      ]
   },
   {
      "type":"Voirie et espace public",
      "count":56423,
      "sous_types":[
         {
            "sous_type":"Trottoirs:Revêtement manquant",
            "count":7561
         },
         {
            "sous_type":"Chaussées:Affaissement, trou, bosse, pavé arraché",
            "count":7312
         },
         {
            "sous_type":"Trottoirs:Affaissement, trou, bosse, pavé arraché",
            "count":6655
         },
         {
            "sous_type":"Panneaux routiers:Panneau endommagé",
            "count":4959
         },
         {
            "sous_type":"Gênes à la circulation des piétons:Autre mobilier urbain gênant",
            "count":4838
         }
      ]
   }
]
```

### Trouver le top 10 des mois avec le plus d'anomalies

La requête `curl -X GET http://localhost:3000/stats/_byMonth`

doit produire le résultat suivant : 

```json
[
   {
      "month":"07/2019",
      "count":56641
   },
   {
      "month":"06/2019",
      "count":46267
   },
   {
      "month":"05/2019",
      "count":44192
   },
   {
      "month":"04/2019",
      "count":41700
   },
   {
      "month":"03/2019",
      "count":41374
   },
   {
      "month":"02/2019",
      "count":34583
   },
   {
      "month":"12/2018",
      "count":30420
   },
   {
      "month":"01/2019",
      "count":29196
   },
   {
      "month":"10/2018",
      "count":28674
   },
   {
      "month":"09/2018",
      "count":26390
   }
]
```

### Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté

La requête `curl -X GET http://localhost:3000/stats/proprete/_ByArrondissement`

doit produire le résultat suivant : 

```json
[
   {
      "arrondissement":"18",
      "count":20008
   },
   {
      "arrondissement":"10",
      "count":12121
   },
   {
      "arrondissement":"19",
      "count":11426
   }
]
```

## Kibana

Dans Kibana, créez un dashboard qui permet de visualiser :

* Le nombre total d'anomalies
* Un histogramme des anomalies réparties par arrondissement
* Un camembert avec le top 5 des types d'anomalies
* Une carte (de type heatmap) de l'ensemble des anomalies

Faites une capture d'écran du dashboard et ajoutez le au dépôt afin qu'il soit visible dans ce readme :

![](./images/dashboard.png?raw=true "Dashboard")

## Pour rendre le travail

Une fois l'ensemble du code complété et le dashboard ajouté, faites une Pull Request !
