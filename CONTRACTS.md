# GeoPrice API Contracts

## Product

### Product prices By Store

Fetch recent product prices by each store filtering by item_uuid or product_uuid, latitude, longitude and radius.

**Method**:  GET

**Endpoint**: `/product/bystore?uuid=<item_uuid | required>&puuid=<product_uuid | conditonally_required>&lat=<latitude | optional>&lng=<longitude | optional>&r=<radius | optional>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| uuid  | Item UUID | required |
| puuid  | Product UUID | required if `uuid` not set |
| lat  | Latitude | optional, default=19.431380 |
| lng  | Longitude | optional, default=-99.133486 |
| r  | Radius | optional, default=10.0 |

**Response:**

```json
[
  {
    "date": "2018-05-09 02:19:27.486000",
    "discount": 0.0,
    "distance": 2.4926858680654145,
    "item_uuid": "cfa07938-fb70-4a86-b099-0a0f650837fa",
    "product_uuid": "af38sd38-ff70-4a86-b099-0sf846s8478c",
    "previous_price": 97.0,
    "price": 97.0,
    "promo": "",
    "source": "chedraui",
    "store": {
      "address": "Anfora 71, Madero, 15360 Ciudad de M\u00e9xico, CDMX, Mexico",
      "delivery_cost": 21.0,
      "delivery_radius": 5.0,
      "delivery_time": "",
      "latitude": 19.435400009155273,
      "longitude": -99.11009979248047,
      "name": "CHEDRAUI M\u00c9XICO ANFORA",
      "postal_code": "00000",
      "store_uuid": "efb8efc2-7b09-11e7-855a-0242ac110005"
        }
    },
    // ...
]
```

### Product prices By Store History

Fetch recent product prices by each store filtering by item_uuid or product_uuid, latitude, longitude, radius and number of prior days (history).

**Method**:  GET

**Endpoint**: `/product/bystore/history?uuid=<item_uuid | required>&puuid=<product_uuid | conditonally_required>&lat=<latitude | optional>&lng=<longitude | optional>&r=<radius | optional>&days=<days | optional>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| uuid  | Item UUID | required |
| puuid  | Product UUID | required if `uuid` not set |
| days | Prior Days | optional, default=7.0 |

**Response:**

```json
{
  "history": {
    "Máximo": [
      {
        "date": "2018-05-03 23:02:04.878000",
        "price": 65.0
      },
      {
        "date": "2018-05-04 23:02:47.097000",
        "price": 65.0
      },
      //...
    ],
    "Mínimo": [
      {
        "date": "2018-05-03 23:02:04.878000",
        "price": 62.0
      },
      {
        "date": "2018-05-04 23:02:47.097000",
        "price": 62.0
      },
      // ...
    ],
    "Promedio": [
      {
        "date": "2018-05-03 23:02:04.878000",
        "price": 64.38408084791534
      },
      {
        "date": "2018-05-04 23:02:47.097000",
        "price": 64.19711049397786
      },
      // ...
    ]
  },
  "history_byretailer": {}
}
```

### Product prices for ticket

Fetch recent group of product prices by each store filtering by item_uuid or product_uuid, latitude, longitude and radius.

**Method**:  POST

**Endpoint**: `/product/ticket`

**Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| uuids | List of Item UUIDs | required |
| puuids  | List of Product UUIDs | required if `uuids` not set |
| lat  | Latitude | optional, default=19.431380 |
| lng  | Longitude | optional, default=-99.133486 |
| r  | Radius | optional, default=10.0 |

**Request Example:**

```json
{
  "uuids": [
    "cfa07938-fb70-4a86-b099-0a0f650837fa",
    "cfa07938-fb70-4a86-b099-0a0f650837fa",
    //...
  ],
  "puuids": [
    "cfa07938-fb70-4a86-b099-0a0f650837fa",
    "cfa07938-fb70-4a86-b099-0a0f650837fa",
    //...
  ],
  "lat": 19.431380, // optional
  "lng": -99.133486, // optional
  "r": 10.0 // optional
}
```

**Response:**

```json
[
  [
    {
      "date": "2018-05-09 02:19:27.486000",
      "discount": 0.0,
      "distance": 2.4926858680654145,
      "item_uuid": "cfa07938-fb70-4a86-b099-0a0f650837fa",
      "product_uuid": "af38sd38-ff70-4a86-b099-0sf846s8478c",
      "previous_price": 97.0,
      "price": 97.0,
      "promo": "",
      "source": "chedraui",
      "store": {
        "address": "Anfora 71, Madero, 15360 Ciudad de M\u00e9xico, CDMX, Mexico",
        "delivery_cost": 21.0,
        "delivery_radius": 5.0,
        "delivery_time": "",
        "latitude": 19.435400009155273,
        "longitude": -99.11009979248047,
        "name": "CHEDRAUI M\u00c9XICO ANFORA",
        "postal_code": "00000",
        "store_uuid": "efb8efc2-7b09-11e7-855a-0242ac110005"
          }
      },
      // ...
  ],
  // ...
]
```

### Product prices By Store History

Fetch recent product prices by each store filtering by item_uuid or product_uuid, latitude, longitude, radius and number of prior days (history).

**Method**:  GET

**Endpoint**: `/product/bystore/history?uuid=<item_uuid | required>&puuid=<product_uuid | conditonally_required>&lat=<latitude | optional>&lng=<longitude | optional>&r=<radius | optional>&days=<days | optional>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| uuid  | Item UUID | required |
| puuid  | Product UUID | required if `uuid` not set |
| days | Prior Days | optional, default=7.0 |

**Response:**

```json
{
  "history": {
    "Máximo": [
      {
        "date": "2018-05-03 23:02:04.878000",
        "price": 65.0
      },
      {
        "date": "2018-05-04 23:02:47.097000",
        "price": 65.0
      },
      //...
    ],
    "Mínimo": [
      {
        "date": "2018-05-03 23:02:04.878000",
        "price": 62.0
      },
      {
        "date": "2018-05-04 23:02:47.097000",
        "price": 62.0
      },
      // ...
    ],
    "Promedio": [
      {
        "date": "2018-05-03 23:02:04.878000",
        "price": 64.38
      },
      {
        "date": "2018-05-04 23:02:47.097000",
        "price": 64.19
      },
      // ...
    ]
  },
  "history_byretailer": {}
}
```

### Product prices of a store

Fetch recent product prices of a given store filtering by store_uuid.

**Method**:  GET

**Endpoint**: `/product/catalogue?r=<retailer | required>&sid=<store_uuid | required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| r | Source/retailer key | required |
| sid  | Store UUID | required |

**Response:**

```json
[
  {
    "date": "Thu, 10 May 2018 08:39:03 GMT",
    "discount": 0.0,
    "item_uuid": "3feef3e9-7f7b-43be-bc63-0a37ee48e3e2",
    "product_uuid": "f5f684e9-7f7b-43be-bc63-0ab4ae8be8ebe",
    "price": 60.5,
    "price_original": 60.5,
    "source": "walmart",
    "store_uuid": "16faeaf4-7ace-11e7-9b9f-0242ac110003"
  },
  // ...
]
```

### Count product prices of a store

Count recent product prices of a given store filtering by store_uuid and time interval.

**Method**:  GET

**Endpoint**: `/product/count_by_store?r=<source | required>&sid=<store_uuid | required>&date_start=<start_date | required>&date_end=<end_date | required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| r | Source/retailer key | required |
| sid  | Store UUID | required |
| date_start  | Starting date (YYYY-MM-DD) | required |
| date_end  | Ending date (YYYY-MM-DD) | required |

**Response:**

```json
{
  "count": 21744,
  "date_end": "2018-05-11",
  "date_start": "2018-05-10",
  "source": "walmart",
  "store_uuid": "16faeaf4-7ace-11e7-9b9f-0242ac110003"
}
```

### Count product prices of a store by last hours

Count recent product prices of a given store filtering by store_uuid and last hours.

**Method**:  GET

**Endpoint**: `/product/count_by_store_hours?r=<source | required>&sid=<store_uuid | required>&last_hours=<last_hours | required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| r | Source/retailer key | required |
| sid  | Store UUID | required |
| last_hours  | Last hours | required |

**Response:**

```json
{
  "count": 21744,
  "date_end": "2018-05-11",
  "date_start": "2018-05-10",
  "source": "walmart",
  "store_uuid": "16faeaf4-7ace-11e7-9b9f-0242ac110003"
}
```

### Product prices of a store (CSV)

Recent product prices of a given store filtering by store_uuid and for the past 48hrs, return as multipart CSV format.

**Method**:  GET

**Endpoint**: `/product/byfile?ret=<source | required>&sid=<store_uuid | required>&stn=<store_name | required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| ret | Source/retailer key | required |
| sid  | Store UUID | required |
| stn  | Store Name | required |

**Response:**

```csv
source,gtin,name,price,promo,store
WALMART,07622210254511,POLVO PARA PREPARAR BEBIDA TANG SABOR FRESA 15 G ,3.299999952316284,,Universidad
WALMART,00780280071944,POLVO PARA PREPARAR BEBIDA ZUKO SABOR GUAYABA 15 G ,3.200000047683716,,Universidad
```

### Retailer prices of products 

Recent product prices of a given retailer filtering by retailer, item_uuid/product_uuid for the past 48hrs, returning as multipart CSV format or JSON.

**Method**:  GET

**Endpoint**: `/product/retailer?retailer=<source | required>&item_uuid=<item_uuid | required>&prod_uuid=<product__uuid | conditional_required>&export=<export | optional>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| retailer | Source/retailer key | required |
| item_uuid  | Item UUID | required |
| prod_uuid  | Product UUID | required if `item_uuid` not set |
| export  | Exporting flag | optional, default=False |

**JSON Response:**

```json
{
  "avg": 65.0,
  "max": 65.0,
  "min": 65.0,
  "prev_avg": 65.0,
  "prev_max": 65.0,
  "prev_min": 65.0,
  "stores": [
    {
      "lat": 25.5075,
      "lng": -103.397,
      "name": "LA ROSITA",
      "price": 65.0
    },
    // ...
  ]
}
```

**Multipart CSV Response:**

```csv
,name,price,lat,lng
0,LA ROSITA,65.0,25.5075,-103.397
1,MIRAMONTES,65.0,19.3172,-99.1256
```

### Compare prices over Retailer-item pairs

Compare prices from a fixed pair retailer-item with additional pairs of other retailer-items

**Method**:  POST

**Endpoint**: `/product/compare/details`

**Request Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| fixed_segment | Principal Segment | required |
| added_segments  | Segments to compare | required |
| date  | Date | optional, default=`today` |

**Example Request:**

```json
{
    "date": "2017-11-01",
    "fixed_segment" : {
        "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
        "retailer": "chedraui"
        },
    "added_segments": [
        { 
            "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
            "retailer": "walmart"
        },
        {
            "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
            "retailer": "soriana"
        }
    ]
}
```

**Response:**

```json
{
  "date": "Mon, 11 Dec 2017 00:00:00 GMT",
  "rows": [
    {
      "fixed": {
        "item_uuid": "930d055d-d781-40bd-8f9c-93e9722046bd",
        "price": 62.0,
        "retailer": "walmart",
        "store": "CERRO DE LA SILLA"
      },
      "segments": [
        {
          "diff": 0.0,
          "dist": 0.0,
          "item_uuid": "930d055d-d781-40bd-8f9c-93e9722046bd",
          "price": 62.0,
          "retailer": "walmart",
          "store": "CERRO DE LA SILLA"
        },
        // ...
      ]
    },
    // ...
  ],
  "segments": [
    {
        "item_uuid": "930d055d-d781-40bd-8f9c-93e9722046bd",
        "retailer": "walmart",
        "stores": [
            {
                "name": "CERRO DE LA SILLA",
                "store_uuid": "245c5926-7ace-11e7-9b9f-0242ac110003"
            },
            // ...
        ]
    },
    // ...
  ]
}
```

### Compare prices history over Store-item pairs

Compare prices from a fixed pair store-item with additional pairs of other retailer-items over time

**Method**:  POST

**Endpoint**: `/product/compare/history`

**Request Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| fixed_segment | Principal Segment | required |
| added_segments  | Segments to compare | required |
| date  | Date | optional, default=`today` |

**Example Request:**

```json
{
  "date_ini": "2017-12-01",
  "date_fin": "2017-12-07",
  "interval": "day",
  "fixed_segment" : {
      "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
      "retailer": "chedraui",
      "store_uuid": "e02a5370-7b09-11e7-855a-0242ac110005",
      "name": "CHEDRAUI SELECTO UNIVERSIDAD"
      },
  "added_segments": [
      {
          "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
          "retailer": "walmart",
          "store_uuid": "16faeaf4-7ace-11e7-9b9f-0242ac110003",
          "name": "Walmart Universidad"
      },
      {
          "item_uuid": "ffea803e-1aba-413c-82b2-f18455bc5f83",
          "retailer": "soriana",
          "store_uuid": "8c399b5e-7b04-11e7-855a-0242ac110005",
          "name": "Soriana Plaza delta-Soriana Hiper"
      }
  ]
}
```

### Price Stats of an item

Today's max, min & avg price from an specific item_uuid or product_uuid of the day

**Method**:  GET

**Endpoint**: `/product/stats?item_uuid=<item_uuid | required>&prod_uuid=<product_uuid | conditional_required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| item_uuid  | Item UUID | required |
| prod_uuid  | Product UUID | required if `item_uuid` not set

**Response:**

```json
{
  "avg_price": 64.42,
  "max_price": 67.0,
  "min_price": 62.0
}
```

### Count prices of retailer

Count amount of price points from given retailer
from a certain time to the next hour.

**Method**:  GET

**Endpoint**: `/product/count_by_retailer_engine?retailer=<source | required>&date=<date | required>`

**Query Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| retailer  | Retailer/Source key | required |
| date  | Time (YYYY:MM:DD HH:mm:SS) | required |

**Response:**

```json
{
  "count": 64,
}
```

----

## Stats

### Get current stats by retailer

**Method**: POST

**Endpoint**: `/stats/current`

**Request Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| filters | List of Filters (category, retailer, item) | required |
| export  | If True returns CSV else JSON | optional, default=False |

**Request Example:**

```json
{"filters":
  [
    {"category":"2416"},
    {"retailer":"chedraui"},
    {"item_uuid":"03954837-4258-463f-96c4-fed1968deda1"}
  ],
  "export":true
}
```

**JSON Response:**

```json
[
  {
      "gtin": "07501072300225",
      "item_uuid": "03954837-4258-463f-96c4-fed1968deda1",
      "name": "TING 1 Bote , 150 g",
      "prices": {
          "chedraui": {
              "avg": "-",
              "max": "-",
              "min": "-",
              "mode": "-",
              "prev_avg": "-",
              "prev_max": "-",
              "prev_min": "-",
              "prev_mode": "-"
          },
          "san_pablo": {
              "avg": 99.5,
              "max": 99.5,
              "min": 99.5,
              "mode": 99.5,
              "prev_avg": 99.5,
              "prev_max": 99.5,
              "prev_min": 99.5,
              "prev_mode": 99.5
          },
          // ...
      }
  },
  //...
]
```

**CSV Response:**

```csv
gtin,Nombre,Chedraui Avg,Chedraui Max,Chedraui Min,Chedraui Mode,Chedraui Prev Avg,Chedraui Prev Max,Chedraui Prev Min,Chedraui Prev Mode,San Pablo Avg,San Pablo Max,San Pablo Min,San Pablo Mode,San Pablo Prev Avg,San Pablo Prev Max,San Pablo Prev Min,San Pablo Prev Mode,Superama Avg,Superama Max,Superama Min,Superama Mode,Superama Prev Avg,Superama Prev Max,Superama Prev Min,Superama Prev Mode,Walmart Avg,Walmart Max,Walmart Min,Walmart Mode,Walmart Prev Avg,Walmart Prev Max,Walmart Prev Min,Walmart Prev Mode
07501072300225,"TING 1 Bote , 150 g",-,-,-,-,-,-,-,-,99.5,99.5,99.5,99.5,99.5,99.5,99.5,99.5,-,-,-,-,-,-,-,-,92.5,92.5,92.5,92.5,92.5,92.5,92.5,92.5
07503003134169,LOSIL C CREMA 1 % 20 G x 1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,54.96739196777344,57.0,52.0,55.0,55.095237731933594,57.0,55.0,55.0
```

### Get item aggregate prices by filters compared to all others

**Method**: POST

**Endpoint**: `/stats/compare`

**Request Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| filters | List of Filters (category, retailer, item) | required |
| client | Source Key to compare with | required |
| date_start | Starting Date of comparison (YYY-MM-DD) | required |
| date_end | Ending Date of comparison (YYY-MM-DD) | required |
| ends | Flag to return only first and last interval | optional, default=false |
| interval | Time Interval (week, month, day) | required |
| export  | If True returns CSV else JSON | optional, default=False |

**Request example:**

```json
{
  "filters":
  [
    {"category":"2388"},
    {"retailer":"superama"}
    {"item":"03954837-4258-463f-96c4-fed1968deda1"}
  ],
  "client" : "chedraui",
  "date_start" : "2017-09-01",
  "date_end" : "2017-09-07",
  "ends" : false,
  "interval" : "month",
  "export":true
}
```

**JSON Response:**

```json
[
  {
      "gtin": "07841141002828",
      "interval_name": "month",
      "intervals": [
          {
              "avg": 304.87179487179486,
              "client": 0,
              "date_end": "Sat, 30 Sep 2017 00:00:00 GMT",
              "date_start": "Fri, 01 Sep 2017 00:00:00 GMT",
              "difference": -304.87179487179486,
              "retailers": [
                  {
                      "difference": "-",
                      "price": 329,
                      "retailer": "F Ahorro"
                  },
                  {
                      "difference": "-",
                      "price": 340,
                      "retailer": "La Comer"
                  },
                  // ...
              ]
          },
          // ...
      ],
      "item_uuid": "02c09f77-4e98-4611-b680-42dd238c05df",
      "name": "ILTUX 1 Caja,14 Tabletas,20 mg"
  },
  // ...
]
```

**CSV Response:**

```csv
gtin,Nombre,Fecha Inicio,Fecha Final,Mi Retailer,City Market,City Market Diferencia,F Ahorro,F Ahorro Diferencia,La Comer,La Comer Diferencia,San Pablo,San Pablo Diferencia,Soriana,Soriana Diferencia,Superama,Superama Diferencia,Walmart,Walmart Diferencia
07841141002828,"ILTUX 1 Caja,14 Tabletas,20 mg",2017-09-01,2017-09-30,0,-,-,329.0,-,340.0,-,264.0,-,-,-,-,-,-,-
07501326001090,"COZAAR 1 Caja,15 Comprimidos,50 mg",2017-09-01,2017-09-30,0,-,-,490.5,-,-,-,413.0,-,387.0,-,-,-,492.0,-
```

### Get stats history

**Method**: POST

**Endpoint:**  `/stats/history`

**Request Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| filters | List of Filters (category, retailer, item) | required |
| client | Source Key to compare with | required |
| date_start | Starting Date of comparison (YYY-MM-DD) | required |
| date_end | Ending Date of comparison (YYY-MM-DD) | required |
| interval | Time Interval (week, month, day) | required |

**Request Example:**

```json
{
  "filters":
  [
    {"category":"2243"},
    {"retailer":"superama"},
    {"item":"03954837-4258-463f-96c4-fed1968deda1"}
  ],
  "client" : "chedraui",
  "date_start" : "2017-08-01",
  "date_end" : "2017-09-19",
  "interval" : "week"
}
```

**Response:**

```json
{
    "metrics": {
        "avg": [
            [
                1501977600000,
                489.65133145037856
            ],
            // ...
        ],
        "max": [
            [
                1501977600000,
                489.65133145037856,
                489.65133145037856
            ],
            // ...
        ],
        "min": [
            [
                1501977600000,
                489.65133145037856,
                489.65133145037856
            ],
            // ...
        ]
    },
    "retailers": [
        {
            "data": [
                [
                    1501977600000,
                    382.5
                ],
                // ...
            ],
            "name": "Chedraui"
        },
        {
            "data": [
                [
                    1501977600000,
                    536.2106918238994
                ],
                // ...
            ],
            "name": "San Pablo"
        },
        // ...
    ],
    "subtitle": "<b>Periodo</b>: 2017-08-01 - 2017-09-24 <br> <b> Retailers:</b> Superama, Walmart, San Pablo, Chedraui.",
    "title": "Tendencia de Precios"
}
```

### Obtain Categories product count per Retailer

**Method**: POST

**Endpoint**: `/stats/category`

**Request Params:**

| Param | Description | Condition |
| ----- | ----------- | --------- |
| filters | List of Filters (category, retailer, item) | required |

**Request Example:**

```json
{
  "filters": [
    {"item_uuid":"56e67b35-d27e-4cac-9e91-533e0578b59c"},
    {"retailer":"walmart"},
    {"retailer":"la_comer"}
  ]
}
```

**Response:**

```json
[
  {
      "name": "comercial_mexicana",
      "retailer": "Comercial Mexicana",
      "x": 10,
      "y": 1425.31,
      "z": 5
  },
  {
      "name": "f_ahorro",
      "retailer": "F Ahorro",
      "x": 20,
      "y": 665.5,
      "z": 2
  },
  // ...
]
```