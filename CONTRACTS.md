# GeoPrice API Contracts


## Stats


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
