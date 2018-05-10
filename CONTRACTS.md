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
    "retailer": "chedraui",
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

```json
[

]
```
