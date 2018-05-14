# Cassandra Cluster 

# Separación de archivos

Jorge
- scripts/stats
- controllers/stats ..
- models/stats ...
- controllers/alarm
- models/alarm

Rodrigo
- models/price
- scripts/dumps
- run.sh 
- models/task
- models/historia
- models/mapa


# To Do

- Use redis for tasks to containarize the app

## Nodes characteristics
### Analytics
- 800 GB SSD
- 16-32 GB Memory
- 2 cores CPU
### Core
- 800 GB SSD


## Cluster architecture
### DEV
- Setup 1 cassandra dev node
  - 2 cores
  - 8 GB
  - 2000 GB HDD
### PRODUCTION
- Setup 4 cassandra production nodes for core
  - 2 cores
  - 16 GB
  - 500 GB HDD
- Setup 4 cassandra production nodes for analytics
  - 2 cores
  - 16 GB memory
  - 500 GB SSD
- Setup 2 cassandra production nodes for backup
  - 2 cores
  - 8 GB memory
  - 2000 HDD


## Schema

price_item_retailer
price_


* Notes:
- Set NTP for distributed systems
- Install datadog to the nodes??
- node duplication
- split dev and production clusters, NOT
- After adding node to the cluster execute nodetool cleanup in every node one by one