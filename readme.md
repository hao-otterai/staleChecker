## StaleTrader
Live stream and real-time identification of news staleness to help investors avoid overreaction to stale and rewrite news

### Project Idea
It has been documented in finance journals that investors tend to Overreact to stale news, news reprint and rewrite, as well as
related content on social media.

### Tech Stack
- Kafka: Ingesting data from news source APIs.
- Spark Streaming: distributed processing, fine-grained load balancing, failure recovery, in-memory operations.
- ElasticSearch: text similarity search to identify if highly similar news exist.
- RethinkDB: key-value storage database which support real-time application.
- Flask: app for result demo


### Data Source
 - financial news data - Dow Jones News archive XML dump


### Engineering Challenge


## Business Value


### MVP
 - ETL for preprocessing input text files
 - Setting up a parameter for storing and updating word rectors
 - Build a scalable distributed training pipeline for word vectors on reasonable sized data.


### Quick Start
 (instructions added later)


### Stretch Goals

### references
https://towardsdatascience.com/understanding-locality-sensitive-hashing-49f6d1f6134
https://santhoshhari.github.io/Locality-Sensitive-Hashing/
https://eng.uber.com/lsh/
