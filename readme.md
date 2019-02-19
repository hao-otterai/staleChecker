## StaleTrader
Real-time identification of news staleness to help investors avoid overreaction to stale information.

### Project Idea
Stale news can appear in various forms, including not only rewrite and recombination of stale news from news organization, but also content from social media. It has been studied in financial research [1] that investors tend to overreact to stale news. Research indicate that overreaction to stale news is usually followed by an immediate correction in the next few days, resulting in investors' loss in the short term.  Depending on who you are and how you utilize such pheonemana, identifying news "staleness" in real-time is important. For example: 

- Hedge funds can exploit it as a reverse trading signal to profit in the short term, i.e., selling on positive stale news and buying on negative stale news. 
- Retail investors, knowing the staleness of news in real-time can help them reduce if not completely avoid overraction to stale information.  
- News aggregration app can server its consumers better by providing a "stalness" indicator to financial news, etc.

In this project, I implemented a customized MinHash - Locality Sensitive Hashing (LSH) [2,3] in Spark and Spark Streaming to find news text similarity. This project also make use of the fact that **only staleness check among news within a time-window of past few days and with the same stock symbol is needed**. Therefore, implementing a vanilla MinHash LSH in spark allows for max flexibiliy in implementing such an idea. 

### Implementation
![pipeline](https://github.com/haoyang09/staleChecker/blob/master/pipeline.png)
- Kafka: Ingesting data from news source.
- Spark: Batch pre-process of historical news, including data cleaning, tokenizing, and hashing.
- Spark Streaming: distributed processing, in-memory operations.
- Redis: In-memory key-value storage to support real-time application.
- Flask: web app of financial news.

### Data Source
 - Real time financial news data APIs are typically not free. In this project, I chose a subset of historical Dow Jones News XML dump for demo purposes. To build a real news app in future, integration with some source of real-time news API is required.

### references
[1] [Financial research publication: When Can the Market Identify Old News](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2433234)

[2] [A blog on Locality Sensitive Hashing](https://towardsdatascience.com/understanding-locality-sensitive-hashing-49f6d1f6134)

[3] [Detecting Abuse at Scale: Locality Sensitive Hashing at Uber Engineering](https://eng.uber.com/lsh/)
