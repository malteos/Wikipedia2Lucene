Wikipedia2Lucene
================

[![Build Status](https://drone.io/github.com/mschwarzer/Wikipedia2Lucene/status.png)](https://drone.io/github.com/mschwarzer/Wikipedia2Lucene/latest)

Import a Wikipedia XML Dump from HDFS to Lucene index or Elasticsearch and retrieve similar Wikipedia articles based on Lucene's MoreLikeThis query. 

This application is an implementation of a text-based document similarity measure that is used as a baseline measure in the research of [Co-Citation Proximity Analysis for Wikipedia](https://github.com/TU-Berlin/cpa-demo).

### Add Wikipedia Articles to Elasticsearch Index

##### Usage
```
java -cp WikiLucene.jar de.tuberlin.dima.mschwarzer.lucene.WikiElasticSearch \
    WIKI-XML-DUMP HOST INDEX TYPE [START] [LIMIT] [RESET]
```

##### Parameters
 * WIKI-XML-DUMP: Path to Wikipedia XML Dump (located on HDFS).
 * HOST: Host or IP of Elasticsearch NameNode.
 * INDEX: Name of Elasticsearch index (default=wikipedia).
 * TYPE: Type of Elasticsearch documents (default=article).
 * START: Skip articles until this value.
 * LIMIT: Process articles until this value.
 
##### Example
```
 java -cp WikiLucene.jar de.tuberlin.dima.mschwarzer.lucene.WikiElasticSearch \
    dima-power-cluster ibm-power-1.dima.tu-berlin.de wikipedia article 100 20000
```
 
---

### Retrieve similar Wikipedia articles (MoreLikeThis results)

##### Usage
```
java -cp WikiLucene.jar de.tuberlin.dima.mschwarzer.lucene.morelikethis.ResultCollector \
    CLUSTER-NAME HOST INDEX TYPE OUTPUT-FILENAME \
    [DOC-FILTER-FILENAME] [SCROLL-START] [SCROLL-LIMIT] [SINGLE-LINE-OUTPUTFORMAT]
```

##### Parameters
 * CLUSTER-NAME: Name of Elasticsearch cluster.
 * HOST: Host or IP of Elasticsearch NameNode.
 * INDEX: Name of Elasticsearch index (default=wikipedia).
 * TYPE: Type of Elasticsearch documents (default=article).
 * OUTPUT-FILE: Path to output file. 
 * DOC-FILTER-FILENAME: Retrieve results for these articles, one article per line. (If not set, all articles are used. [Example](https://github.com/mschwarzer/Wikipedia2Lucene/blob/master/src/main/resources/articlenames.csv))
 * SCROLL-START: Skip articles until this value.
 * SCROLL-LIMIT: Process articles until this value.
    
##### Example
```
java -cp WikiLucene.jar de.tuberlin.dima.mschwarzer.lucene.morelikethis.ResultCollector \
    dima-power-cluster ibm-power-1.dima.tu-berlin.de \
    wikipedia article ./mlt_results.csv ./articles.csv 10 20000
```

