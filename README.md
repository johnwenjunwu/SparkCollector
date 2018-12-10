# SparkCollector
## Requirements

MacOS with Apache-Spark 2.4.0 and Java8 installed.

*   Java 8 `$ brew update; brew tap homebrew/cask-versions; brew cask install java`
*   Apache-Spark `$ brew install apache-spark`
## Usage

Write code: extends AbstractCollector to use the project

Build:

`$ mvn package`

Run: collect data from one seed.

`$ spark-submit --class collector.Example target/simple-project-1.0.jar`

Output examples: 

- Time: 1543984800000 ms
- (293,www.bbc.com#)
- (293,www.bbc.com#orb-modules)
- (293,www.bbc.com)
- (278,www.bbc.co.uk/news)
- (268,www.bbc.co.uk)
- (243,www.bbc.co.uk/sport)
- (74,www.bbc.co.uk/accessibility)

## Server
### Http Request:
#### URL SparkCollector: Return top 5 URLs
- `http://localhost:8501/Crawler?start=1&url=https://www.google.com/search?q=ucla` start URL urlCrawler
- `http://localhost:8501/Crawler?start=2` get results from URL urlCrawler
- `http://localhost:8501/Crawler?start=0` stop URL urlCrawler
#### Website SparkCollector: Return top 10 websites
- `http://localhost:8502/Crawler?start=1&url=https://www.google.com/search?q=china` start website urlCrawler
- `http://localhost:8502/Crawler?start=2` get results from website urlCrawler
- `http://localhost:8502/Crawler?start=0` stop website urlCrawler
#### Keyword SparkCollector: Return top 20 keywords (with length > 3)
- `http://localhost:8503/Crawler?start=1&url=https://www.bbc.com/` start keyword urlCrawler
- `http://localhost:8503/Crawler?start=2` get results from keyword urlCrawler
- `http://localhost:8503/Crawler?start=0` stop keyword urlCrawler

#### POI Collector: Return Top 10 Rating POIs from close to far
- `http://localhost:8504/Poi?start=1&lon=116.3978&lat=39.9033` start
- `http://localhost:8504/Poi?start=2` get results
- `http://localhost:8504/Poi?start=0` stop

### Return Json Object
- `[{"_1":325,"_2":"www.google.com/about/"},{"_1":294,"_2":"www.google.com/search?q\u003ducla#"},...,{"_1":287,"_2":"www.ucla.edu/"}]`
- `[{"_1":20260,"_2":"en.wikipedia.org"},{"_1":2648,"_2":"www.google.com"},{"_1":1130,"_2":"www.cia.gov"},{"_1":368,"_2":"www.aljazeera.com"},{"_1":322,"_2":"www.nytimes.com"},{"_1":319,"_2":"www.china.org.cn"},{"_1":290,"_2":"www.cnbc.com"},{"_1":283,"_2":"www.reuters.com"},{"_1":175,"_2":"www.bbc.com"},{"_1":171,"_2":"support.google.com"}]`
- `[{"_1":38,"_2":"with"},{"_1":18,"_2":"from"},{"_1":12,"_2":"mins"},...,{"_1":7,"_2":"weekend"}]`
- `[{"_1":5.0,"_2":"北京彭胜医院 116.421604337309 39.8790360490599"},{"_1":4.9,"_2":"北京方庄购物中心 116.42875232754 39.8655340880918"},{"_1":4.8,"_2":"老佛爷百货 116.374899989768 39.9141985798095"},...,{"_1":4.7,"_2":"首都电影院(金融街店) 116.360392231333 39.91567130133"}]`

## Crawler Demo
![Crawler Demo](/demo/demo1.gif?raw=true)

## POI Demo
![POI Demo](/demo/demo2.gif?raw=true)
