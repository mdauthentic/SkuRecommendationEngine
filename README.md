# Find Most Similar SKU
The application is built with Apache Spark using Scala api.

Prerequisites:
- `Apache Spark v3.0`
- `Scala v2.12`
- `sbt`
- `Java`

### Usage:
This code is tested with Apache Spark v3.0
- unzip file
- in the current directory, open a terminal
- run `sbt assembly` to build an uber-jar on terminal and wait a few minutes to build
- confirm there is "ProjectDir/target/scala-2.12/SkuRecommendationEngine-assembly-0.1.jar", then
- run the following command to start the spark application

```bash
spark-submit \
--class com.home24.engine.Main \
--master local \
/ProjectDir/target/scala-2.12/RecommendationEngine-assembly-0.1.jar /ProjectDir/src/main/scala/resources/test-data.json
```

- when prompted to "Enter Sku", enter the sku id of interest

See sample output below;


|Most Similar SKU's|Rank|
|------------------|----:|
|sku-10            |10  |
|sku-11            |10  |
|sku-6             |9   |
|sku-5             |8   |
|sku-9             |7   |
|sku-8             |6   |
|sku-1             |1   |
|sku-2             |1   |
|sku-3             |1   |
|sku-4             |1   |


- press ':q' to exit


### Ranking Strategy
Compute the sku similarity based on their attribute
* create new column entry for each attribute and assign numbers `9` to `0` in a left to right manner based on the position of the attributes such that if `a, b, c,.. j` are the attributes, the first attribute i.e "a" is assigned `9` and the last i.e. "j" is assigned `0`.
* merge all the values from the new column and convert the result into string
* do a pair-wise comparison of attributes with the newly merged column in previous step using Spark's in-built `rank.over()` function

