# F3KM-master
Scala and Spark code for F3KM: Federated, Fair and Fast k-means. <br>
This code performs 2 distributed algorithms for clustering, using [Apache Spark](https://spark.apache.org/). The implemented algorithms are
- DisBCDKM
- F3KM
For those readers who are not familar with Scala and Spark, we provide a matlab code [here](https://github.com/zsk66/F3KM-MATLAB).
## Getting Started
```
run driver.scala
```
## Debug
When you run this code, it may suffer StackOverFlow error. I suggest you set RAM with -Xss4m or -Xss8m 
