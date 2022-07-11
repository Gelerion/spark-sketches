# Overview
The aim is to integrate the [DataSketches](https://datasketches.apache.org/) library seamlessly
into Spark and use all the advantages of approximate algorithms.   
The project contains a set of functions that are indistinguishable from the native ones. 
They are available in both forms - as a Spark function or as a SQL function.
No more `.toRdd` casts or error-prone `map`, `mapPartition`, etc. methods.

Currently two types of sketches are supported:  
- `HyoerLogLog Sketch`  
  HLL sketches are smaller in size because of the nature of the underlying HLL algorithm.  
  However, the HLL algorithm is not designed to allow Intersections. If the use-case  
  for counting uniques only requires merging and does not require Intersection operations  
  then the HLL algorithm is a reasonable choice.
  
- `Theta Sketch`  
  The Theta Sketch from the DS library is a larger sketch, but it is designed from    
  the outset to enable set Intersection and set difference operations.  
  Because set expressions are so powerful from an analysis point-of-view, many users  
  choose the Theta Sketch in spite of its larger size.

It was fully test with Spark `2.4.3` and Scala `2.11.8`

- Type safety with User defined types (propagated to the parquet metadata)
- Code generation
- High performance aggregations using TypedImperativeAggregate

Usage exmaple
- Real time count-distinc analytics (over-time)
- History update

# Examples
- Build sketch example
Scala  
```
import org.apache.spark.sql.functions_ex._

df.groupBy($"dimension")
  .agg(theta_sketch_build($"metric"))

// or

df.groupBy($"dimension")
  .agg(hll_sketch_build($"metric"))
```
  
SQL  
```
SketchFunctionsRegistrar.registerFunctions(spark)
```
```
SELECT theta_sketch_build(metric), hll_sketch_build(metric) 
      FROM dataset
      GROUP BY dimension
```

The sketch will be created in the form of `raw bytes` thus letting us save it into a file and recreate the sketch  
after loading it. Or let us feed these files to our real-time analytic layer like Druid.

- Merge sketches example  
  
One of the most fascinating features about the sketches is that they are mergeable.  
This is extremely useful when  

Merge sketch example:
```
import org.apache.spark.sql.functions_ex._

spark.createDataset(data)
      .groupBy($"name", $"age")
      .agg(hll_sketch_build($"id").as("distinct_ids_sketch"))
      // drop dimension, merge sketches
      .groupBy($"name")
      .agg(hll_sketch_merge("distinct_ids_sketch").as("distinct_ids_sketch"))
      
// or

spark.createDataset(data)
      .groupBy($"name", $"age")
      .agg(theta_sketch_build($"id").as("distinct_ids_sketch"))
      // drop dimension, merge sketches
      .groupBy($"name")
      .agg(theta_sketch_merge("distinct_ids_sketch").as("distinct_ids_sketch"))
```

Evaluate sketch example:
```
import org.apache.spark.sql.functions_ex._

df.select($"dimension", theta_sketch_get_estimate($"sketch_metric").as("approx_count_distinct"))
// or
df.select($"dimension", hll_sketch_get_estimate($"sketch_metric").as("approx_count_distinct"))
```

#### Using SQL
```
SketchFunctionsRegistrar.registerFunctions(spark)
```
```
SELECT tmp.name, theta_sketch_get_estimate(theta_sketch_merge(tmp.distinct_ids_sketch)) as approx_count_distinct
FROM (SELECT name, age, theta_sketch_build(id) as distinct_ids_sketch
      FROM users_theta
      GROUP BY name, age) as tmp
GROUP BY tmp.name
```

Enjoy!


##### generating GPG
```
mvn clean deploy -Dgpg.passphrase="myPassphrase" -Dgpg.keyname="key" -Prelease
```