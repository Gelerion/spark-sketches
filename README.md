# Overview
The goal of this project is to seamlessly integrate probabilistic algorithms, in the form of the
[DataSketches](https://datasketches.apache.org/) into [Spark](https://spark.apache.org/). This can be an excellent 
solution for both real-time analytics and incremental historical data updates, as long as the results can tolerate 
slight inaccuracies (which have mathematically proven error bounds).
  
The project currently supports two types of sketches:
-   **HyperLogLog Sketch**: It is usually smaller in size due to the underlying implementation. However, this family does not support intersection operations.
-   **Theta Sketch**: It is usually a bit larger than the HLL Sketch, but it supports intersection and difference operations.

# Highlights

-   Supports all versions of Spark from 2.4 to 3.4+
-   Provides a set of functions that are identical to the native ones, eliminating the need for `.toRdd` casts or error-prone methods such as `map`, `mapPartition`, etc.
-   Easy to use with both `SQL` and `DataFrame`/`Dataset` APIs
-   Type safety with user-defined types
-   Code generation
-   High-performance aggregations leveraging `TypedImperativeAggregate`.

# Get it!

## Maven
```xml
<dependency> 
 <groupId>com.gelerion.spark.sketches</groupId> 
 <artifactId>spark-sketches</artifactId> 
 <version>1.0.0</version> 
</dependency>
```
#### Versions compatibility Matrix
| Project version | Scala | Spark          |
|-----------------|-------|----------------|
| 1.0.0           | 2.12  | 3.3.x -> 3.x.x |
| 0.9.0           | 2.12  | 3.0.x -> 3.2.x |
| 0.8.2           | 2.11  | 2.4.3          |

  
# Use It!

## In SQL
First, register the functions. This only needs to be done once.
```scala
import org.apache.spark.sql.registrar

SketchFunctionsRegistrar.registerFunctions(spark)
```
#### Aggregating
The sketch will be created in the form of `raw bytes`. This allows us to save it into a file and recreate
the sketch after loading it. Alternatively, we can feed these files to our real-time analytic layer, such as Druid.
```sql
SELECT dimension_1, dimension_2, theta_sketch_build(metric)
      FROM table
      GROUP BY dimension_1, dimension_2
```
#### Merging sketches
One of the most fascinating features about the sketches is that they are mergeable.
The function takes other sketch as an input.
```sql
SELECT dim_1, theta_sketch_merge(sketch)
      FROM table
      GROUP BY dimension_1
```
#### Getting results
```sql
SELECT theta_sketch_get_estimate(sketch) FROM table
```
  
## In Scala
First, import the functions.
```scala
import org.apache.spark.sql.functions_ex._
```
#### Aggregating
```scala
df.groupBy($"dimension")
  .agg(theta_sketch_build($"metric"))
```
#### Merging sketches
```scala
df.groupBy($"dimension")
  .agg(theta_sketch_merge($"sketch"))
```
#### Getting results
```scala
df.select(theta_sketch_get_estimate($"sketch"))
```

### Available functions
- Theta sketch
  - theta_sketch_build
  - theta_sketch_merge
  - theta_sketch_get_estimate
- HLL sketch
  - hll_sketch_build
  - hll_sketch_merge
  - hll_sketch_get_estimate
