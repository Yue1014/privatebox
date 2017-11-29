### Friendly Taint-Tracking Interfaces(for Spark now)

Suppose there is a RDD named rddUnTainted, we need to add some taint to some of 
the data in spark.

We will do this by
```
val rddTainted = rddUnTainted.taint(f: T => Any)
```

Here, function f transfer the data to a tag list,

For example, suppose data is (Int, Int, Int) and we would like to add
taint to the first two element of this data, then f = (1, 2, 0)

If one of the element of a data is Array(or vector), like (Int, Int, Array[Double])

we could add different taint to the Array of same taint to all element of the array,
like (1, 2, Array(1, 2, 3)). Then the first two elements of the tuple will be tainted
and the array elements will also be tainted

To get the taint from data, we could use as following
```
// get the tuple of (data, tag) and then collect
val result = taintedRDD.zipWithTaint()
result.collect()

// collect the tuple of (data, tag) directly
val result = taintedRDD.collectWithTaint()

```

#### Implementation

To implement this, we will need to add a boolean variable in each rdd to indicate
if a rdd is tainted or not(But how to propogate this tag(rules...)). 
And we should also add create a new operation for RDD,
to represent a tainted RDD.