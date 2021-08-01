# K-Means
MapReduce is a framework adopted to solve the problem of manage high quantity of data in input and in output in a distributed way. This framework works well with big data because is based on **functional programming** instead of object-oriented programming. It is a declarative type of programming style that is focused on what to solve rather than on how to solve. The main principles of functional programming are **purity** (have no side effect), **immutability** (there are no variables), **higher order functions** (a function takes another function as an argument or returns a function), **composition** (application of one function to the result of another function to produce a third function) and **currying** (process of turning a function with multiple arguments into a function with less arguments).

The general idea, as used often in such case, is to use a *divide and conquer approach*. First, we split the high quantity of input into smaller pieces that can be processed by a single machine. Then each machine completes its process over the data and produce its output. Finally, all the single output can be aggregate to form the final output. Thinking in this way we can occur into some problem such as how to split and distribute data or how to coordinate the access to the data. As we can see in a moment, Hadoop and Spark solve these problems in different way.

The MapReduce algorithm works with two functions:

- **Map** function: receives in input the smaller pieces of data mentioned above. In particular, it receives a key-value pair and produce as output a list of key-value pairs. This function is invoked by *Mapper function*. Multiple mapper runs in parallel and each processing a portion of the input data. In fact, each mappers run on nodes which hold their portion of the data locally, to avoid network traffic.

- **Reduce** function: receives as input a key-list of values pair and produce as output a list of key-value pairs. The function, in this case, is invoked by *Reducer function*.

The algorithm is the following:

1. Choose k initial points x_1, . . . , x_k at random from the set X.
2. Apply the MapReduce given by k-meansMap and k-meansReduce to X.
3. Compute the new centroids x’1, . . . , x’k from the results of the MapReduce.
4. Broadcast the new centroids to each machine on the cluster.
5. Repeat steps from 2 to 4 until the new centroids meet the stop condition.

*Our stop condition is that the difference between all the centroids belonging to two successive iterations is below to 0.1.*

## NOTE
The code is running in VM provided by our institute.

