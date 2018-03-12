Title: TensorFlowOnSpark or the namedtuple patch strikes again!
Date: 2018-03-13
Category: programming
Tags: spark, tensorflow, python, rant

**tl;dr** PySpark namedtuple serialization patch can break your TensorFlow
code if you are using TensorFlowOnSpark.

This blog post continues the exploration of the edge cases of the namedtuple
serialization patch in PySpark. If you are lost or just want to catch up on
the context, have a look at
["PySpark silently breaks your namedtuples"][pyspark-namedtuple].

Introduction
------------

[TensorFlowOnSpark][tf-on-spark] is a Python library which allows to distribute
the training and inference of TensorFlow models using Spark. If you are familiar
with the way distributed TensorFlow works, you might be surprised: "Why use
Spark at all?". The answer is out of scope of this blogpost, so if you are truly
wondering the project [`README`][why-tf-on-spark] has a list of (controversial)
reasons for bridging the two.

In the following, we will look at how TensorFlow is affected by the namedtuple
serialization patch, and what we can do about it.

How does TensorFlowOnSpark work? Roughly, to launch a distributed training,

1. TensorFlowOnSpark first delegates to Spark to allocate and setup
   driver/executors.
2. Then it creates a fake RDD with a single partition for each executor,
   and dispatches the `train` function via `foreachPartition`.
3. Finally, the `train` function configures the environment for starting a
   TensorFlow worker (or a parameter server), and calls the user code.

```python
sc.parallelize(range(num_executors), numSlices=num_executors) \
    .foreachPartition(train(...))
```

Since our goal is not to explore the internals of TensorFlowOnSpark, we will
leave TensorFlowOnSpark out of the picture, and instead use custom code for
the `train` function. That said, all we would do transitively applies to any
TensorFlowOnSpark code.

The problem
-----------

[Feature columns][feature-columns] is one of the higher level APIs for defining
machine learning models in TensorFlow. The foal of feature columns is to
transform and preprocess the raw data before plugging it into the model.
A trivial identity model using just a single feature column might look something
like:

```python
# Unless indicated otherwise, all the code is executed on the PySpark driver.
>>> import tensorflow as tf
>>> def identity(features, feature_columns):
...     return tf.feature_column.input_layer(features, feature_columns)
...
>>> features = {"x": [42]}
>>> feature_columns = [tf.feature_column.numeric_column("x")]
>>> model = identity(features, feature_columns)
```

We can *run* the model and verify that the produced result is indeed the value
of `"x"`:

```python
>>> with tf.Session() as sess:
...     sess.run(model)
...
array([[42.]], dtype=float32)
```

Looks good so far. How about we run it on a PySpark executor instead of the
driver:

```python
>>> rdd = sc.parallelize(range(2), numSlices=2)
>>> rdd.foreachPartition(
...     lambda _: tf.Session().run(identity(features, feature_columns)))
org.apache.spark.api.python.PythonException:
Traceback (most recent call last):
  File "<stdin>", line 2, in identity
  File "[...]/tensorflow/python/feature_column/feature_column.py", line 280, in input_layer
    trainable, cols_to_vars)
  File "[...]/tensorflow/python/feature_column/feature_column.py", line 170, in _internal_input_layer
    feature_columns = _clean_feature_columns(feature_columns)
  File "[...]/tensorflow/python/feature_column/feature_column.py", line 2027, in _clean_feature_columns
    'Given (type {}): {}.'.format(type(column), column))
ValueError: Items of feature_columns must be a _FeatureColumn. Given (type <class 'collections._NumericColumn'>): _NumericColumn(key='x', shape=(1,), default_value=None, dtype=tf.float32, normalizer_fn=None).
```

Ouch! What happened? Is it a bug in TensorFlow? The name `_NumericColumn` does
sound a lot like `_FeatureColumn`... and this `collections` module looks oddly
familiar! Well, enough with the guessing, let's see what `isinstance` has to
say.

```python
>>> def check_feature_columns(feature_columns):
...     from tensorflow.python.feature_column import feature_column as fc
...     return all(isinstance(c, fc._FeatureColumn) for c in feature_columns)
...
>>> check_feature_columns(feature_columns)
True
```

Not much, but then [last time][pyspark-namedtuple] we saw that the code working
flawlessly on the driver might fail having crossed the serialization
boundary. Therefore, to be absolutely sure we need to check on the executors as
well.

```python
>>> rdd.mapPartitions(lambda _: [check_feature_columns(feature_columns)]).collect()
[False, False]
```

And of course, this is where things get interesting. The `_NumericColumn` is a
`_FeatureColumn` on the driver, but not so on the executors. The namedtuple
patch strikes again!

The patch revisited
-------------------

Before we move a on, a quick summary of our previous findings:

* The goal of the patch is to make all namedtuples picklable in a format which
  would allow the executors to reconstruct the namedtuple definition even if the
  namedtuple has been defined in the REPL, *aka* the interactive shell.
* The patch equally affects user-defined namedtuples and the ones used by the
  standard/third-party libraries.
* By default all classes are pickled using the name of the class and the
  instance dict. The patch ensures that all namedtuple classes override this
  behaviour and additionally pickle the structure of the namedtuple.

Back to `_NumericColumn`. A lot of TensorFlow classes, `_NumericColumn`
[included][numeric-column], inherit from `collections.namedtuple` to reduce the
boilerplate in class definitions. You could see a trace of this in the
method-resolution-order

```python
>>> fc._NumericColumn.mro()
[<class 'tensorflow.python.feature_column.feature_column._NumericColumn'>,
 <class 'tensorflow.python.feature_column.feature_column._DenseColumn'>,
 <class 'tensorflow.python.feature_column.feature_column._FeatureColumn'>,
 <class 'collections._NumericColumn'>,
 <class 'tuple'>,  # !
 <class 'object'>]
```

This alone, however, does not break the inheritance relation between
`_NumericColumn` and `_FeatureColumn`, which is clearly present in the
MRO for the driver version of the class. What does break it, is the custom
pickling behaviour added by PySpark.

Think about how an instance of `_NumericColumn` is pickled. The `pickle`
implementation would attempt to lookup the `__reduce__` method in the
`_NumericColumn` hierarchy. To do this, it would traverse the hierarchy in MRO
and stop at the first class defining the method. The MRO for `_NumericColumn`
contains two eligible classes:

```python
>>> [cls for cls in fc._NumericColumn.mro() if "__reduce__" in vars(cls)]
[<class 'collections._NumericColumn'>,
 <class 'object'>]
```

However, since `collections._NumericColumn` is before `object` in the MRO,
pickle would always use its`__reduce__` implementation and **not** the default
one in `object`. Note also that `collections._NumericColumn.__reduce__` has no
idea about the higher levels of the hierarchy it is part of, and will therefore
try to pickle the instance is if it was just a `collections._NumericColumn`.

```python
>>> serialized = pickle.dumps(*feature_columns)
>>> type(pickle.loads(serialized))
<class 'collections._NumericColumn'>  # Not ``fc._NumericColumn``!
```

This is exactly what is happening when the `feature_columns` are serialized
to be passed to `check_feature_columns`. Hopefully, now the `False` we get
when executing `check_feature_columns` on the executors makes sense.

The revert
----------

The dynamic nature of Python makes even the wildest dreams possible (which in
part explains the existence of the patch in question). Specifically, it allows
us to revert the patch the same way it was applied. The trick is to undo
`pyspark.serializers._hijack_namedtuple` step-by-step.

```python
import collections

import pyspark  # Force the patch.

collections.namedtuple.__code__ = collections._old_namedtuple.__code__
del collections.namedtuple.__hijack
del collections._old_namedtuple
del collections._old_namedtuple_kwdefaults
```

Caveats:
* This code needs to be executed both on the driver and the executors
* Any namedtuples defined before executing the revert will need to be
  postprocessed manually by removing the `__reduce__` method and setting
  `__module__` to the correct value. Therefore, it is crucial to apply
  the revert  **before** loading any standard library/third-party code
  involving namedtuples.

Conclusion
----------

Namedtuples are hard to avoid when working with real-world Python code.
Yet the namedtuple serialization patch in PySpark makes the experience
of using them much less enjoyable. It is capable of causing unexpected,
hard to diagnose failures in your PySpark application, as we have seen
in the case of TensorFlow feature columns.

If you have experienced similar issues with PySpark, feel free to join the
[discussion][SPARK-22674] on the Spark JIRA.

[pyspark-namedtuple]: {filename}../pyspark-namedtuple.md
[tf-on-spark]: https://github.com/yahoo/TensorFlowOnSpark
[why-tf-on-spark]: https://github.com/yahoo/TensorFlowOnSpark#why-tensorflowonspark
[tf-on-spark-train]: https://github.com/yahoo/TensorFlowOnSpark/blob/bc8bddd5d4f12665d8c9a5195ba6631eacaed7af/tensorflowonspark/TFCluster.py#L54
[feature-columns]: https://www.tensorflow.org/get_started/feature_columns
[numeric-column]: https://github.com/tensorflow/tensorflow/blob/f47b6c9ec5e6c4561a6ed97ef2342ea737dcd80c/tensorflow/python/feature_column/feature_column.py#L2031
[SPARK-22674]: https://issues.apache.org/jira/browse/SPARK-22674
