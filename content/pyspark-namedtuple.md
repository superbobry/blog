Title: PySpark silently brakes your namedtuples
Date: 2018-02-23
Category: spark, python, rant

**tl;dr** you might want to stay away from using namedtuple with custom
methods in your PySpark applications.

Introduction
------------

[PySpark][pyspark] is a Python API for the (Apache) Spark cluster computing
framework. It allows to write Spark applications in pure Python handling the
Python⇄JVM interop transparently behind the scenes. In this short writeup
we are going to explore a peculiar quirk in the current (2.2.1) PySpark
implementation -- namedtuple serialization.

[`collections.namedtuple`][namedtuple] is a concise way of defining *data
classes*, that is classes whose sole purpose is storing a bunch of
attributes. For example,

```python
from collections import namedtuple

Summary = namedtuple("Summary", ["min", "max"])
```

defines the `Summary` class with two attributes `min` and `max`. The class comes
with a constructor, equality/comparison methods, `__hash__`, `__str__`,
`__repr__` etc. Compare this to the equivalent `class` version

```python
from functools import total_ordering

@total_ordering
class Summary:
    __slots__ = ["min", "max"]

    def __init__(self, min, max):
        self.min = min
        self.max = max

    def __eq__(self, other):
        return self.min == other.min and self.max == other.max

    def __lt__(self, other):
        return (self.min, self.max) < (other.min, other.max)

    # __hash__, __str__, __repr__
```

Replacing all this boilerplate with just a single line is so convenient that
people often use `namedtuple` to create a base class and then add their methods
on top. In case of `Summary` we could, for instance, define a method `combine`,
combining two instances into a single one:

```python
class Summary(namedtuple("Summary", ["min", "max"])):
    def combine(self, other):
        return Summary(min(self.min, other.min), max(self.max, other.max))
```

Newer Python versions (≥ 3.6) come with a different way to define namedtuples --
`typing.NamedTuple`. Unlike the `collections` one, it uses a more natural
class-based syntax and therefore does not require an extra layer of inheritance
to be able to define methods. In the following we'll be using the `typing`
powered implementation of `Summary`.

```python
import typing

class Summary(typing.NamedTuple):
    min: int
    max: int

    def combine(self, other):
        return Summary(min(self.min, other.min), max(self.max, other.max))
```

namedtuples are widespread in the Python ecosystem. Pretty much every Python
project has at least a single namedtuple either defined directly in the project
or coming from third-party libraries ([including][cpython-namedtuple] the
standard library).

The problem
-----------

Now that we've covered namedtuples, let's bring PySpark into the picture. Assume
we have summarized each partition of an RDD of integer numbers

```python
# On the driver.
>>> rdd = sc.parallelize([Summary(-1, 4), Summary(0, 6)], numSlices=2)
```

and now want to combine the summaries via [`RDD.reduce`][rdd-reduce]

```python
# On the driver.
>>> rdd.reduce(lambda s1, s2: s1.combine(s2))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "[...]/python/pyspark/rdd.py", line 837, in reduce
    return reduce(f, vals)
  File "<stdin>", line 1, in <lambda>
AttributeError: 'Summary' object has no attribute 'combine'
```

Ooops. What happened here? The `Summary` class must have the `combine` method,
after all, we have just defined it, but for some reason it is not available
to PySpark. What could've gone wrong? To answer this question we'd resort to
the old-school `print` debugging strategy.

Down the `print` hole
---------------------

The `AttributeError` we got clearly indicates the cause of the failure -- the
`combine` method is not there. What if PySpark somehow transformed the
`Summary`, so that the executors actually got a different `Summary` class? As
crazy at it sounds it's worth checking out.

```python
def print_and_combine(s1, s2):
    print(type(s1), type(s2))
    return s1.combine(s2)
```

Plugging `print_and_combine` into the `reduce` call we get

```python
# On the driver.
>>> rdd.reduce(print_and_combine)
<class 'collections.Summary'> <class 'collections.Summary'>
Traceback (most recent call last):
  ...
AttributeError: 'Summary' object has no attribute 'combine'
```

Hmmm... not getting any better, `collections.Summary`? What is this
`collections` module? We never created one, and even if we did, the `Summary`
class has been defined in the interpreter and therefore should have `__main__`
as its module. So, what's going on?

I must warn you, the answer is not pretty.

To execute `rdd.reduce` call PySpark creates a task (in fact multiple tasks) for
each RDD partition. The tasks would then be serialized and sent to the executors
to be executed. The default serializer in PySpark is
[`PickleSerializer`][pickle-serializer]
which delegates all of the work to the infamous `pickle` module. The pickle
protocol is [extensible][pickle-protocol] and allows to customize pickling
(serialization) and unpickling (deserialization) for instances of any class.
If the class does not define any custom behaviour it will use the default
pickling mechanism, which is to export the name of the class and the instance
dict. This is exactly how namedtuples are serialized

```python
>>> import pickle
>>> pickle.dumps(Summary(0, 0))
b'\x80\x03c__main__\nSummary\nq\x00K\x00K\x00\x86q\x01\x81q\x02.'
```

The problem with the default mechanism in the PySpark case is that in
order to unpickle `Summary` on the executors we need the `Summary` class to be
defined int the `__main__` module within the executors' interpreter. This is
where things get dirty. In fact, the above snippet was produced outside of
PySpark. If you run it on the PySpark driver you'd get a different output

```python
# On the driver.
>>> pickle.dumps(Summary(0, 0))
b'\x80\x03cpyspark.serializers\n_restore\nq\x00X\x05\x00\x00\x00Summaryq\x01X\x07\x00\x00\x00count_aq\x02X\x07\x00\x00\x00count_bq\x03\x86q\x04K\x00K\x00\x86q\x05\x87q\x06Rq\x07.'
```

WAT. WAT? WAT!

The Truth
---------

At import time PySpark
[*patches*][pyspark-namedtuple]
`collections.namedtuple` to have a custom implementation of the pickle protocol
which exposes the name of the class, **the field names** and the corresponding
values. This changes makes all namedtuple instances picklable in an
easy-to-unpickle format. To unpickle a namedtuple instance, the executor would
have to

1. recreate the corresponding `namedtuple` from the class and field names,
2. instantiate the class with field values.

```python
def load_namedtuple(cls_name, field_names, field_values):
    cls = namedtuple(cls_name, field_names)
    return cls(*field_values)
```

Therefore, during the execution of the `reduce` call the executors indeed use a
different version of the `Summary` class. Specifically, it does not have the
`combine` method.

### Sidenote #1

The way PySpark makes this happen is interesting. Instead of swapping
`collections.namedtuple` for their custom implementation and making the change
obvious to the users

```python
collections.namedtuple = pyspark_namedtuple
```

PySpark does this

```python
collections.namedtuple.__code__ = pyspark_namedtuple.__code__
```

which hides the change and makes debugging notoriously hard. The only visible
artefact of the patching is the `__module__` attribute of the created
namedtuples

```python
# On the driver.
>>> namedtuple("Summary", ["min", "max"])
<class 'collections.Summary'>
```

This happens because `namedtuple` dynamically [infers][namedtuple-__module__]
for the defined class from the stack frame preceding the `namedtuple`
call. Without the patch this stack frame points to the module calling
`collections.namedtuple` (`__main__` if called from the interpreter). With the
patch however, there is an [extra][pyspark-namedtuple-extra] function call
from the `collections` module.

The `typing.NamedTuple` version is not affected by this quirk, because a class
definition has the correct `__module__` set at class compilation time.

### Sidenote #2:

The patch makes the pickled representation of **every** namedtuple instance
redundant, since it puts the field names alongside the values. Despite the
[misleading comment][pyspark-namedtuple-comment]
in the PySpark source code, namedtuples defined in third-party modules are also
affected.

```python
# On the driver.
>>> from urllib.robotparser import RequestRate
>>> pickle.dumps(RequestRate(requests=1, seconds=60))
b'\x80\x03cpyspark.serializers\n_restore\nq\x00X\x0b\x00\x00\x00RequestRateq\x01X\x08\x00\x00\x00requestsq\x02X\x07\x00\x00\x00secondsq\x03\x86q\x04K\x01K<\x86q\x05\x87q\x06Rq\x07.'
```

### Sidenote #3:

Normal (non-namedtuple) classes defined in the interpreter cannot be distributed
within PySpark. Attempting to do so would lead to unhelpful, hard-to-understand
errors

```python
>>> class Dummy: pass
...
>>> sc.parallelize([Dummy()]).map(lambda d: d).collect()
8/02/24 21:59:17 WARN scheduler.TaskSetManager: Lost task 3.0 in stage 1.0 (TID 5, ..., executor 2): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  ...
AttributeError: Can't get attribute 'Dummy' on <module 'pyspark.daemon' from '[...]/pyspark.zip/pyspark/daemon.py'>
```

Finale
------

Is the patch necessary? It's hard to say. On one hand, it makes pickling "just
work" for 99% of the use-cases which are namedtuples with no methods. On the
other:

* it incurs a performance penalty,
* it does the *wrong* thing for namedtuples with methods,
* it complicates the debugging by patching the __code__/__globals__ of
  `collections.namedtuple`.

As a PySpark user I would prefer for it to be at the minimum opt-in, and
clearly documented; or better yet -- entirely removed from the codebase.

<!--
The PR introducing the patch has no concerns on the destructive behaviour...
https://github.com/apache/spark/pull/1623
-->

P. S.
-----

There is a way to easily undo the patch for namedtuples defined in modules (as
opposed to the interpreter). These could be safely pickled/unpickled by the
default implementation, so there is no reason to tolerate the performance drop
introduced by PySpark.

```python
Summary = Summary = namedtuple("Summary", ["min", "max"])
Summary.__module__ = __name__  # Only needed for ``collections.namedtuple``.
del Summary.__reduce__
```

[pyspark]: https://spark.apache.org
[namedtuple]: https://docs.python.org/3/library/collections.html#collections.namedtuple
[cpython-namedtuple]: https://github.com/python/cpython/search?l=Python&q=namedtuple&type=&utf8=%E2%9C%93
[rdd-reduce]: http://spark.apache.org/docs/2.2.1/api/python/pyspark.html#pyspark.RDD.reduce
[pickle-serializer]: https://github.com/apache/spark/blob/branch-2.2/python/pyspark/serializers.py#L439
[pickle-protocol]: https://docs.python.org/3/library/pickle.html#pickling-class-instances
[pyspark-namedtuple]: https://github.com/apache/spark/blob/branch-2.2/python/pyspark/serializers.py#L375
[namedtuple-__module__]: https://github.com/python/cpython/blob/master/Lib/collections/__init__.py#L473
[pyspark-namedtuple-extra]: https://github.com/apache/spark/blob/7a2ada223e14d09271a76091be0338b2d375081e/python/pyspark/serializers.py#L513
[pyspark-namedtuple-comment]: https://github.com/apache/spark/blob/branch-2.2/python/pyspark/serializers.py#L427
