Title: Distributing TensorFlow over YARN using Spark
Date: 2018-05-15
Category: programming
Tags: spark, tensorflow, python

**tl;dr** TODO

Introduction
------------

Outline:
* Spark as a way to reserve nodes and monitor the progress
* setting up TF to use HDFS
* the peculiarities of hgfile
* ps nodes forever -- a Process approach
* chief is done -- everybodies' done
* reserving ports
* HDFS barrier
* knit -- alternative
* libhdfs does not survive fork()
* environment == google -- does not start a TF server
