# HPC Common Crawl Analytics

This project contains analytics designed to run in Spark PBS clusters. 

## Getting an HPC Account

This project was developed on the DoD HPC Center Topaz cluster. To get an account: 

* Get your Cyber Awareness Certificate
* Send to your sponsoring principle
* Get assigned to a sub-project 

## Logging in

* Peruse the [documentation for your system](https://centers.hpc.mil/systems/unclassified.html)
* Follow the instructions for configuring [PKINIT](https://centers.hpc.mil/users/pkinitUserGuide.html)

## Running Jobs

### On a Cluster

* To build the jar, download sbt-1.0.2 and run `sbt package`
* Copy the jar to `$WORKDIR` and edit the last line of `spark-submit.pbs` with your jar location, then run:

### On a personal installation:

* Run `sbt package` in the root directory of this project
* Identify the path to the jar file. It is likely in the /target directory and starts with `cc-2.11...`
* Run `spark-submit --class edu.usma.cc.SimpleApp \path-to-jar-file.jar`

```bash
$ qsub spark-submit.pbs 
```

## Spark 2.-.- idiosyncracies

* Using map & flatMap methods on Datasets
You can no longer map directly over the rows of DataFrames, as they are now simply a type alias for Dataset[Row]. In Spark 2, to map over a Dataset[T], an encoder for type T must be available in the current context. Encoders are available for standard types and for case classes, but not for Row since it is a generic container type. If you wish to map over the rows of a DataFrame df, you should now convert it to an RDD first:

* Where before you would write:

`df.map { row: Row  => ... }`

* Now you should write:

`df.rdd.map { row: Row => ... }`

