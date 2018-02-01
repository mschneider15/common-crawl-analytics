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

* To build the jar, download sbt-1.0.2 and run `sbt package`
* Copy the jar to `$WORKDIR` and edit the last line of `spark-submit.pbs` with your jar location, then run:

```bash
$ qsub spark-submit.pbs 
```
