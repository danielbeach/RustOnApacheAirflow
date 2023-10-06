# RustOnApacheAirflow
The ultimate Data Engineering Chadstack. Apache Airflow running Rust. Bring it.

This is part of larger blog post trying to do something never done before.
Rust on Airflow. Is it even possible?

Using the `astro cli` we get a local Airflow up and running. Next we write a Rust program
that can do fixed-width to tab conversion of files, both reading from and writing to `s3` buckets. 
Next build a `linux` binary.

We then write an Airflow DAG that can download the Rust binary onto the local
Airflow instance, then trigger that Rust binary passing it a s3 file uri for 
processing.
