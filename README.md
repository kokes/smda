# smda - small/medium data analysis

This project has been driven by my multi-year effort to streamline one of my recurring tasks - _"I have a 200MB CSV file at hand and I want to look through it - filters, aggregations, charts, exports. I don't want to set up infrastructure or write much code to do so."_

All the solutions I've used (they are superb) are pretty hard to set up for newcomers, they often require a lot of infrastructure, or they only work for data of certain size. I wanted to overcome these issues, hence this project. The tool in question does not handle everything, far from it, it's meant to serve as a tool to quickly grep a given dataset without worrying about databases, schemas, available RAM, virtual environments, Docker etc.

Deployment simplicity is definitely at the heart of this project, a lot of effort was invested in ease of use and it definitely affected the core architecture and functionality. 

## Usage

Go to [releases](https://github.com/kokes/smda/releases), download a version for your operating system and architecture, unpack it and launch it. A local webserver will be launched, you interact with that through your web browser, that's it.

## Main ideas

There are essentially three major things we want to address in smda:

1. We face issues in terms of resource utilisation, namely RAM. In order to keep that in check, we're leveraging the fact that nowadays disks are fairly speedy, both in terms of throughput and latency. So all the data imported is processed and stored efficiently on disk - in a binary and compressed format (rather similar to Parquet or ORC).
2. Existing tools mostly require you to specify a schema beforehand. While that is necessary for proper data engineering, it does get in the way when completing simple data exploration tasks. For this reason we employ schema inference, so that you don't have to hand pick your data types.
3. Many tools require fairly heavy software stacks in order to function. Language runtimes, package managers, compilers, task schedulers, ... We wanted to keep things simple, so the whole thing here is a single binary. It has major costs associated with it, but the deployment simplicity is worth it.

There are a few other things we're addressing, but they are out of scope for this README, we'll elaborate on those in the docs at some point.

We'll also have a note on the inspirations that influenced the design of smda, but for now we'll resort to a short list: [SQLite](https://www.sqlite.org/index.html), [Datasette](https://datasette.io/), [ORC](https://en.wikipedia.org/wiki/Apache_ORC), [PostgreSQL](https://www.postgresql.org/), [Trino](https://trino.io/), and [OpenRefine](https://openrefine.org/) to name just a few.

## Ultimate goals

The goal was to create a data exploration tool, and while the basis of that is committed here, there are a few major components we'd like to add in order to consider it "feature complete" in a 1.0 sense.

1. JOINs and other SQL functionality - at this point we support basics SQL and while we're not aiming to support everything in the standard (it's HUGE), we'd like to get some commonly used features, most notably JOINs.
2. Charting - exploratory data analyses rely on charting, it's pretty much essential to understand your data. And while we have some experience integrating client-side charting in web apps, we just haven't been able to squeeze it in just yet.
3. Object storage - we'd like to decouple compute and storage - it's something we thought from day one, both the overall architecture and on-disk binary format are amenable to this. The ultimate aim here is to make the compute layer so thin that it could be launched from a Lambda function.


## Closing notes

At this point the tools is in flux, the APIs (both REST and Go) keep changing, the binary format may be overhauled at some point, the code base and tooling is changing as well. For these reasons, the tool is to meant to be integrated into larger systems, it's meant as an ad-hoc data exploration tool.

If you have any bug reports, objections, questions, or proposals, you can [file an issue](https://github.com/kokes/smda/issues), [e-mail me](mailto:ondrej.kokes@gmail.com), or ping me [on twitter](https://twitter.com/pndrej).
