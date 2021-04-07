Most of the setup needed is here. The terraform script sets up most, just two bits:

- need to change bucket name and public key for it to work
- you might want to change the region here
- the instance doesn't assume that role I worked so hard to define... for some reason (it's noted in the console, but I can't access the s3 bucket)


It looks like we're getting (in eu-central-1) about 100 MB/s tput and 50 ms latency, that should allow us to write a simple cost based optimiser

Though... what if we introduce parallelism and download each chunk separately... that becomes clunky to calculate here, but it would be the most performant for a single-tenant system.
