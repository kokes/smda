Most of the setup needed is here. The terraform script sets up most, just two bits:

- need to change bucket name and public key for it to work
- you might want to change the region here
- the instance doesn't assume that role I worked so hard to define... for some reason (it's noted in the console, but I can't access the s3 bucket)


It looks like we're getting (in eu-central-1) about 100 MB/s tput and 50 ms latency, that should allow us to write a simple cost based optimiser

Though... what if we introduce parallelism and download each chunk separately... that becomes clunky to calculate here, but it would be the most performant for a single-tenant system.

More notes:
- we got IAM working in the end (without any changes)
- the tput is single threaded - if we launch parallel requests, we get 100 MB/s on each, yay
- we don't get region automatically set by the EC2 instance, this might be an issue (we'll have to be explicit I guess)
 - should we get it from `curl http://169.254.169.254/latest/meta-data/placement/region` or is that too clunky? (i'm now using AWS_REGION env)
 - we can also use the sdk to read metadata
 - last but not least - we can let `LoadDefaultConfig` do its thing and if it fails, add a region to `cfg.Region` (string assignment works, we just need to know the value)
- as it stands, I think we'll want to parallelise both - use s3manager or something to pull whole files, and load individual chunks in parallel if we want only partial files - either way, let's spin this in goroutines
- we can always merge contiguous chunk ranges - if we get a request for 0-100 and 100-200, we can grab both at the same time, save request latency... but, we lose paralelism... so the cost based optimiser might end up quite complicated

I'd start by cloning the whole chunk to begin with and we'll see what we come up with later. It might be loading ranges in parallel, the whole chunk in parts in parallel, joining ranges, ... who knows.


### aside: DO

tried digital ocean to see what's what - it should support aws sdk

works quite well, it's fairly fast (200M+/s), but... seems to be rate limited

```
2021/04/08 11:24:09 operation error S3: GetObject, exceeded maximum number of attempts, 3, https response error StatusCode: 503, RequestID: , HostID: , api error SlowDown: Reduce your request rate.
```

anywho, in frankfurt, i'm getting 300 MB/s and 10-15 ms latency


### aside: minio

this will be quite good for (integration) testing

latency... around a milisecond, tput limited by the local disk... so pretty nice
