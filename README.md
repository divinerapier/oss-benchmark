# OSS Benchmark

## Test With Aliyun OSS

``` bash
$ oss-benchmark -provider aliyun-oss -endpoint oss-cn-huhehaote-internal.aliyuncs.com -access-key LTAIACu8JOf8gch8 -secret-key OSnaLPaz03o7Kbpqh7phmJTAlR3BNb -bucket momenta-images -file ./list.txt
```

## Test With AWS S3

``` bash
$ oss-benchmark -provider aws-s3 -region cn-north-1 -access-key LTAIACu8JOf8gch8 -secret-key OSnaLPaz03o7Kbpqh7phmJTAlR3BNb -bucket momenta-images -file ./list.txt
```

## Test With AWS S3 Compitable

``` bash
$ oss-benchmark -provider ceph-s3 -endpoint oss-cn-huhehaote-internal.aliyuncs.com -access-key LTAIACu8JOf8gch8 -secret-key OSnaLPaz03o7Kbpqh7phmJTAlR3BNb -bucket momenta-images -file ./list.txt
```
