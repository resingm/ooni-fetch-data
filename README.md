# TODO

More information will follow soon...


The [OONI web connectivity test](https://ooni.org/nettest/web-connectivity/)
([test specification](https://github.com/ooni/spec/blob/master/nettests/ts-017-web-connectivity.md))
tests splits into four different stages:

 1. Resolver identification
 2. DNS lookup
 3. TCP connect
 4. HTTP GET request

Following this structure allows us to spilt the input data into four
different tables, where each of them is a timeseries in themselves.

Resolver identification


