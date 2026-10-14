# Redpanda Crypto Library

The Redpanda crypto library provides a convenient API for any subsystem that
desires to use a cryptographic function.  It will abstract away the actual
implementation (OpenSSL).

This library provides the following implementations:

* HMAC
* MD5, SHA256 and SHA512 digest generation
* Cryptographically secure DRBG
* Base64 decoding

## Cryptographic Secure Digest

This library currently supports the following digest types:

* MD5
* SHA256
* SHA512

There are two ways of performing a digest: one-shot and multi-part.

For one-shot, you can do the following:

```c++
#include "crypto/crypto.h"

bytes msg = {...};
auto digest = crypto::digest(crypto::digest_type::SHA256, msg);
```

For multi-part operation, you can do the following:

```c++
#include "crypto/crypto.h"

bytes msg = {...};
crypto::digest_ctx ctx(crypto::digest_type::SHA256);
ctx.update(msg);
auto digest = std::move(ctx).final();
```

## HMAC

Like digests, this library can perform HMAC two ways: one-shot and multi-part.

For one-shot:

```c++
#include "crypto/crypto.h"

bytes key = {...};
bytes msg = {...};

auto sig = crypto::hmac(crypto::digest_type::SHA256, key, msg);
```

For multi-part:

```c++
#include "crypto/crypto.h"

bytes key = {...};
bytes msg = {...};

crypto::hmac_ctx ctx(crypto::digest_type::SHA256, key);
ctx.update(msg);
auto sig = std::move(ctx).final();
```

## RNG Generation

To get cryptographically secure random data, please use
`crypto::generate_random`.  There are two functions you can use.  One will
place random data into the provided buffer and another will return an allocated
buffer with random data.  Both functions also have a flag to indicate if the caller
wants to use the 'private' DRBG.  For more information on this please refer to
OpenSSL's [documentation](https://www.openssl.org/docs/man3.0/man3/RAND_priv_bytes.html)
on this.
