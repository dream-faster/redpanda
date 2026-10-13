/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/bytes.h"
#include "crypto/exceptions.h"
#include "crypto/types.h"

#include <memory>

namespace crypto {
///////////////////////////////////////////////////////////////////////////////
/// Digest operations
///////////////////////////////////////////////////////////////////////////////

/**
 * Context structure used to perform digest operations
 */
struct digest_ctx final {
public:
    /**
     * Constructs new digest context with specified type
     * @throws crypto::exception For implementation error
     */
    explicit digest_ctx(digest_type type);
    ~digest_ctx() noexcept;
    digest_ctx(const digest_ctx&) = delete;
    digest_ctx& operator=(const digest_ctx&) = delete;
    digest_ctx(digest_ctx&&) noexcept;
    digest_ctx& operator=(digest_ctx&&) noexcept;

    /**
     * @return size_t Size of digest
     */
    size_t size() const;

    static size_t size(digest_type type);

    /**
     * Updates digest operation with message
     *
     * @return digest_ctx& Itself for update chaining
     * @throws crypto::exception On internal error
     */
    digest_ctx& update(bytes_view msg);
    digest_ctx& update(std::string_view msg);

    /**
     * Finalizes digest operation and returns the digest
     *
     * @return bytes The digest
     * @throws crypto::exception On internal error
     */
    bytes final() &&;

    /**
     * Finalizes digest operation and returns the digest in the provided buffer
     *
     * @param digest The buffer to place the digest into.  It must be exactly
     * the size of the digest
     * @return The provided buffer
     * @throws crypto::exception On internal error or if @p digest is an invalid
     * size
     */
    bytes_span<> final(bytes_span<> digest) &&;
    std::span<char> final(std::span<char> digest) &&;

    /**
     * Finishes digest operation and resets context so it can be reused
     *
     * @return bytes The digest
     * @throws crypto::exception On internal error
     */
    bytes reset();

    /**
     * Finishes digest operation and resets context so it can be used again
     *
     * @param digest The buffer to place the digest into.  It must be exactly
     * the size of the digest
     * @return The provided buffer
     * @throws crypto::exception On internal error or if @p digest is an invalid
     * size
     */
    bytes_span<> reset(bytes_span<> digest);
    std::span<char> reset(std::span<char> digest);

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

/**
 * Performs one-shot digest operation
 *
 * @param type The type to produce
 * @param msg The message to digest
 * @return bytes The resulting digest
 * @throws crypto::exception On internal error
 */
bytes digest(digest_type type, bytes_view msg);
bytes digest(digest_type type, std::string_view msg);

///////////////////////////////////////////////////////////////////////////////
/// MAC operations
///////////////////////////////////////////////////////////////////////////////

/**
 * Context structure used to perform HMAC operations
 */
struct hmac_ctx final {
public:
    /**
     * Construct a new hmac ctx object
     *
     * @param type The type of HMAC to generate
     * @param key The key to use
     * @throws crypto::exception On internal error
     */
    hmac_ctx(digest_type type, bytes_view key);
    hmac_ctx(digest_type type, std::string_view key);
    ~hmac_ctx() noexcept;
    hmac_ctx(const hmac_ctx&) = delete;
    hmac_ctx& operator=(const hmac_ctx&) = delete;
    hmac_ctx(hmac_ctx&&) noexcept;
    hmac_ctx& operator=(hmac_ctx&&) noexcept;

    /**
     * @return size_t Size of the resulting HMAC signature
     */
    size_t size() const;
    static size_t size(digest_type type);

    /**
     * Updates HMAC operation
     *
     * @return hmac_ctx& Itself for update chaining
     * @throws crypto::exception On internal error
     */
    hmac_ctx& update(bytes_view msg);
    hmac_ctx& update(std::string_view msg);

    /**
     * Finalizes HMAC operation and returns signature
     *
     * @return bytes The signature
     * @throws crypto::exception On internal error
     */
    bytes final() &&;

    /**
     * Finalizes digest operation and returns the signature in the provided
     * buffer
     *
     * @param signature The buffer to place the signature into.  It must be
     * exactly the size of the signature
     * @return The provided buffer
     * @throws crypto::exception On internal error or if @p signature is an
     * invalid size
     */
    bytes_span<> final(bytes_span<> signature) &&;
    std::span<char> final(std::span<char> signature) &&;

    /**
     * Finalizes HMAC operation and returns signature and resets context so it
     * can be used again
     *
     * @return bytes The signature
     * @throws crypto::exception On internal error
     */
    bytes reset();

    /**
     * Finalizes digest operation and returns the signature in the provided
     * buffer and resets the context so it can be used again
     *
     * @param signature The buffer to place the signature into.  It must be
     * exactly the size of the signature
     * @return The provided buffer
     * @throws crypto::exception On internal error or if @p signature is an
     * invalid size
     */
    bytes_span<> reset(bytes_span<> signature);
    std::span<char> reset(std::span<char> signature);

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

/**
 * Performs one-shot digest operation
 *
 * @param type The type of digest to create
 * @param key The key to use
 * @param msg The message
 * @return bytes The signature
 * @throws crypto::exception On internal error
 */
bytes hmac(digest_type type, bytes_view key, bytes_view msg);
bytes hmac(digest_type type, std::string_view key, std::string_view msg);

// https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-131Ar2.pdf
// According to the above, hmac keys in fips needs to be at least 112 bits (14
// bytes). In scram, the first operation is to compute HMAC with the password as
// the key. So, in fips mode, scram passwords have the same requirement (>= 14
// bytes).
inline constexpr size_t hmac_key_fips_min_bytes = 14;
/**
 * Checks if given key is too short for hmac, in fips mode
 *
 * @param key The key to used with hmac
 */
bool is_hmac_key_too_short(const bytes_view key);
/**
 * Checks if given password is too short to be used as an hmac key, in fips mode
 *
 * @param password The scram password in plain text
 */
bool is_scram_password_too_short(const std::string_view password);

///////////////////////////////////////////////////////////////////////////////
/// DRBG operation
///////////////////////////////////////////////////////////////////////////////

using use_private_rng = ss::bool_class<struct use_private_rng_tag>;

/**
 * Used to generate some random data and stick it into a provided buffer
 *
 * @param buf The buffer to place random data into
 * @param private_rng Whether or not to use the 'private' DRBG
 * @note For private vs public, please see:
 * https://www.openssl.org/docs/man3.0/man3/RAND_priv_bytes.html
 * @return bytes_span<> The buffer
 * @throws crypto::exception On internal error
 */
bytes_span<> generate_random(
  bytes_span<> buf, use_private_rng private_rng = use_private_rng::no);

/**
 * Generates the requested number of random bytes
 *
 * @param len The amount of data to generate
 * @param private_rng Whether or not to use the 'private' DRBG
 * @note For private vs public, please see:
 * https://www.openssl.org/docs/man3.0/man3/RAND_priv_bytes.html
 * @return bytes The buffer of random data
 * @throws crypto::exception On internal error
 */
bytes generate_random(
  size_t len, use_private_rng private_rng = use_private_rng::no);

/**
 * Secure RNG structure that can be used with stdlib RNG utilities
 *
 * @tparam UsePrivate Whether or not to use the private RNG
 * @tparam UIntType The return type
 */
template<bool UsePrivate, std::unsigned_integral UIntType>
struct secure_rng {
public:
    using result_type = UIntType;

    static constexpr result_type min() {
        return std::numeric_limits<result_type>::min();
    }
    static constexpr result_type max() {
        return std::numeric_limits<result_type>::max();
    }

    result_type operator()();
};

using secure_private_rng = secure_rng<true, uint32_t>;
using secure_public_rng = secure_rng<false, uint32_t>;
} // namespace crypto
