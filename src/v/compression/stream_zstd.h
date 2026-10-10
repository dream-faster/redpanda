/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/iobuf.h"

#include <zstd.h>

namespace compression {
class stream_zstd {
public:
    iobuf compress(const iobuf& b) { return do_compress(b); }
    iobuf uncompress(const iobuf& b) { return do_uncompress(b); }
    iobuf compress(iobuf&& b) { return do_compress(b); }
    iobuf uncompress(iobuf&& b) { return do_uncompress(b); }

    /// Allocate this shard's zstd workspaces.
    ///
    /// \param decompression_size sizes the decompression workspace only. The
    /// compression workspace is sized from the compression level, which bounds
    /// it independently of how much data any one call compresses.
    static void init_workspace(size_t decompression_size);

    /// Number of compression workspaces allocated on this shard.
    ///
    /// The workspace is allocated once, at startup, and is never resized, so
    /// this stops at one for the life of the process. Exposed so tests can
    /// assert that rather than infer it.
    static size_t compressor_allocations();

private:
    iobuf do_compress(const iobuf&);
    iobuf do_uncompress(const iobuf&);

    static ZSTD_CCtx* compressor();
    static ZSTD_DCtx* decompressor();
};

} // namespace compression
