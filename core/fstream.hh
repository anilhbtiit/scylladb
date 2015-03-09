/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

// File <-> streams adapters
//
// Seastar files are block-based due to the reliance on DMA - you must read
// on sector boundaries.  The adapters in this file provide a byte stream
// interface to files, while retaining the zero-copy characteristics of
// seastar files.

#include "file.hh"
#include "reactor.hh"

// FIXME: caching support

class file_data_source_impl : public data_source_impl {
    file _file;
    size_t _buffer_size;
    uint64_t _pos = 0;
public:
    file_data_source_impl(file&& f, size_t buffer_size)
            : _file(std::move(f)), _buffer_size(buffer_size) {}
    virtual future<temporary_buffer<char>> get() override;
    file& fd() { return _file; }
    void seek(uint64_t pos) { _pos = pos; }
};

class file_data_source : public data_source {
    file_data_source_impl* impl() {
        return static_cast<file_data_source_impl*>(data_source::impl());
    }
public:
    file_data_source(file&& f, size_t buffer_size = 8192)
        : data_source(std::make_unique<file_data_source_impl>(std::move(f), buffer_size)) {}
    file& fd() { return impl()->fd(); }
    void seek(uint64_t pos) { impl()->seek(pos); }
};

// Extends input_stream with file-specific operations, such as seeking.
class file_input_stream : public input_stream<char> {
    file_data_source* fd() {
        return static_cast<file_data_source*>(input_stream::fd());
    }
public:
    explicit file_input_stream(file&& f, size_t buffer_size = 8192)
        : input_stream(file_data_source(std::move(f), buffer_size)) {}
    void seek(uint64_t pos) {
        reset();
        fd()->seek(pos);
    }
};
