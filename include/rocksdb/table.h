// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Currently we support two types of tables: plain table and block-based table.
//   1. Block-based table: this is the default table type that we inherited from
//      LevelDB, which was designed for storing data in hard disk or flash
//      device.
//   2. Plain table: it is one of RocksDB's SST file format optimized
//      for low query latency on pure-memory or really low-latency media.
//
// A tutorial of rocksdb table formats is available here:
//   https://github.com/facebook/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats
//
// Example code is also available
//   https://github.com/facebook/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats#wiki-examples

#pragma once
#include <memory>
#include <string>
#include <unordered_map>

#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace rocksdb {

// -- Block-based Table
class FlushBlockPolicyFactory;
class RandomAccessFile;
class TableBuilder;
class TableReader;
class WritableFile;
struct EnvOptions;
struct Options;

using std::unique_ptr;

enum ChecksumType : char {
  kNoChecksum = 0x0,  // not yet supported. Will fail
  kCRC32c = 0x1,
  kxxHash = 0x2,
};

// For advanced user only
struct BlockBasedTableOptions {
  // @flush_block_policy_factory creates the instances of flush block policy.
  // which provides a configurable way to determine when to flush a block in
  // the block based tables.  If not set, table builder will use the default
  // block flush policy, which cut blocks by block size (please refer to
  // `FlushBlockBySizePolicy`).
  std::shared_ptr<FlushBlockPolicyFactory> flush_block_policy_factory;

  // TODO(kailiu) Temporarily disable this feature by making the default value
  // to be false.
  //
  // Indicating if we'd put index/filter blocks to the block cache.
  // If not specified, each "table reader" object will pre-load index/filter
  // block during table initialization.
  bool cache_index_and_filter_blocks = false;

  // The index type that will be used for this table.
  enum IndexType : char {
    // A space efficient index block that is optimized for
    // binary-search-based index.
    kBinarySearch,

    // The hash index, if enabled, will do the hash lookup when
    // `Options.prefix_extractor` is provided.
    kHashSearch,
  };

  IndexType index_type = kBinarySearch;

  // Influence the behavior when kHashSearch is used.
  // if false, stores a precise prefix to block range mapping
  // if true, does not store prefix and allows prefix hash collision
  // (less memory consumption)
  bool hash_index_allow_collision = true;

  // Use the specified checksum type. Newly created table files will be
  // protected with this checksum type. Old table files will still be readable,
  // even though they have different checksum type.
  ChecksumType checksum = kCRC32c;
};

// Table Properties that are specific to block-based table properties.
struct BlockBasedTablePropertyNames {
  // value of this propertis is a fixed int32 number.
  static const std::string kIndexType;
};

// Create default block based table factory.
extern TableFactory* NewBlockBasedTableFactory(
    const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

#ifndef ROCKSDB_LITE

enum EncodingType : char {
  // Always write full keys without any special encoding.
  kPlain,
  // Find opportunity to write the same prefix once for multiple rows.
  // In some cases, when a key follows a previous key with the same prefix,
  // instead of writing out the full key, it just writes out the size of the
  // shared prefix, as well as other bytes, to save some bytes.
  //
  // When using this option, the user is required to use the same prefix
  // extractor to make sure the same prefix will be extracted from the same key.
  // The Name() value of the prefix extractor will be stored in the file. When
  // reopening the file, the name of the options.prefix_extractor given will be
  // bitwise compared to the prefix extractors stored in the file. An error
  // will be returned if the two don't match.
  kPrefix,
};

// Table Properties that are specific to plain table properties.
struct PlainTablePropertyNames {
  static const std::string kPrefixExtractorName;
  static const std::string kEncodingType;
};

// -- Plain Table with prefix-only seek
// For this factory, you need to set Options.prefix_extrator properly to make it
// work. Look-up will starts with prefix hash lookup for key prefix. Inside the
// hash bucket found, a binary search is executed for hash conflicts. Finally,
// a linear search is used.
// @user_key_len: plain table has optimization for fix-sized keys, which can be
//                specified via user_key_len.  Alternatively, you can pass
//                `kPlainTableVariableLength` if your keys have variable
//                lengths.
// @bloom_bits_per_key: the number of bits used for bloom filer per prefix. You
//                      may disable it by passing a zero.
// @hash_table_ratio: the desired utilization of the hash table used for prefix
//                    hashing. hash_table_ratio = number of prefixes / #buckets
//                    in the hash table
// @index_sparseness: inside each prefix, need to build one index record for how
//                    many keys for binary search inside each hash bucket.
//                    For encoding type kPrefix, the value will be used when
//                    writing to determine an interval to rewrite the full key.
//                    It will also be used as a suggestion and satisfied when
//                    possible.
// @huge_page_tlb_size: if <=0, allocate hash indexes and blooms from malloc.
//                      Otherwise from huge page TLB. The user needs to reserve
//                      huge pages for it to be allocated, like:
//                          sysctl -w vm.nr_hugepages=20
//                      See linux doc Documentation/vm/hugetlbpage.txt
// @encoding_type: how to encode the keys. See enum EncodingType above for
//                 the choices. The value will determine how to encode keys
//                 when writing to a new SST file. This value will be stored
//                 inside the SST file which will be used when reading from the
//                 file, which makes it possible for users to choose different
//                 encoding type when reopening a DB. Files with different
//                 encoding types can co-exist in the same DB and can be read.

const uint32_t kPlainTableVariableLength = 0;
extern TableFactory* NewPlainTableFactory(
    uint32_t user_key_len = kPlainTableVariableLength,
    int bloom_bits_per_prefix = 10, double hash_table_ratio = 0.75,
    size_t index_sparseness = 16, size_t huge_page_tlb_size = 0,
    EncodingType encoding_type = kPlain, bool full_scan_mode = false);

#endif  // ROCKSDB_LITE

// A base class for table factories.
class TableFactory {
 public:
  virtual ~TableFactory() {}

  // The type of the table.
  //
  // The client of this package should switch to a new name whenever
  // the table format implementation changes.
  //
  // Names starting with "rocksdb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  // Returns a Table object table that can fetch data from file specified
  // in parameter file. It's the caller's responsibility to make sure
  // file is in the correct format.
  //
  // NewTableReader() is called in two places:
  // (1) TableCache::FindTable() calls the function when table cache miss
  //     and cache the table object returned.
  // (1) SstFileReader (for SST Dump) opens the table and dump the table
  //     contents using the interator of the table.
  // options and soptions are options. options is the general options.
  // Multiple configured can be accessed from there, including and not
  // limited to block cache and key comparators.
  // file is a file handler to handle the file for the table
  // file_size is the physical file size of the file
  // table_reader is the output table reader
  virtual Status NewTableReader(
      const Options& options, const EnvOptions& soptions,
      const InternalKeyComparator& internal_comparator,
      unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
      unique_ptr<TableReader>* table_reader) const = 0;

  // Return a table builder to write to a file for this table type.
  //
  // It is called in several places:
  // (1) When flushing memtable to a level-0 output file, it creates a table
  //     builder (In DBImpl::WriteLevel0Table(), by calling BuildTable())
  // (2) During compaction, it gets the builder for writing compaction output
  //     files in DBImpl::OpenCompactionOutputFile().
  // (3) When recovering from transaction logs, it creates a table builder to
  //     write to a level-0 output file (In DBImpl::WriteLevel0TableForRecovery,
  //     by calling BuildTable())
  // (4) When running Repairer, it creates a table builder to convert logs to
  //     SST files (In Repairer::ConvertLogToTable() by calling BuildTable())
  //
  // options is the general options. Multiple configured can be acceseed from
  // there, including and not limited to compression options.
  // file is a handle of a writable file. It is the caller's responsibility to
  // keep the file open and close the file after closing the table builder.
  // compression_type is the compression type to use in this table.
  virtual TableBuilder* NewTableBuilder(
      const Options& options, const InternalKeyComparator& internal_comparator,
      WritableFile* file, CompressionType compression_type) const = 0;
};

#ifndef ROCKSDB_LITE
// Create a special table factory that can open both of block based table format
// and plain table, based on setting inside the SST files. It should be used to
// convert a DB from one table format to another.
// @table_factory_to_write: the table factory used when writing to new files.
// @block_based_table_factory:  block based table factory to use. If NULL, use
//                              a default one.
// @plain_table_factory: plain table factory to use. If NULL, use a default one.
extern TableFactory* NewAdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write = nullptr,
    std::shared_ptr<TableFactory> block_based_table_factory = nullptr,
    std::shared_ptr<TableFactory> plain_table_factory = nullptr);

#endif  // ROCKSDB_LITE

}  // namespace rocksdb
