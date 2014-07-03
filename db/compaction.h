//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "db/version_set.h"

namespace rocksdb {

class Version;
class ColumnFamilyData;

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Returns the lowest level that is being compacted.  Inputs between
  // "base_level" and "output_level" will be merged.
  int base_level() const { return base_level_; }

  // Outputs will go to this level
  int output_level() const { return output_level_; }

  // Returns the number of input levels in this compaction.
  // This number equals to "output_level() - base_level() + 1".
  int input_levels() const { return input_levels_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() const { return edit_; }

  // Returns the number of input files at level "base_level() + which".
  // The function will return 0 if when "which" < 0 or >= "input_levels()".
  int num_input_files(int which) const {
    if (which >= 0 && which < input_levels_) {
      return inputs_[which].size();
    }
    return 0;
  }

  // Returns input version of the compaction
  Version* input_version() const { return input_version_; }

  // Returns the ColumnFamilyData associated with the compaction.
  ColumnFamilyData* column_family_data() const { return cfd_; }

  // Returns the ith input file at level "base_level() + which",
  // and "which" must be >= 0 and < "input_levels()".
  FileMetaData* input(int which, int i) const {
    assert(which < input_levels_ && which >= 0);
    return inputs_[which][i];
  }

  // Returns the list of file meta data associated with level
  // "base_level() + which", where "which" must be >= 0 and
  // < "input_levels()"
  std::vector<FileMetaData*>* const inputs(int which) {
    assert(which < input_levels_ && which >= 0);
    return &inputs_[which];
  }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // What compression for output
  CompressionType OutputCompressionType() const { return output_compression_; }

  // Whether need to write output file to second DB path.
  uint32_t GetOutputPathId() const { return output_path_id_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // If true, then the comaction can be done by simply deleting input files.
  bool IsDeletionCompaction() const {
    return deletion_compaction_;
  }

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the available information we have guarantees that
  // the input "user_key" does not exist in any level beyond "output_level()".
  bool KeyNotExistsBeyondOutputLevel(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

  // Clear all files to indicate that they are not being compacted
  // Delete this compaction from the list of running compactions.
  void ReleaseCompactionFiles(Status status);

  // Returns the summary of the compaction in "output" with maximum "len"
  // in bytes.  The caller is responsible for the memory management of
  // "output".
  void Summary(char* output, int len);

  // Return the score that was used to pick this compaction run.
  double score() const { return score_; }

  // Is this compaction creating a file in the bottom most level?
  bool BottomMostLevel() { return bottommost_level_; }

  // Does this compaction include all sst files?
  bool IsFullCompaction() { return is_full_compaction_; }

  // Was this compaction triggered manually by the client?
  bool IsManualCompaction() { return is_manual_compaction_; }

  // Returns the size in bytes that the output file should be preallocated to.
  // In level compaction, that is max_file_size_. In universal compaction, that
  // is the sum of all input file sizes.
  uint64_t OutputFilePreallocationSize();

 private:
  friend class CompactionPicker;
  friend class UniversalCompactionPicker;
  friend class FIFOCompactionPicker;
  friend class LevelCompactionPicker;

  Compaction(Version* input_version, int base_level, int out_level,
             uint64_t target_file_size, uint64_t max_grandparent_overlap_bytes,
             uint32_t output_path_id, CompressionType output_compression,
             bool seek_compaction = false, bool deletion_compaction = false);

  const int base_level_;    // the lowest level to be compacted
  const int output_level_;  // levels to which output files are stored
  const int input_levels_;  // output_level_ - base_level_
  uint64_t max_output_file_size_;
  uint64_t max_grandparent_overlap_bytes_;
  Version* input_version_;
  VersionEdit* edit_;
  int number_levels_;
  ColumnFamilyData* cfd_;

  uint32_t output_path_id_;
  CompressionType output_compression_;
  bool seek_compaction_;
  // If true, then the comaction can be done by simply deleting input files.
  bool deletion_compaction_;

  // Each compaction reads inputs from "base_level_" and "base_level_+1"
  std::vector<std::vector<FileMetaData*>> inputs_;

  // State used to check for number of of overlapping grandparent files
  // (parent == "output_level_", grandparent == "output_level_ + 1")
  // This vector is updated by Version::GetOverlappingInputs().
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  uint64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files
  int base_index_;   // index of the file in files_[base_level_]
  int parent_index_; // index of some file with same range in files_[level_+1]
  double score_;     // score that was used to pick this compaction.

  // Is this compaction creating a file in the bottom most level?
  bool bottommost_level_;
  // Does this compaction include all sst files?
  bool is_full_compaction_;

  // Is this compaction requested by the client?
  bool is_manual_compaction_;

  // "level_ptrs_" holds indices into "input_version_->levels_", where each
  // index remembers which file of an associated level we are currently used
  // to check KeyNotExistsBeyondOutputLevel() for deletion operation.
  // As it is for checking KeyNotExistsBeyondOutputLevel(), it only
  // records indices for all levels beyond "output_level_".
  std::vector<size_t> level_ptrs_;

  // mark (or clear) all files that are being compacted
  void MarkFilesBeingCompacted(bool mark_as_compacted);

  // Initialize whether the compaction is producing files at the
  // bottommost level.
  //
  // @see BottomMostLevel()
  void SetupBottomMostLevel(bool is_manual);

  // In case of compaction error, reset the nextIndex that is used
  // to pick up the next file to be compacted from files_by_size_
  void ResetNextCompactionIndex();
};

}  // namespace rocksdb
