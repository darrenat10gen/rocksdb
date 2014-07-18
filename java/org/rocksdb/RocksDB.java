// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.Closeable;
import java.io.IOException;
import org.rocksdb.util.Environment;

/**
 * A RocksDB is a persistent ordered map from keys to values.  It is safe for
 * concurrent access from multiple threads without any external synchronization.
 * All methods of this class could potentially throw RocksDBException, which
 * indicates sth wrong at the rocksdb library side and the call failed.
 */
public class RocksDB extends RocksObject {
  public static final int NOT_FOUND = -1;
  private static final String[] compressionLibs_ = {
      "snappy", "zlib", "bzip2", "lz4", "lz4hc"};

  /**
   * Loads the necessary library files.
   * Calling this method twice will have no effect.
   */
  public static synchronized void loadLibrary() {
    // loading possibly necessary libraries.
    for (String lib : compressionLibs_) {
      try {
      System.loadLibrary(lib);
      } catch (UnsatisfiedLinkError e) {
        // since it may be optional, we ignore its loading failure here.
      }
    }
    // However, if any of them is required.  We will see error here.
    System.loadLibrary("rocksdbjni");
  }

  /**
   * Tries to load the necessary library files from the given list of
   * directories.
   *
   * @param paths a list of strings where each describes a directory
   *     of a library.
   */
  public static synchronized void loadLibrary(List<String> paths) {
    for (String lib : compressionLibs_) {
      for (String path : paths) {
        try {
          System.load(path + "/" + Environment.getSharedLibraryName(lib));
          break;
        } catch (UnsatisfiedLinkError e) {
          // since they are optional, we ignore loading fails.
        }
      }
    }
    boolean success = false;
    UnsatisfiedLinkError err = null;
    for (String path : paths) {
      try {
        System.load(path + "/" + Environment.getJniLibraryName("rocksdbjni"));
        success = true;
        break;
      } catch (UnsatisfiedLinkError e) {
        err = e;
      }
    }
    if (success == false) {
      throw err;
    }
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the default options w/ createIfMissing
   * set to true.
   *
   * @param path the path to the rocksdb.
   * @param status an out value indicating the status of the Open().
   * @return a rocksdb instance on success, null if the specified rocksdb can
   *     not be opened.
   *
   * @see Options.setCreateIfMissing()
   * @see Options.createIfMissing()
   */
  public static RocksDB open(String path) throws RocksDBException {
    RocksDB db = new RocksDB();

    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    Options options = new Options();
    db.open(options.nativeHandle_, options.cacheSize_, path);
    db.transferCppRawPointersOwnershipFrom(options);
    options.dispose();
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path.
   */
  public static RocksDB open(Options options, String path)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    RocksDB db = new RocksDB();
    db.open(options.nativeHandle_, options.cacheSize_, path);
    db.transferCppRawPointersOwnershipFrom(options);
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance for 
   * reading a specific column family only.
   */
  public static RocksDB openColumnFamilyForRead(Options options, String dbpath, String familyName)
      throws RocksDBException {

    RocksDB db = new RocksDB();
    long column = db.openColumnFamilyRead(options.nativeHandle_, options.cacheSize_, dbpath, familyName);
    db.familyHandle_ = column;
    db.transferCppRawPointersOwnershipFrom(options);
    return db;
  }

  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  /**
   * Close the RocksDB instance.
   * This function is equivalent to dispose().
   */
  public void close() {
    dispose();
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   */
  public void put(byte[] key, byte[] value) throws RocksDBException {
    put(nativeHandle_, key, key.length, value, value.length);
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   */
  public void put(WriteOptions writeOpts, byte[] key, byte[] value)
      throws RocksDBException {
    put(nativeHandle_, writeOpts.nativeHandle_,
        key, key.length, value, value.length);
  }

  /**
   * Apply the specified updates to the database.
   */
  public void write(WriteOptions writeOpts, WriteBatch updates)
      throws RocksDBException {
    write(writeOpts.nativeHandle_, updates.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key.
   *
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   */
  public int get(byte[] key, byte[] value) throws RocksDBException {
    return get(nativeHandle_, key, key.length, value, value.length);
  }

  /**
   * Get the value associated with the specified key.
   *
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   */
  public int get(ReadOptions opt, byte[] key, byte[] value)
      throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_,
               key, key.length, value, value.length);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @see RocksDBException
   */
  public byte[] get(byte[] key) throws RocksDBException {
    return get(nativeHandle_, key, key.length);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @see RocksDBException
   */
  public byte[] get(ReadOptions opt, byte[] key) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, key.length);
  }

  /**
   * Returns a map of keys for which values were found in DB.
   *
   * @param keys List of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   * entry is the corresponding value in DB.
   *
   * @see RocksDBException
   */
  public Map<byte[], byte[]> multiGet(List<byte[]> keys)
      throws RocksDBException {
    assert(keys.size() != 0);

    List<byte[]> values = multiGet(
        nativeHandle_, keys, keys.size());

    Map<byte[], byte[]> keyValueMap = new HashMap<byte[], byte[]>();
    for(int i = 0; i < values.size(); i++) {
      if(values.get(i) == null) {
        continue;
      }

      keyValueMap.put(keys.get(i), values.get(i));
    }

    return keyValueMap;
  }


  /**
   * Returns a map of keys for which values were found in DB.
   *
   * @param List of keys for which values need to be retrieved.
   * @param opt Read options.
   * @return Map where key of map is the key passed by user and value for map
   * entry is the corresponding value in DB.
   *
   * @see RocksDBException
   */
  public Map<byte[], byte[]> multiGet(ReadOptions opt, List<byte[]> keys)
      throws RocksDBException {
    assert(keys.size() != 0);

    List<byte[]> values = multiGet(
        nativeHandle_, opt.nativeHandle_, keys, keys.size());

    Map<byte[], byte[]> keyValueMap = new HashMap<byte[], byte[]>();
    for(int i = 0; i < values.size(); i++) {
      if(values.get(i) == null) {
        continue;
      }

      keyValueMap.put(keys.get(i), values.get(i));
    }

    return keyValueMap;
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   */
  public void remove(byte[] key) throws RocksDBException {
    remove(nativeHandle_, key, key.length);
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   */
  public void remove(WriteOptions writeOpt, byte[] key)
      throws RocksDBException {
    remove(nativeHandle_, writeOpt.nativeHandle_, key, key.length);
  }

  /**
   * Return a heap-allocated iterator over the contents of the database.
   * The result of newIterator() is initially invalid (caller must
   * call one of the Seek methods on the iterator before using it).
   *
   * Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   *
   * @return instance of iterator object.
   */
  public RocksIterator newIterator() {
    return new RocksIterator(iterator0(nativeHandle_, familyHandle_));
  }

  /**
   * Private constructor.
   */
  protected RocksDB() {
    super();
  }

  /**
   * Transfer the ownership of all c++ raw-pointers from Options
   * to RocksDB to ensure the life-time of those raw-pointers
   * will be at least as long as the life-time of any RocksDB
   * that uses these raw-pointers.
   */
  protected void transferCppRawPointersOwnershipFrom(Options opt) {
    filter_ = opt.filter_;
    opt.filter_ = null;
  }

  // native methods
  protected native long openColumnFamilyRead(
      long optionsHandle, long cacheSize, String path, String family) throws RocksDBException;
  protected native void open(
      long optionsHandle, long cacheSize, String path) throws RocksDBException;
  protected native void put(
      long handle, byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  protected native void put(
      long handle, long writeOptHandle,
      byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  protected native void write(
      long writeOptHandle, long batchHandle) throws RocksDBException;
  protected native int get(
      long handle, byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  protected native int get(
      long handle, long readOptHandle, byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  protected native List<byte[]> multiGet(
      long dbHandle, List<byte[]> keys, int keysCount);
  protected native List<byte[]> multiGet(
      long dbHandle, long rOptHandle, List<byte[]> keys, int keysCount);
  protected native byte[] get(
      long handle, byte[] key, int keyLen) throws RocksDBException;
  protected native byte[] get(
      long handle, long readOptHandle,
      byte[] key, int keyLen) throws RocksDBException;
  protected native void remove(
      long handle, byte[] key, int keyLen) throws RocksDBException;
  protected native void remove(
      long handle, long writeOptHandle,
      byte[] key, int keyLen) throws RocksDBException;
  protected native long iterator0(long optHandle, long familyHandle);
  private native void disposeInternal(long handle);

  protected Filter filter_;
  protected long familyHandle_ = 0;
}
