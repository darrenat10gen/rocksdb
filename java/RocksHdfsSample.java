// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

import org.bson.BSON;
import org.bson.BasicBSONObject;
import org.rocksdb.*;

public class RocksHdfsSample {
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args) {
    if (args.length < 4) {
      System.out.println("usage: RocksHDFSSample hdfs_uri db_path family_name limit");
      return;
    }
    String hdfs_uri = args[0];
    String db_path = args[1];
    String family_name = args[2];
    int limit = Integer.parseInt(args[3]);

    RocksDB db = null;
    Options options = new Options();
    options.setEnv(RocksEnv.getHdfsEnv(hdfs_uri));

    ReadOptions readOptions = new ReadOptions();
    readOptions.setFillCache(false);

    try {
      db = RocksDB.openColumnFamilyForRead(options, db_path, family_name);

      RocksIterator iterator = db.newIterator();

      int count = 0;
      BasicBSONObject bo = null;
      for (iterator.seekToFirst(); iterator.isValid() && count++ < limit; iterator.next()) {
        iterator.status();
        assert(iterator.key() != null);
        assert(iterator.value() != null);
        byte[] value = iterator.value();
        bo = (BasicBSONObject)BSON.decode(value);
      }
      
      if(bo != null){
    	  System.out.println("Found " + count + " BSON docs... last one is " + bo.toString());
      } else {
    	  System.out.println("No docs found");    	  
      }
    	  
      iterator.dispose();
      System.out.println("Done.");

    } catch (RocksDBException e) {
      System.err.println(e);
    }
    if (db != null) {
      db.close();
    }
    // be sure to dispose c++ pointers
    options.dispose();
    readOptions.dispose();
  }
}
