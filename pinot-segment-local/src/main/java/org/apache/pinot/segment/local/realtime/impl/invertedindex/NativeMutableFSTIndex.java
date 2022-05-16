package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFST;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFSTImpl;
import org.apache.pinot.segment.spi.index.mutable.MutableTextIndex;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class NativeMutableFSTIndex implements MutableTextIndex {
  private final String _column;
  private final MutableFST _mutableFST;
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;

  private int _nextDictId = 0;

  public NativeMutableFSTIndex(String column) {
    _column = column;
    _mutableFST = new MutableFSTImpl();

    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
  }

  @Override
  public void add(String document) {

  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    return null;
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    return null;
  }

  @Override
  public void close()
      throws IOException {

  }
}
