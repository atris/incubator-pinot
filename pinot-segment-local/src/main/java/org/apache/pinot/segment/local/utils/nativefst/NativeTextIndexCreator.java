package org.apache.pinot.segment.local.utils.nativefst;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.roaringbitmap.Container;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator.ENGLISH_STOP_WORDS_SET;


public class NativeTextIndexCreator implements TextIndexCreator {
  static final String TEMP_DIR_SUFFIX = ".nativetext.idx.tmp";
  static final String FST_FILE_NAME = "native.fst";
  static final String INVERTED_INDEX_FILE_NAME = "inverted.index.buf";
  public static final int HEADER_LENGTH = 20;

  private String _columnName;
  private FSTBuilder _fstBuilder;
  private final File _indexFile;
  private final File _tempDir;
  private final File _fstIndexFile;
  private final File _invertedIndexFile;
  final Map<String, RoaringBitmapWriter<RoaringBitmap>> _postingListMap = new TreeMap<>();
  final RoaringBitmapWriter.Wizard<Container, RoaringBitmap> _bitmapWriterWizard = RoaringBitmapWriter.writer();
  private int _nextDocId = 0;
  private int _fstDataSize;
  private int _numBitMaps;

  public NativeTextIndexCreator(String column, File indexDir)
      throws IOException {
    _columnName = column;
    _fstBuilder = new FSTBuilder();
    _indexFile = new File(indexDir, column + V1Constants.Indexes.NATIVE_TEXT_INDEX_FILE_EXTENSION);
    _tempDir = new File(indexDir, column + TEMP_DIR_SUFFIX);
    if (_tempDir.exists()) {
      FileUtils.cleanDirectory(_tempDir);
    } else {
      FileUtils.forceMkdir(_tempDir);
    }
    _fstIndexFile = new File(_tempDir, FST_FILE_NAME);
    _invertedIndexFile = new File(_tempDir, INVERTED_INDEX_FILE_NAME);
  }

  @Override
  public void add(String document) {
    List<String> tokens;
    try {
      tokens = analyze(document, new StandardAnalyzer(ENGLISH_STOP_WORDS_SET));
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }

    for (String token : tokens) {
      addToPostingList(token);
    }

    _nextDocId++;
  }

  @Override
  public void add(String[] documents, int length) {
    for (int i = 0; i < length; i++) {
      add(documents[i]);
    }
  }

  @Override
  public void seal()
      throws IOException {
    int dictId = 0;
    int numPostingLists = _postingListMap.size();
    try (BitmapInvertedIndexWriter invertedIndexWriter = new BitmapInvertedIndexWriter(_invertedIndexFile,
        numPostingLists)) {

      for (Map.Entry<String, RoaringBitmapWriter<RoaringBitmap>> entry : _postingListMap.entrySet()) {
        byte[] byteArray = entry.getKey().getBytes(UTF_8);
        _fstBuilder.add(byteArray, 0, byteArray.length, dictId++);
        invertedIndexWriter.add(entry.getValue().get());
      }
    }

    FST fst = _fstBuilder.complete();
    _fstDataSize = fst.save(new FileOutputStream(_fstIndexFile));
    generateIndexFile();
  }

  @Override
  public void close()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  public List<String> analyze(String text, Analyzer analyzer)
      throws IOException {
    List<String> result = new ArrayList<String>();
    TokenStream tokenStream = analyzer.tokenStream(_columnName, text);
    CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      result.add(attr.toString());
    }
    return result;
  }

  /**
   * Adds the given value to the posting list.
   */
  void addToPostingList(String value) {
    RoaringBitmapWriter<RoaringBitmap> bitmapWriter = _postingListMap.get(value);
    if (bitmapWriter == null) {
      bitmapWriter = _bitmapWriterWizard.get();
      _postingListMap.put(value, bitmapWriter);
      _numBitMaps++;
    }
    bitmapWriter.add(_nextDocId);
  }

  private void generateIndexFile()
      throws IOException {
    ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH);
    long invertedIndexFileLength = _invertedIndexFile.length();
    headerBuffer.putLong(invertedIndexFileLength);
    headerBuffer.putLong(_fstDataSize);
    headerBuffer.putInt(_numBitMaps);
    headerBuffer.position(0);

    try (FileChannel indexFileChannel = new RandomAccessFile(_indexFile, "rw").getChannel();
        FileChannel invertedIndexFileChannel = new RandomAccessFile(_invertedIndexFile, "r").getChannel();
        FileChannel fstFileChannel = new RandomAccessFile(_fstIndexFile, "rw").getChannel()) {
      indexFileChannel.write(headerBuffer);
      fstFileChannel.transferTo(0, _fstDataSize, indexFileChannel);
      invertedIndexFileChannel.transferTo(0, invertedIndexFileLength, indexFileChannel);
    }
  }
}
