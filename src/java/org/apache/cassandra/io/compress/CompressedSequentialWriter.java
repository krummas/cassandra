/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.compress;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.drizzle.adler.Adler32;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.io.sstable.SSTableMetadata.Collector;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.SequentialWriter;

public class CompressedSequentialWriter extends SequentialWriter
{
    public static SequentialWriter open(String dataFilePath,
                                        String indexFilePath,
                                        boolean skipIOCache,
                                        CompressionParameters parameters,
                                        Collector sstableMetadataCollector)
    {
        return new CompressedSequentialWriter(new File(dataFilePath), indexFilePath, skipIOCache, parameters, sstableMetadataCollector);
    }

    // holds offset in the file where current chunk should be written
    // changed only by flush() method where data buffer gets compressed and stored to the file
    private long chunkOffset = 0;

    // index file writer (random I/O)
    private final CompressionMetadata.Writer metadataWriter;
    private final ICompressor compressor;

    // used to store compressed data
    private final ByteBuffer compressed;
    private final ByteBuffer checksumBuffer;

    // holds a number of already written chunks
    private int chunkCount = 0;

    private final Adler32 checksum = new Adler32();

    private long originalSize = 0, compressedSize = 0;

    private final Collector sstableMetadataCollector;

    public CompressedSequentialWriter(File file,
                                      String indexFilePath,
                                      boolean skipIOCache,
                                      CompressionParameters parameters,
                                      Collector sstableMetadataCollector)
    {
        super(file, parameters.chunkLength(), skipIOCache);
        this.compressor = parameters.sstableCompressor;

        // buffer for compression should be the same size as buffer itself
        compressed = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(byteBuffer.capacity()));
        checksumBuffer = ByteBuffer.allocateDirect(4);
        /* Index File (-CompressionInfo.db component) and it's header */
        metadataWriter = CompressionMetadata.Writer.open(indexFilePath);
        metadataWriter.writeHeader(parameters);

        this.sstableMetadataCollector = sstableMetadataCollector;
    }

    @Override
    public long getOnDiskFilePointer()
    {
        try
        {
            return fileChannel.position();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    @Override
    public void sync()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void flushData()
    {
        seekToChunkStart();
        int compressedLength;
        try
        {
            byteBuffer.flip();
            compressed.clear();
            compressedLength = compressor.compress(byteBuffer, validBufferBytes, compressed);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Compression exception", e); // shouldn't happen
        }

        originalSize += validBufferBytes;
        compressedSize += compressedLength;

        // update checksum
        checksumBuffer.clear();
        checksum.checksum(compressed, compressed.position(), compressed.remaining(), checksumBuffer);
        try
        {
            // write an offset of the newly written chunk to the index file
            metadataWriter.writeLong(chunkOffset);
            chunkCount++;

            // write data itself
            compressed.limit(compressedLength);
            fileChannel.write(compressed);
            // write corresponding checksum
            fileChannel.write(checksumBuffer);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
        byteBuffer.clear();
        // next chunk should be written right after current + length of the checksum (int)
        chunkOffset += compressedLength + 4;
    }



    @Override
    public FileMark mark()
    {
        return new CompressedFileWriterMark(chunkOffset, current, validBufferBytes, chunkCount + 1);
    }

    @Override
    public synchronized void resetAndTruncate(FileMark mark)
    {
        // TODO: actually impleement:
//        assert mark instanceof CompressedFileWriterMark;
//
//        CompressedFileWriterMark realMark = (CompressedFileWriterMark) mark;
//
//        // reset position
//        current = realMark.uncDataOffset;
//
//        if (realMark.chunkOffset == chunkOffset) // current buffer
//        {
//            // just reset a buffer offset and return
//            validBufferBytes = realMark.bufferOffset;
//            return;
//        }
//
//        // synchronize current buffer with disk
//        // because we don't want any data loss
//        syncInternal();
//
//        // setting marker as a current offset
//        chunkOffset = realMark.chunkOffset;
//
//        // compressed chunk size (- 4 bytes reserved for checksum)
//        int chunkSize = (int) (metadataWriter.chunkOffsetBy(realMark.nextChunkIndex) - chunkOffset - 4);
//        if (compressed.buffer.length < chunkSize)
//            compressed.buffer = new byte[chunkSize];
//
//        try
//        {
//         //   out.seek(chunkOffset);
//           // out.readFully(compressed.buffer, 0, chunkSize);
//
//            int validBytes;
//            try
//            {
//                // decompress data chunk and store its length
////                validBytes = compressor.uncompress(compressed.buffer, 0, chunkSize, buffer, 0);
//            }
//            catch (IOException e)
//            {
//                throw new CorruptBlockException(getPath(), chunkOffset, chunkSize);
//            }
//
//            checksum.update(compressed.buffer, 0, chunkSize);
//
//            if (out.readInt() != (int) checksum.getValue())
//                throw new CorruptBlockException(getPath(), chunkOffset, chunkSize);
//        }
//        catch (CorruptBlockException e)
//        {
//            throw new CorruptSSTableException(e, getPath());
//        }
//        catch (EOFException e)
//        {
//            throw new CorruptSSTableException(new CorruptBlockException(getPath(), chunkOffset, chunkSize), getPath());
//        }
//        catch (IOException e)
//        {
//            throw new FSReadError(e, getPath());
//        }
//
//        checksum.reset();
//
//        // reset buffer
//        validBufferBytes = realMark.bufferOffset;
//        bufferOffset = current - validBufferBytes;
//        chunkCount = realMark.nextChunkIndex - 1;
//
//        // truncate data and index file
//        truncate(chunkOffset);
//        metadataWriter.resetAndTruncate(realMark.nextChunkIndex);
    }

    /**
     * Seek to the offset where next compressed data chunk should be stored.
     */
    private void seekToChunkStart()
    {
        if (getOnDiskFilePointer() != chunkOffset)
        {
            try
            {
                fileChannel.position(chunkOffset);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, getPath());
            }
        }
    }

    @Override
    public void close()
    {
        if (!fileChannel.isOpen())
            return; // already closed

        super.close();
        sstableMetadataCollector.addCompressionRatio(compressedSize, originalSize);
        metadataWriter.finalizeHeader(current, chunkCount);
        try
        {
            metadataWriter.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public static void main(String ... args) throws IOException, ConfigurationException
    {
        int FILE_SIZE=1024*1024*20;
        byte [] test = new byte[FILE_SIZE];
        for (int i = 0; i < FILE_SIZE; i++)
            test[i] = (byte)(i%100);
        long start = System.currentTimeMillis();

        Map<String, String> options = new HashMap<String, String>();
        options.put("chunk_length_kb","65536");
        options.put("sstable_compression", "SnappyCompressor");
        SSTableMetadata.Collector sstableMetadataCollector = SSTableMetadata.createCollector(BytesType.instance).replayPosition(null);
        CompressionParameters params = CompressionParameters.create(options);
        for (int i = 0; i < 1000; i++)
        {
            CompressedSequentialWriter sw = new CompressedSequentialWriter(new File("/tmp/testcomp"), "/tmp/indexfile", true, params, sstableMetadataCollector);
            sw.write(test);
            //sw.flush();
            sw.close();
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class CompressedFileWriterMark implements FileMark
    {
        // chunk offset in the compressed file
        final long chunkOffset;
        // uncompressed data offset (real data offset)
        final long uncDataOffset;

        final int bufferOffset;
        final int nextChunkIndex;

        public CompressedFileWriterMark(long chunkOffset, long uncDataOffset, int bufferOffset, int nextChunkIndex)
        {
            this.chunkOffset = chunkOffset;
            this.uncDataOffset = uncDataOffset;
            this.bufferOffset = bufferOffset;
            this.nextChunkIndex = nextChunkIndex;
        }
    }
}
