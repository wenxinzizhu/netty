/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;


import java.nio.ByteBuffer;

/**
 * Acts a Thread cache for allocations. This implementation is moduled after
 * <a href="http://people.freebsd.org/~jasone/jemalloc/bsdcan2006/jemalloc.pdf">jemalloc</a> and the descripted
 * technics of <a href="https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/
 * 480222803919">Scalable memory allocation using jemalloc</a>.
 */
final class PoolThreadCache {
    final PoolArena<byte[]> heapArena;
    final PoolArena<ByteBuffer> directArena;

    // Hold the caches for the different size classes, which are tiny, small and normal.
    private final PoolChunkCache<byte[]>[] tinySubPageHeapCaches;
    private final PoolChunkCache<byte[]>[] smallSubPageHeapCaches;
    private final PoolChunkCache<ByteBuffer>[] tinySubPageDirectCaches;
    private final PoolChunkCache<ByteBuffer>[] smallSubPageDirectCaches;
    private final PoolChunkCache<byte[]>[] normalHeapCaches;
    private final PoolChunkCache<ByteBuffer>[] normalDirectCaches;

    // Used for bitshifting when calculate the index of normal caches later
    private final int valNormalDirect;
    private final int valNormalHeap;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena,
                    int tinyCacheSize, int smallCacheSize, int normalCacheSize,
                    int maxCacheSize, int maxCacheArraySize) {
        if (maxCacheSize < 0) {
            throw new IllegalArgumentException("maxCacheSize: " + maxCacheSize + " (expected: >= 0)");
        }
        if (maxCacheArraySize < 0) {
            throw new IllegalArgumentException("maxCacheArraySize: " + maxCacheArraySize + " (expected: >= 0)");
        }
        this.heapArena = heapArena;
        this.directArena = directArena;
        if (directArena != null) {
            tinySubPageDirectCaches = createSubPageCaches(tinyCacheSize, directArena.numTinySubpagePools);
            smallSubPageDirectCaches = createSubPageCaches(smallCacheSize, directArena.numSmallSubpagePools);

            valNormalDirect = normalCacheIdx(directArena.pageSize);
            normalDirectCaches = createNormalCaches(
                    normalCacheSize, maxCacheSize, maxCacheArraySize, valNormalDirect, directArena);
        } else {
            // No directArea is configured so just null out all caches
            tinySubPageDirectCaches = null;
            smallSubPageDirectCaches = null;
            normalDirectCaches = null;
            valNormalDirect = -1;
        }
        if (heapArena != null) {
            // Create the caches for the heap allocations
            tinySubPageHeapCaches = createSubPageCaches(tinyCacheSize, heapArena.numTinySubpagePools);
            smallSubPageHeapCaches = createSubPageCaches(smallCacheSize, heapArena.numSmallSubpagePools);

            valNormalHeap = normalCacheIdx(heapArena.pageSize);
            normalHeapCaches = createNormalCaches(
                    normalCacheSize, maxCacheSize, maxCacheArraySize, valNormalHeap, heapArena);
        } else {
            // No heapArea is configured so just null out all caches
            tinySubPageHeapCaches = null;
            smallSubPageHeapCaches = null;
            normalHeapCaches = null;
            valNormalHeap = -1;
        }
    }

    private static <T> SubPagePoolChunkCache<T>[] createSubPageCaches(int cacheSize, int numCaches) {
        if (cacheSize > 0) {
            @SuppressWarnings("unchecked")
            SubPagePoolChunkCache<T>[] cache = new SubPagePoolChunkCache[numCaches];
            for (int i = 0; i < cache.length; i++) {
                // TODO: maybe use cacheSize / cache.length
                cache[i] = new SubPagePoolChunkCache<T>(cacheSize);
            }
            return cache;
        } else {
            return null;
        }
    }

    private static <T> NormalPoolChunkCache<T>[] createNormalCaches(
            int cacheSize, int maxCacheSize, int maxCacheArraySize, int val, PoolArena<T> area) {
        if (cacheSize > 0) {
            int max = Math.min(area.chunkSize, maxCacheSize);
            int arraySize = Math.min(maxCacheArraySize, max / area.pageSize);

            @SuppressWarnings("unchecked")
            NormalPoolChunkCache<T>[] cache = new NormalPoolChunkCache[arraySize];
            int size = area.pageSize;
            for (int i = 0; i < cache.length; i++) {
                cache[normalCacheIdx(size) >> val] =
                        new NormalPoolChunkCache<T>(cacheSize);
                size = area.normalizeCapacity(size);
            }
            return cache;
        } else {
            return null;
        }
    }

    // TODO: Find a better name
    private static int normalCacheIdx(int val) {
        int res = 0;
        while (val > 1) {
            val >>= 1;
            res++;
        }
        return res;
    }

    /**
     * Try to allocate a tiny buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    boolean allocateTiny(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
        return allocate(cacheForTiny(area, normCapacity), buf, reqCapacity);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    boolean allocateSmall(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
        return allocate(cacheForSmall(area, normCapacity), buf, reqCapacity);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    boolean allocateNormal(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
        return allocate(cacheForNormal(area, normCapacity), buf, reqCapacity);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static boolean allocate(PoolChunkCache<?> cache, PooledByteBuf buf, int reqCapacity) {
        if (cache == null) {
            // no cache found so just return false here
            return false;
        }
        return cache.allocate(buf, reqCapacity);
    }

    /**
     * Add {@link PoolChunk} and {@code handle} to the cache if there is enough room.
     * Returns {@code true} if it fit into the cache {@code false} otherwise.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    boolean add(PoolArena<?> area, PoolChunk chunk, long handle, int normCapacity) {
        PoolChunkCache<?> cache;
        if (area.isTinyOrSmall(normCapacity)) {
            if (PoolArena.isTiny(normCapacity)) {
                cache = cacheForTiny(area, normCapacity);
            } else {
                cache = cacheForSmall(area, normCapacity);
            }
        } else {
            cache = cacheForNormal(area, normCapacity);
        }
        if (cache == null) {
            return false;
        }
        return cache.add(chunk, handle);
    }

    /**
     *  Should be called if the Thread that uses this cache is about to exist to release resources out of the cache
     */
    void free() {
        free(tinySubPageDirectCaches);
        free(smallSubPageDirectCaches);
        free(normalDirectCaches);
        free(tinySubPageHeapCaches);
        free(smallSubPageHeapCaches);
        free(normalHeapCaches);
    }

    private static void free(PoolChunkCache<?>[] caches) {
        if (caches == null) {
            return;
        }
        for (int i = 0; i < caches.length; i++) {
            free(caches[i]);
        }
    }

    private static void free(PoolChunkCache<?> cache) {
        if (cache == null) {
            return;
        }
        cache.free();
    }

    void freeUpIfNecessary() {
        freeUpIfNecessary(tinySubPageDirectCaches);
        freeUpIfNecessary(smallSubPageDirectCaches);
        freeUpIfNecessary(normalDirectCaches);
        freeUpIfNecessary(tinySubPageHeapCaches);
        freeUpIfNecessary(smallSubPageHeapCaches);
        freeUpIfNecessary(normalHeapCaches);
    }

    private static void freeUpIfNecessary(PoolChunkCache<?>[] caches) {
        if (caches == null) {
            return;
        }
        for (int i = 0; i < caches.length; i++) {
            freeUpIfNecessary(caches[i]);
        }
    }

    private static void freeUpIfNecessary(PoolChunkCache<?> cache) {
        if (cache == null) {
            return;
        }
        cache.freeUpIfNecessary();
    }

    private PoolChunkCache<?> cacheForTiny(PoolArena<?> area, int normCapacity) {
        int idx = PoolArena.tinyIdx(normCapacity);
        if (area.isDirect()) {
            return cache(tinySubPageDirectCaches, idx);
        }
        return cache(tinySubPageHeapCaches, idx);
    }

    private PoolChunkCache<?> cacheForSmall(PoolArena<?> area, int normCapacity) {
        int idx = PoolArena.smallIdx(normCapacity);
        if (area.isDirect()) {
            return cache(smallSubPageDirectCaches, idx);
        }
        return cache(smallSubPageHeapCaches, idx);
    }

    private PoolChunkCache<?> cacheForNormal(PoolArena<?> area, int normCapacity) {
        int idx = normalCacheIdx(normCapacity >> valNormalDirect);
        if (area.isDirect()) {
            return cache(normalDirectCaches, idx);
        }
        return cache(normalHeapCaches, idx);
    }

    private static <T> PoolChunkCache<T> cache(PoolChunkCache<T>[] cache, int idx) {
        if (cache == null || idx > cache.length - 1) {
            return null;
        }
        return cache[idx];
    }

    /**
     * Cache used for buffers which are backed by NORMAL size.
     *
     * @param <T>
     */
    private static final class SubPagePoolChunkCache<T> extends PoolChunkCache<T> {
        SubPagePoolChunkCache(int size) {
            super(size);
        }

        @Override
        protected void initBuf(
                PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity) {
            chunk.initBufWithSubpage(buf, handle, reqCapacity);
        }
    }

    /**
     * Cache used for buffers which are backed by TINY or SMALL size.
     *
     * @param <T>
     */
    private static final class NormalPoolChunkCache<T> extends PoolChunkCache<T> {
        NormalPoolChunkCache(int size) {
            super(size);
        }

        @Override
        protected void initBuf(
                PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity) {
            chunk.initBuf(buf, handle, reqCapacity);
        }
    }

    /**
     * Cache of {@link PoolChunk} and handles which can be used to allocate a buffer without locking at all.
     *
     * @param <T>
     */
    private abstract static class PoolChunkCache<T> {
        private final Entry<T>[] entries;
        private int head;
        private int tail;
        // Holds the number of allocations since the last freeUpIfNecessary() or free() call.
        private long allocations;

        @SuppressWarnings("unchecked")
        PoolChunkCache(int size) {
            entries = new Entry[powerOfTwo(size)];
            for (int i = 0; i < entries.length; i++) {
                entries[i] = new Entry<T>();
            }
        }

        private static int powerOfTwo(int res) {
            if (res <= 2) {
                return 2;
            }
            res--;
            res |= res >> 1;
            res |= res >> 2;
            res |= res >> 4;
            res |= res >> 8;
            res |= res >> 16;
            res++;
            return res;
        }

        /**
         * Init the {@link PooledByteBuf} using the provided chunk and handle with the capacity restrictions.
         */
        protected abstract void initBuf(PoolChunk<T> chunk, long handle,
                                        PooledByteBuf<T> buf, int reqCapacity);

        /**
         * Add to cache if not already full.
         */
        public boolean add(PoolChunk<T> chunk, long handle) {
            Entry<T> entry = entries[tail];
            if (entry.chunk != null) {
                // cache is full
                return false;
            }
            entry.chunk = chunk;
            entry.handle = handle;
            tail = nextIdx(tail);
            return true;
        }

        /**
         * Allocate something out of the cache if possible and remove the entry from the cache.
         */
        public boolean allocate(PooledByteBuf<T> buf, int reqCapacity) {
            Entry<T> entry = entries[head];
            if (entry.chunk == null) {
                return false;
            }
            allocations++;
            initBuf(entry.chunk, entry.handle, buf, reqCapacity);
            // only null out the chunk as we only use the chunk to check if the buffer is full or not.
            entry.chunk = null;
            head = nextIdx(head);
            return true;
        }

        /**
         * Clear out this cache and free up all previous cached {@link PoolChunk}s and {@code handle}s.
         */
        public void free() {
            allocations = 0;
            for (int i = head;; i = nextIdx(i)) {
                if (!freeEntry(entries[i])) {
                    // all cleared
                    return;
                }
            }
        }

        /**
         * Free up cached {@link PoolChunk}s if not allocated frequently enough.
         */
        public void freeUpIfNecessary() {
            long allocs = allocations;
            allocations = 0;

            // free up all cached buffers until it match the allocation count over the last perioid
            if (allocs < size()) {
                for (int i = head; allocs > 0; i = nextIdx(i)) {
                    if (!freeEntry(entries[i])) {
                        // all freed
                        return;
                    }
                    allocs--;
                }
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static boolean freeEntry(Entry entry) {
            PoolChunk chunk = entry.chunk;
            if (chunk == null) {
                return false;
            }
            // need to synchronize on the area from which it was allocated before.
            synchronized (chunk.arena) {
                chunk.parent.free(chunk, entry.handle);
            }
            entry.chunk = null;
            return true;
        }

        /**
         * Return the number of cached entries.
         */
        private int size()  {
            return tail - head & entries.length - 1;
        }

        private int nextIdx(int index) {
            // use bitwise operation as this is faster as using modulo.
            return (index + 1) & entries.length - 1;
        }

        private static final class Entry<T> {
            PoolChunk<T> chunk;
            long handle;
        }
    }
}
