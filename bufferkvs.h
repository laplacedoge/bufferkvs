/**
 * MIT License
 * 
 * Copyright (c) 2023 Alex Chen
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef __BKVS_H__
#define __BKVS_H__

#include <stdint.h>

#ifdef BKVS_DEBUG

#include <stdarg.h>
#include <stdio.h>

#endif

/* basic data types. */
typedef int8_t      bkvs_s8;
typedef uint8_t     bkvs_u8;

typedef int16_t     bkvs_s16;
typedef uint16_t    bkvs_u16;

typedef int32_t     bkvs_s32;
typedef uint32_t    bkvs_u32;

typedef int64_t     bkvs_s64;
typedef uint64_t    bkvs_u64;

enum _bkvs_res {

    /* all is well, so far :) */
    BKVS_OK             = 0,

    /* generic error occurred. */
    BKVS_ERR            = -1,

    /* failed to allocate memory. */
    BKVS_ERR_NO_MEM     = -2,

    /* failed to find the key. */
    BKVS_ERR_NO_KEY     = -3,

    /* iterating stoped. */
    BKVS_ERR_ITER_STOP  = -8,
};



#ifdef BKVS_DEBUG

/* logging function for debug. */
#define BKVS_LOG(fmt, ...)  printf(fmt, ##__VA_ARGS__)

/* assertion macro used in the APIs. */
#define BKVS_ASSERT(expr)   \
    if (!(expr)) { BKVS_LOG("[BKVS] %s:%d: assertion failed: \"%s\"\n", \
        __FILE__, __LINE__, #expr); while (1);};

/* returned result used in the APIs. */
typedef enum _bkvs_res      bkvs_res;

#else

/* returned result used in the APIs. */
typedef bkvs_s32            bkvs_res;

/* assertion macro used in the APIs. */
#define BKVS_ASSERT(expr)

#endif

/* hash callback function for the key. */
typedef bkvs_u32 (*bkvs_hash_cb)(const char *key);

/* configuration of the buffer key-value set. */
typedef struct _bkvs_conf {

    /* hash callback function for the key. */
    bkvs_hash_cb hash_cb;

    /* number of the buckets. */
    bkvs_u32 bucket_num;

    /* maximum number of the the key-value pairs. */
    bkvs_u32 pair_num_max;
} bkvs_conf;

/* status of the buffer key-value set. */
typedef struct _bkvs_stat {

    /* number of the the key-value pairs. */
    bkvs_u32 pair_num;
} bkvs_stat;

typedef struct _bkvs_buff {
    bkvs_u8 *ptr;
    bkvs_u32 size;
} bkvs_buff;

/* context of the buffer key-value set. */
typedef struct _bkvs_ctx    bkvs_ctx;

typedef bkvs_res (*bkvs_foreach_cb)(const char *key, bkvs_buff *buff, bkvs_u32 idx, bkvs_u32 num);

bkvs_u32 bkvs_hash_cb_djb2(const char *str);

bkvs_u32 bkvs_hash_cb_sdbm(const char *str);

bkvs_res bkvs_new(bkvs_ctx **ctx, bkvs_conf *conf);

bkvs_res bkvs_del(bkvs_ctx *ctx);

bkvs_res bkvs_status(bkvs_ctx *ctx, bkvs_stat *stat);

bkvs_res bkvs_put(bkvs_ctx *ctx, const char *key, const void *buff, bkvs_u32 size);

bkvs_res bkvs_drop(bkvs_ctx *ctx, const char *key);

bkvs_res bkvs_empty(bkvs_ctx *ctx);

bkvs_res bkvs_has(bkvs_ctx *ctx, const char *key);

bkvs_res bkvs_get(bkvs_ctx *ctx, const char *key, bkvs_buff *buff);

bkvs_res bkvs_foreach(bkvs_ctx *ctx, bkvs_foreach_cb cb);

#endif
