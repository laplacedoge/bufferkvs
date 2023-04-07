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

#include <stdlib.h>
#include <string.h>

#include "bufferkvs.h"
#include "bufferqueue.h"

/* context of the buffer key-value set. */
struct _bkvs_ctx {
    struct _bkvs_ctx_conf {

        /* hash callback function for the key. */
        bkvs_hash_cb hash_cb;

        /* number of the buckets. */
        bkvs_u32 bucket_num;

        /* maximum number of the the key-value pairs. */
        bkvs_u32 pair_num_max;
    } conf;
    struct _bkvs_ctx_cache {

        /* number of the the key-value pairs. */
        bkvs_u32 pair_num;
    } cache;

    /* buckets. */
    bque_ctx *buckets[];
};

/* pair of the key-value. */
typedef struct _bkvs_pair {
    bkvs_u32 key_size;
    bkvs_u32 value_size;
    char *key;
    char *value;
} bkvs_pair;

typedef struct _bkvs_search_ctx {
    const char *key;
    bque_u32 key_size;
    bque_u32 bucket_idx;
    bque_u32 pair_idx;
    bque_buff buff;
} bkvs_search_ctx;

bkvs_u32 bkvs_hash_cb_djb2(const char *str) {
    bkvs_u32 hash = 5381;
    bkvs_u32 c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }

    return hash;
}

bkvs_u32 bkvs_hash_cb_sdbm(const char *str) {
    bkvs_u32 hash = 0;
    bkvs_u32 c;

    while ((c = *str++)) {
        hash = c + (hash << 6) + (hash << 16) - hash;
    }

    return hash;
}

#define BKVS_DEF_HASH_CB        bkvs_hash_cb_djb2

#define BKVS_DEF_BUCKET_NUM     128

#define BKVS_DEF_PAIR_NUM_MAX   1024

static bkvs_search_ctx search_ctx = {0};

/**
 * @brief create a buffer key-value set.
 * 
 * @param ctx the address of the context pointer.
 * @param conf configuration pointer.
*/
bkvs_res bkvs_new(bkvs_ctx **ctx, bkvs_conf *conf) {
    bkvs_ctx *alloc_ctx;
    bkvs_hash_cb hash_cb;
    bkvs_u32 bucket_num;
    bkvs_u32 pair_num_max;
    bkvs_u32 alloc_size;

    BKVS_ASSERT(ctx != NULL);

    /* configure context. */
    if (conf != NULL) {
        if (conf->hash_cb != NULL) {
            hash_cb = conf->hash_cb;
        } else {
            hash_cb = BKVS_DEF_HASH_CB;
        }
        if (conf->bucket_num != 0) {
            bucket_num = conf->bucket_num;
        } else {
            bucket_num = BKVS_DEF_BUCKET_NUM;
        }
        pair_num_max = conf->pair_num_max;
    } else {
        hash_cb = BKVS_DEF_HASH_CB;
        bucket_num = BKVS_DEF_BUCKET_NUM;
        pair_num_max = BKVS_DEF_PAIR_NUM_MAX;
    }

    /* allocate context. */
    alloc_size = sizeof(bkvs_ctx) + sizeof(bque_ctx *) * bucket_num;
    alloc_ctx = (bkvs_ctx *)malloc(alloc_size);
    if (alloc_ctx == NULL) {
        return BKVS_ERR_NO_MEM;
    }

    /* initialize context. */
    memset(alloc_ctx, 0, sizeof(bkvs_ctx));
    alloc_ctx->conf.hash_cb = hash_cb;
    alloc_ctx->conf.bucket_num = bucket_num;
    alloc_ctx->conf.pair_num_max = pair_num_max;

    /* output context. */
    *ctx = alloc_ctx;

    return BKVS_OK;
}

/**
 * @brief delete the buffer key-value set.
 * 
 * @param ctx context pointer.
*/
bkvs_res bkvs_del(bkvs_ctx *ctx) {
    BKVS_ASSERT(ctx != NULL);

    /* delete key-value pair queues. */
    bkvs_empty(ctx);

    /* free context. */
    free(ctx);

    return BKVS_OK;
}

bkvs_res bkvs_status(bkvs_ctx *ctx, bkvs_stat *stat) {
    BKVS_ASSERT(ctx != NULL);
    BKVS_ASSERT(stat != NULL);

    /* get status. */
    stat->pair_num = ctx->cache.pair_num;

    return BKVS_OK;
}

static bkvs_res create_pair_que(bque_ctx **ctx) {
    bque_conf conf;
    bque_res res;

    /* configure key-value pair queue. */
    conf.buff_num_max = 0;
    conf.buff_size_max = 0;

    /* create key-value pair queue. */
    res = bque_new(ctx, &conf);
    if (res != BQUE_OK) {
        if (res == BQUE_ERR_NO_MEM) {
            return BKVS_ERR_NO_MEM;
        } else {
            return BKVS_ERR;
        }
    }

    return BKVS_OK;
}

static bkvs_res create_pair(bkvs_pair *pair, const char *key, const void *buff, bkvs_u32 size) {
    bkvs_u32 key_size;
    char *alloc_key;
    char *alloc_value;

    BKVS_ASSERT(pair != NULL);
    BKVS_ASSERT(key != NULL);
    BKVS_ASSERT(buff != NULL);
    BKVS_ASSERT(size != 0);

    /* allocate memory for key. */
    key_size = strlen(key) + 1;
    alloc_key = (char *)malloc(key_size);
    if (alloc_key == NULL) {
        return BKVS_ERR_NO_MEM;
    }

    /* allocate memory for value. */
    alloc_value = (char *)malloc(size);
    if (alloc_value == NULL) {
        free(alloc_key);

        return BKVS_ERR_NO_MEM;
    }

    // /* allocate memory for key-value pair. */
    // alloc_pair = (bkvs_pair *)malloc(sizeof(bkvs_pair));
    // if (alloc_pair == NULL) {
    //     free(alloc_key);
    //     free(alloc_value);

    //     return BKVS_ERR_NO_MEM;
    // }

    /* copy key and value. */
    memcpy(alloc_key, key, key_size);
    memcpy(alloc_value, buff, size);

    /* initialize key-value pair. */
    pair->key = alloc_key;
    pair->key_size = key_size;
    pair->value = alloc_value;
    pair->value_size = size;

    // /* output key-value pair. */
    // *pair = alloc_pair;

    return BKVS_OK;
}

static bque_res search_key_cb(bque_buff *buff, bque_u32 idx, bque_u32 num) {
    bkvs_pair *pair;

    pair = (bkvs_pair *)buff->ptr;
    if (pair->key_size == search_ctx.key_size &&
        memcmp(pair->key, search_ctx.key, search_ctx.key_size) == 0) {
        search_ctx.pair_idx = idx;
        search_ctx.buff.ptr = buff->ptr;
        search_ctx.buff.size = buff->size;

        return BQUE_ERR_ITER_STOP;
    }

    return BQUE_OK;
}

static bkvs_res search_key(bkvs_ctx *ctx, const char *key) {
    bque_u32 bucket_idx;
    bque_res mod_bque_res;

    BKVS_ASSERT(ctx != NULL);
    BKVS_ASSERT(key != NULL);

    /* hash key string and get the bucket index. */
    bucket_idx = ctx->conf.hash_cb(key) % ctx->conf.bucket_num;
    if (ctx->buckets[bucket_idx] == NULL) {
        return BKVS_ERR_NO_KEY;
    }

    /* search key in the key-value pair queues. */
    search_ctx.key = key;
    search_ctx.key_size = strlen(key) + 1;
    mod_bque_res = bque_foreach(ctx->buckets[bucket_idx], search_key_cb, BQUE_ITER_FORWARD);
    if (mod_bque_res != BQUE_ERR_ITER_STOP) {
        return BKVS_ERR_NO_KEY;
    }

    search_ctx.bucket_idx = bucket_idx;

    return BKVS_OK;
}

bkvs_res bkvs_put(bkvs_ctx *ctx, const char *key, const void *buff, bkvs_u32 size) {
    bque_res mod_bque_res;
    bkvs_res res;

    BKVS_ASSERT(ctx != NULL);
    BKVS_ASSERT(key != NULL);
    BKVS_ASSERT(buff != NULL);
    BKVS_ASSERT(size != 0);

    /* search key. */
    res = search_key(ctx, key);
    if (res == BKVS_ERR_NO_KEY) {
        bkvs_u32 bucket_idx;
        bkvs_pair pair;

        /* hash key string and get the bucket index. */
        bucket_idx = ctx->conf.hash_cb(key) % ctx->conf.bucket_num;

        /* create key-value pair queue. */
        if (ctx->buckets[bucket_idx] == NULL) {
            res = create_pair_que(&ctx->buckets[bucket_idx]);
            if (res != BKVS_OK) {
                return res;
            }
        }

        /* put key-value pair. */
        res = create_pair(&pair, key, buff, size);
        if (res != BKVS_OK) {
            return res;
        }
        mod_bque_res = bque_enqueue(ctx->buckets[bucket_idx], &pair, sizeof(bkvs_pair));
        if (mod_bque_res != BQUE_OK) {
            if (mod_bque_res == BQUE_ERR_NO_MEM) {
                return BKVS_ERR_NO_MEM;
            } else {
                return BKVS_ERR;
            }
        }

        /* update key-value pair number. */
        ctx->cache.pair_num++;
    } else if (res == BKVS_OK) {
        char *alloc_value;
        bkvs_pair *pair;

        /* allocate memory for value. */
        alloc_value = (char *)malloc(size);
        if (alloc_value == NULL) {
            return BKVS_ERR_NO_MEM;
        }

        /* copy value. */
        memcpy(alloc_value, buff, size);

        /* update value. */
        pair = (bkvs_pair *)search_ctx.buff.ptr;
        free(pair->value);
        pair->value = alloc_value;
        pair->value_size = size;
    } else {
        return res;
    }

    return BKVS_OK;
}

bkvs_res bkvs_drop(bkvs_ctx *ctx, const char *key) {
    bque_res mod_bque_res;
    bkvs_res res;
    bkvs_pair *pair;

    BKVS_ASSERT(ctx != NULL);
    BKVS_ASSERT(key != NULL);

    /* search key. */
    res = search_key(ctx, key);
    if (res != BKVS_OK) {
        return res;
    }

    /* delete key-value pair. */
    pair = (bkvs_pair *)search_ctx.buff.ptr;
    free(pair->key);
    free(pair->value);
    mod_bque_res = bque_drop(ctx->buckets[search_ctx.bucket_idx], search_ctx.pair_idx, NULL, NULL);
    if (mod_bque_res != BQUE_OK) {
        return BKVS_ERR;
    }

    /* update key-value pair number. */
    ctx->cache.pair_num--;

    return BKVS_OK;
}

static bque_res empty_cb(bque_buff *buff, bque_u32 idx, bque_u32 num) {
    bkvs_pair *pair;

    pair = (bkvs_pair *)buff->ptr;
    free(pair->key);
    free(pair->value);

    return BQUE_OK;
}

bkvs_res bkvs_empty(bkvs_ctx *ctx) {
    BKVS_ASSERT(ctx != NULL);

    /* empty key-value pair queues. */
    for (bkvs_u32 i = 0; i < ctx->conf.bucket_num; i++) {
        if (ctx->buckets[i] != NULL) {
            bque_foreach(ctx->buckets[i], empty_cb, BQUE_ITER_FORWARD);
            bque_del(ctx->buckets[i]);
        }
    }
    memset(ctx->buckets, 0, sizeof(bque_ctx *) * ctx->conf.bucket_num);

    return BKVS_OK;
}

bkvs_res bkvs_has(bkvs_ctx *ctx, const char *key) {
    bkvs_res res;
    bkvs_pair *pair;

    BKVS_ASSERT(ctx != NULL);

    /* search key. */
    return search_key(ctx, key);
}

bkvs_res bkvs_get(bkvs_ctx *ctx, const char *key, bkvs_buff *buff) {
    bkvs_res res;
    bkvs_pair *pair;

    BKVS_ASSERT(ctx != NULL);
    BKVS_ASSERT(key != NULL);
    BKVS_ASSERT(buff != NULL);
    BKVS_ASSERT(size != NULL);

    /* search key. */
    res = search_key(ctx, key);
    if (res != BKVS_OK) {
        return res;
    }

    /* copy value. */
    pair = (bkvs_pair *)search_ctx.buff.ptr;
    buff->ptr = (bkvs_u8 *)pair->value;
    buff->size = pair->value_size;

    return BKVS_OK;
}

bkvs_res bkvs_foreach(bkvs_ctx *ctx, bkvs_foreach_cb cb) {
    bque_res mod_bque_res;
    bque_stat mod_bque_stat;
    bque_buff mod_bque_buff;
    bque_u32 pair_idx;
    bkvs_pair *pair;
    bkvs_buff buff;
    bkvs_res res;

    BKVS_ASSERT(ctx != NULL);
    BKVS_ASSERT(cb != NULL);

    /* foreach key-value pair queues. */
    pair_idx = 0;
    for (bkvs_u32 i = 0; i < ctx->conf.bucket_num; i++) {
        if (ctx->buckets[i] != NULL) {

            mod_bque_res = bque_status(ctx->buckets[i], &mod_bque_stat);
            if (mod_bque_res != BQUE_OK) {
                return BKVS_ERR;
            }

            for (bkvs_u32 j = 0; j < mod_bque_stat.buff_num; j++) {
                mod_bque_res = bque_item(ctx->buckets[i], j, &mod_bque_buff);
                if (mod_bque_res != BQUE_OK) {
                    return BKVS_ERR;
                }

                pair = (bkvs_pair *)mod_bque_buff.ptr;
                buff.ptr = (bkvs_u8 *)pair->value;
                buff.size = pair->value_size;
                res = cb(pair->key, &buff, pair_idx, ctx->cache.pair_num);
                if (res == BKVS_ERR_ITER_STOP) {
                    return BKVS_ERR_ITER_STOP;
                }
                pair_idx++;
            }
        }
    }

    return BKVS_OK;
}
