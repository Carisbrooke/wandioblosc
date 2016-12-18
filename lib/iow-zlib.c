/*
 *
 * Copyright (c) 2007-2016 The University of Waikato, Hamilton, New Zealand.
 * All rights reserved.
 *
 * This file is part of libwandio.
 *
 * This code has been developed by the University of Waikato WAND
 * research group. For further information please see http://www.wand.net.nz/
 *
 * libwandio is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * libwandio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 */


#include "config.h"
#include <zlib.h>
#include "wandio.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <blosc.h>

//repu1sion -----
#define NUM_THREADS 4
#define COMPRESSOR "zlib"
#define BUF_OUT_SIZE 1024*1024
//---------------

/* Libwandio IO module implementing a zlib writer */

enum err_t {
	ERR_OK	= 1,
	ERR_EOF = 0,
	ERR_ERROR = -1
};

struct zlibw_t {
	z_stream strm;
	Bytef outbuff[BUF_OUT_SIZE];
	iow_t *child;
	enum err_t err;
	int inoffset;
};


extern iow_source_t zlib_wsource; 

#define DATA(iow) ((struct zlibw_t *)((iow)->data))
#define min(a,b) ((a)<(b) ? (a) : (b))




iow_t *zlib_wopen(iow_t *child, int compress_level)
{
	iow_t *iow;
	if (!child)
		return NULL;
	iow = malloc(sizeof(iow_t));
	iow->source = &zlib_wsource;
	iow->data = malloc(sizeof(struct zlibw_t));

	//repu1sion -----
	int rv = 0;
	int num_threads = NUM_THREADS;

        blosc_init();

        blosc_set_nthreads(num_threads);
        printf("Using %d threads \n", num_threads);

        rv = blosc_set_compressor(COMPRESSOR);
        if (rv < 0)
        {
                printf("Error setting compressor %s\n", COMPRESSOR);
                return NULL;
        }
        printf("Using %s compressor\n", COMPRESSOR);
	//---------------

	DATA(iow)->child = child;

	DATA(iow)->strm.next_in = NULL;
	DATA(iow)->strm.avail_in = 0;
	DATA(iow)->strm.next_out = DATA(iow)->outbuff;
	DATA(iow)->strm.avail_out = sizeof(DATA(iow)->outbuff);
	DATA(iow)->strm.zalloc = Z_NULL;
	DATA(iow)->strm.zfree = Z_NULL;
	DATA(iow)->strm.opaque = NULL;
	DATA(iow)->err = ERR_OK;

	deflateInit2(&DATA(iow)->strm, 
			compress_level,	/* Level */
			Z_DEFLATED, 	/* Method */
			15 | 16, 	/* 15 bits of windowsize, 16 == use gzip header */
			9,		/* Use maximum (fastest) amount of memory usage */
			Z_DEFAULT_STRATEGY
		);

	return iow;
}

static int64_t zlib_wwrite(iow_t *iow, const char *buffer, int64_t len)
{
	if (DATA(iow)->err == ERR_EOF) {
		return 0; /* EOF */
	}
	if (DATA(iow)->err == ERR_ERROR) {
		return -1; /* ERROR! */
	}

	//repu1sion -----
	int csize;
	const char *dta = buffer;
	int isize = (int)len;
	int osize = BUF_OUT_SIZE;
	
	//---------------
	DATA(iow)->strm.next_in = (Bytef*)buffer;  
	DATA(iow)->strm.avail_in = len;

	while (DATA(iow)->err == ERR_OK && DATA(iow)->strm.avail_in > 0) 
	{	//repu1sion: when buffer is full we write it to file and set next_out, avail_out again
		while (DATA(iow)->strm.avail_out <= 0) 
		{
			int bytes_written = wandio_wwrite(DATA(iow)->child, (char *)DATA(iow)->outbuff, sizeof(DATA(iow)->outbuff));
			if (bytes_written <= 0) 
			{	//error
				DATA(iow)->err = ERR_ERROR;
				/* Return how much data we managed to write ok */
				if (DATA(iow)->strm.avail_in != (uint32_t)len)
					return len-DATA(iow)->strm.avail_in;
				/* Now return error */
				return -1;
			}
			DATA(iow)->strm.next_out = DATA(iow)->outbuff;
			DATA(iow)->strm.avail_out = sizeof(DATA(iow)->outbuff);
		}
		//repu1sion: do the blosc compression on buffer
		csize = blosc_compress(9, 1, sizeof(char), isize, dta, DATA(iow)->strm.next_out, osize);
		printf("input data size: %d , compressed data size: %d \n", isize, csize);
		//repu1sion: manage all avail_in, avail_out, next_out vars.
		DATA(iow)->strm.avail_in -= isize;	//repu1sion: it should be 0, anyway
		DATA(iow)->strm.avail_out -= csize;	//repu1sion: decrease available space in output buffer
		DATA(iow)->strm.next_out += csize;	//repu1sion: move pointer forward
		
#if 0
		/* Decompress some data into the output buffer */
		int err=deflate(&DATA(iow)->strm, 0);
		switch(err) {
			case Z_OK:
				DATA(iow)->err = ERR_OK;
				break;
			default:
				DATA(iow)->err = ERR_ERROR;
		}
#endif
	}
	/* Return the number of bytes compressed */
	return len-DATA(iow)->strm.avail_in;	//repulsion: len - 0 = len, so we mostly return len here
}

//XXX - replace deflate() here too
static void zlib_wclose(iow_t *iow)
{
	int res;
	
	while (1) {
		//XXX - replace it
		res = deflate(&DATA(iow)->strm, Z_FINISH);

		if (res == Z_STREAM_END)
			break;
		if (res == Z_STREAM_ERROR) {
			fprintf(stderr, "Z_STREAM_ERROR while closing output\n");
			break;
		}
	
		wandio_wwrite(DATA(iow)->child, 
				(char*)DATA(iow)->outbuff,
				sizeof(DATA(iow)->outbuff)-DATA(iow)->strm.avail_out);
		DATA(iow)->strm.next_out = DATA(iow)->outbuff;
		DATA(iow)->strm.avail_out = sizeof(DATA(iow)->outbuff);
	}

	deflateEnd(&DATA(iow)->strm);
	wandio_wwrite(DATA(iow)->child, 
			(char *)DATA(iow)->outbuff,
			sizeof(DATA(iow)->outbuff)-DATA(iow)->strm.avail_out);
	wandio_wdestroy(DATA(iow)->child);
	free(iow->data);
	free(iow);


	//repu1sion -----
	blosc_destroy();
	//---------------
}

iow_source_t zlib_wsource = {
	"zlibw",
	zlib_wwrite,
	zlib_wclose
};

