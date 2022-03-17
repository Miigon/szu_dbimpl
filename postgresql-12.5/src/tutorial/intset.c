/*
 * src/tutorial/intset.c
 *
   
   my implementation of the intset type.
   Author: Miigon 2022-03-17
   https://github.com/Miigon
   
******************************************************************************/

#include <stdbool.h>	/* needed for bools and `true`/`false` */

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */

PG_MODULE_MAGIC;

// NOTE: didn't use struct, to avoid issues caused by c-struct alignment and padding 
// discrepancy between different platforms.

// NOTE: In compliance with the requirements in Assignment 1,
// intset need not support negative numbers
typedef void* intset_ptr;		// intset
typedef uint32_t eintset_t; // intset element (can be changed)

// structure: [len: 4bytes] [cardi: 4bytes] [sorted_array: (len - 8)bytes]
// max cardinality is (len - 8)/sizeof(eintset_t)

// underlying data-sturcture of intset is a sorted (non-decreasing) array.

// headers
#define ISET_HEADER_SZ (sizeof(uint32_t)*2)
// Note: Per https://www.postgresql.org/docs/12/xfunc-c.html#XFUNC-C-BASETYPE
// the length of the object should never be read or set directly.
// (due to possible platform-specific encoding of the length)
// SET_VARSIZE and VARSIZE should be used to set and retrieve the object length.
#define ISET_SZ(iset) (VARSIZE(iset))
#define ISET_CARDI(iset) (((uint32_t*)(iset))[1])

// cardinality & underlying sorted array
#define ISET_MAX_CARDI(iset) ((ISET_SZ(iset)-ISET_HEADER_SZ)/sizeof(eintset_t))
#define ISET_ARR(iset) ((eintset_t*)((char*)(iset)+ISET_HEADER_SZ))
#define ISET_MIN_TOTAL_SZ_FOR_CARDI(cardi) (ISET_HEADER_SZ + ((cardi) * sizeof(eintset_t)))

PG_FUNCTION_INFO_V1(intset_in);

static int
eintset_cmp(const void *a, const void *b) {
	return *(eintset_t*)a - *(eintset_t*)b;
}

// allocate a intset of cardinality `cardi`.
static inline intset_ptr
alloc_intset(uint32_t cardi) {
	uint32_t total_size = ISET_MIN_TOTAL_SZ_FOR_CARDI(cardi);
	intset_ptr iset = (intset_ptr)palloc(total_size);
	SET_VARSIZE(iset, total_size);
	ISET_CARDI(iset) = cardi;
	return iset;
}

static inline intset_ptr
clone_intset(intset_ptr src) {
	uint32_t cardi_src = ISET_CARDI(src);
	intset_ptr iset = alloc_intset(cardi_src);
	memcpy(ISET_ARR(iset), ISET_ARR(src), cardi_src * sizeof(eintset_t));
	return iset;
}

Datum
intset_in(PG_FUNCTION_ARGS) {
	char *str = PG_GETARG_CSTRING(0);
	intset_ptr result;

	char *p = str;

	// precount the number of slots to reserve for elements
	uint32_t nreserve_slot = 1;
	while(*p != '\0') {
		if(*p == ',') nreserve_slot++;
		p++;
	}

	// allocate buffer to hold all the input elements (which may contain duplication)
	eintset_t *buf = (eintset_t *)palloc(nreserve_slot * sizeof(eintset_t));
	
	p = str;
	int	bracket_state = 0; // 0-none    1-left    2-left and right
	enum {
		NS_NOT_EXPECTING_NUM,	// not expecting a number
		NS_EXPECTING_NUM,		// expecting a new number
		NS_INSIDE_NUM,			// inside a number, expect next character to be a digit
	} number_state = NS_NOT_EXPECTING_NUM;
	
	uint32_t last_num = 0;
	uint32_t nelements = 0; // may contain duplicate elements
	while(*p != '\0') {
		switch(*p) {
			case ' ':
			case '\t':
			case '\r':
			case '\n':
			case '\v':
			case '\f':
				// whitespaces
				if(number_state == NS_INSIDE_NUM) {
					buf[nelements++] = last_num;
					number_state = NS_NOT_EXPECTING_NUM;
				}
				break;
			case '{':
				if(bracket_state == 0) {
					bracket_state = 1;
					number_state = NS_EXPECTING_NUM;
				} else {
					goto syntax_error;
				}
				break;
			case '}':
				if(bracket_state == 1){
					bracket_state = 2;
					if(number_state == NS_INSIDE_NUM) {
						// finish the last number
						buf[nelements++] = last_num;
					}
					if(number_state == NS_EXPECTING_NUM && nelements != 0) {
						// trailing comma
						goto syntax_error;
					}
					// not really needed, just for consistency
					number_state = NS_NOT_EXPECTING_NUM;
				} else {
					goto syntax_error;
				}
				break;
			default:
				// contents within brackets
				if(bracket_state != 1) {
					goto syntax_error;
				}
				if(*p >= '0' && *p <= '9') {
					if(number_state == NS_NOT_EXPECTING_NUM) {
						goto syntax_error;
					}
					if(number_state == NS_EXPECTING_NUM) {
						number_state = NS_INSIDE_NUM;
						last_num = 0;
					}
					last_num = last_num * 10 + (*p-'0');
				} else if(*p == ',') {
					if(number_state == NS_INSIDE_NUM || number_state == NS_NOT_EXPECTING_NUM) {
						buf[nelements++] = last_num;
						number_state = NS_EXPECTING_NUM;
					} else {
						goto syntax_error;
					}
				} else { // unrecognized character
					goto syntax_error;
				}
				break;
		}
		p++;
	}

	if(bracket_state != 2 || number_state != NS_NOT_EXPECTING_NUM) {
		// unclosed bracket
		goto syntax_error;
	}

	if(nelements == 0) { // empty set
		pfree(buf);
		result = alloc_intset(0);
		PG_RETURN_POINTER(result);
	} 
	
	// non-empty set, sort & deduplicate
	pg_qsort(buf, nelements, sizeof(eintset_t), eintset_cmp);
	uint32_t cardi = 1;
	eintset_t last = buf[0];
	for(int i=1;i<nelements;i++) {
		if(buf[i] == last) {
			continue;
		}
		buf[cardi++] = buf[i];
		last = buf[i];
	}

	result = alloc_intset(cardi);
	memcpy(ISET_ARR(result), buf, sizeof(eintset_t) * cardi);
	pfree(buf);
	PG_RETURN_POINTER(result);

	syntax_error:
	ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"intset", str)));
}

// a fast algorithm to compute the number of base 10 digits in a uint32 number
// source & credit: https://stackoverflow.com/questions/25892665
// fix the bug where x = 0 the algorithm incorrectly returns 0 instead of 1

static uint32_t
ndigits_base2(uint32_t x) {
	// __builtin_clz: need architectural support
	if(x == 0) return 1;
    return x ? 32 - __builtin_clz(x) : 0;
}

static uint32_t
ndigits_base10(uint32_t x) {
	if(x == 0) return 1;
    static const unsigned char lookup[33] = {
        0, 0, 0, 0, 1, 1, 1, 2, 2, 2,
        3, 3, 3, 3, 4, 4, 4, 5, 5, 5,
        6, 6, 6, 6, 7, 7, 7, 8, 8, 8,
        9, 9, 9
    };
    static const unsigned int powers_10[] = {
        1, 10, 100, 1000, 10000, 100000, 
        1000000, 10000000, 100000000, 1000000000,
    };
    unsigned int digits = lookup[ndigits_base2(x)];
    return digits + (x >= powers_10[digits]);
}

PG_FUNCTION_INFO_V1(intset_out);

Datum
intset_out(PG_FUNCTION_ARGS) {
	intset_ptr iset = (intset_ptr)PG_GETARG_POINTER(0);
	char *result;

	uint32_t cardi = ISET_CARDI(iset);
	eintset_t *arr = ISET_ARR(iset);

	// count the memory space needed for result
	uint32_t reslen = 2; // "{}"
	for(uint32_t i=0; i<cardi; i++) {
		reslen += ndigits_base10(arr[i]);
		if(i != cardi-1) {
			reslen += 1; // ','
		}
	}

	result = (char*)palloc(reslen + 1); // zero-terminated

	char *p = result;
	
	*(p++) = '{';
	for(uint32_t i=0; i<cardi; i++) {
		eintset_t num = arr[i];
		uint32 ndigits = ndigits_base10(num);

		p += ndigits - 1;
		for(uint32_t j=0;j<ndigits;j++) {
			*(p--) = '0' + (num % 10);
			num /= 10;
		}
		p += ndigits + 1;
		if(i != cardi - 1) {
			*(p++) = ',';
		}		
	}
	*(p++) = '}';
	
	result[reslen] = '\0';
	PG_RETURN_CSTRING(result);
}

// ***************** binary input/output functions *****************

PG_FUNCTION_INFO_V1(intset_recv);

Datum
intset_recv(PG_FUNCTION_ARGS) {
	StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	intset_ptr result;

	uint32_t cardi = pq_getmsgint(buf, 4);
	result = alloc_intset(cardi);
	eintset_t *arr = ISET_ARR(result);
	// arr provided by input binary stream is assumed to be sorted.
	for(uint32_t i=0;i<cardi;i++) {
		arr[i] = pq_getmsgint(buf, 4);
	}

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(intset_send);

// Note: produces platform-independent (endianness-independent) binary
// representation of iset
Datum
intset_send(PG_FUNCTION_ARGS) {
	StringInfoData buf;
	
	intset_ptr iset = (intset_ptr) PG_GETARG_POINTER(0);
	uint32_t cardi = ISET_CARDI(iset);
	eintset_t *arr = ISET_ARR(iset);

	pq_begintypsend(&buf);
	pq_sendint32(&buf, cardi);
	for(uint32_t i=0;i<cardi;i++) {
		pq_sendint32(&buf, arr[i]);
	}
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

// ***************** operators *****************

PG_FUNCTION_INFO_V1(intset_contain);

Datum
intset_contain(PG_FUNCTION_ARGS) {
	eintset_t a = PG_GETARG_UINT32(0);
	intset_ptr iset = (intset_ptr) PG_GETARG_POINTER(1);
	uint32_t cardi = ISET_CARDI(iset);
	eintset_t *arr = ISET_ARR(iset);
	
	if(cardi == 0) { PG_RETURN_BOOL(false); }

	uint32_t l = 0, r = cardi-1;
	while(l<=r) {
		uint32_t mid = (l+r)/2;
		if(arr[mid] == a) {
			PG_RETURN_BOOL(true);
		}
		if(arr[mid] < a) {
			l = mid + 1;
		} else if(arr[mid] > a) {
			if(mid == 0) { break; } // prevent uint underflow.
			r = mid - 1;
		}
	}

	PG_RETURN_BOOL(false);
}

PG_FUNCTION_INFO_V1(intset_cardinality);

Datum
intset_cardinality(PG_FUNCTION_ARGS)
{
	intset_ptr iset = (intset_ptr) PG_GETARG_POINTER(0);

	PG_RETURN_UINT32(ISET_CARDI(iset));
}

PG_FUNCTION_INFO_V1(intset_improper_superset);

int intset_improper_superset_internal(intset_ptr a, intset_ptr b) {
	eintset_t *arr_a = ISET_ARR(a);
	eintset_t *arr_b = ISET_ARR(b);
	uint32_t cardi_a = ISET_CARDI(a);
	uint32_t cardi_b = ISET_CARDI(b);

	if(cardi_b == 0) { return true; } // b is empty set
	if(cardi_a < cardi_b) { return false; }
	if(arr_b[0] < arr_a[0] || arr_b[cardi_b-1] > arr_a[cardi_a-1]) {
		// the range of elements in b exceeded the range of elements in a
		// either at the lower end or the higher end (or both).
		// In either case, a cannot be an improper superset of b
		return false;
	}

	uint32_t i = 0;
	for(uint32_t j=0;j<cardi_b;j++) {
		uint32_t x = arr_b[j];
		while(i < cardi_a && arr_a[i] < x) i++;
		if(i >= cardi_a || arr_a[i] != x) {
			// b contains an element that's not found in a
			return false;
		}
	}
	return true;
}

Datum
intset_improper_superset(PG_FUNCTION_ARGS) {
	intset_ptr a = (intset_ptr) PG_GETARG_POINTER(0);
	intset_ptr b = (intset_ptr) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(intset_improper_superset_internal(a, b));
}

PG_FUNCTION_INFO_V1(intset_improper_subset);

Datum
intset_improper_subset(PG_FUNCTION_ARGS) {
	intset_ptr a = (intset_ptr) PG_GETARG_POINTER(0);
	intset_ptr b = (intset_ptr) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(intset_improper_superset_internal(b, a));
}

PG_FUNCTION_INFO_V1(intset_equal);

bool intset_equal_internal(intset_ptr a, intset_ptr b) {
	uint32_t cardi_a = ISET_CARDI(a);
	uint32_t cardi_b = ISET_CARDI(b);

	if(cardi_a != cardi_b) { return false; }
	return (memcmp(ISET_ARR(a), ISET_ARR(b), cardi_a * sizeof(eintset_t)) == 0);
}

Datum
intset_equal(PG_FUNCTION_ARGS) {
	intset_ptr a = (intset_ptr) PG_GETARG_POINTER(0);
	intset_ptr b = (intset_ptr) PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(intset_equal_internal(a, b));
}

PG_FUNCTION_INFO_V1(intset_notequal);

Datum
intset_notequal(PG_FUNCTION_ARGS) {
	intset_ptr a = (intset_ptr) PG_GETARG_POINTER(0);
	intset_ptr b = (intset_ptr) PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(!intset_equal_internal(a, b));
}

PG_FUNCTION_INFO_V1(intset_intersect);

Datum
intset_intersect(PG_FUNCTION_ARGS) {
	intset_ptr a = (intset_ptr) PG_GETARG_POINTER(0);
	intset_ptr b = (intset_ptr) PG_GETARG_POINTER(1);
	intset_ptr result;
	eintset_t *arr_a = ISET_ARR(a);
	eintset_t *arr_b = ISET_ARR(b);
	uint32_t cardi_a = ISET_CARDI(a);
	uint32_t cardi_b = ISET_CARDI(b);

	if(cardi_a == 0 || cardi_b == 0) {
		result = alloc_intset(0);
		PG_RETURN_POINTER(result);
	}
	
	// precount intersection cardinality
	// (trade time for space)
	uint32_t i=0, j=0, cardi=0;
	while(i < cardi_a && j < cardi_b) {
		if(arr_a[i] > arr_b[j]) { j++; }
		else if(arr_a[i] < arr_b[j]) { i++; }
		else { i++; j++; cardi++; }
	}
	
	result = alloc_intset(cardi);
	eintset_t *arr = ISET_ARR(result);

	i=0, j=0, cardi=0;
	while(i < cardi_a && j < cardi_b) {
		if(arr_a[i] > arr_b[j]) { j++; }
		else if(arr_a[i] < arr_b[j]) { i++; }
		else { arr[cardi++] = arr_a[i]; i++; j++; }
	}

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(intset_union);

Datum
intset_union(PG_FUNCTION_ARGS) {
	intset_ptr a = (intset_ptr) PG_GETARG_POINTER(0);
	intset_ptr b = (intset_ptr) PG_GETARG_POINTER(1);
	intset_ptr result;
	eintset_t *arr_a = ISET_ARR(a);
	eintset_t *arr_b = ISET_ARR(b);
	uint32_t cardi_a = ISET_CARDI(a);
	uint32_t cardi_b = ISET_CARDI(b);

	if(cardi_a == 0) {
		result = clone_intset(b);
		PG_RETURN_POINTER(result);
	}
	if(cardi_b == 0) {
		result = clone_intset(a);
		PG_RETURN_POINTER(result);
	}
	
	// precount union cardinality
	// (trade time for space)
	uint32_t i=0, j=0, cardi=0;
	while(i < cardi_a && j < cardi_b) {
		if(arr_a[i] > arr_b[j]) { j++; cardi++; }
		else if(arr_a[i] < arr_b[j]) { i++; cardi++; }
		else { i++; j++; cardi++; }
	}
	cardi += cardi_a - i; // remaining elements from a
	cardi += cardi_b - j; // remaining elements from b
	
	result = alloc_intset(cardi);
	eintset_t *arr = ISET_ARR(result);

	i=0, j=0, cardi=0;
	while(i < cardi_a && j < cardi_b) {
		if(arr_a[i] > arr_b[j]) { arr[cardi++] = arr_b[j++]; }
		else if(arr_a[i] < arr_b[j]) { arr[cardi++] = arr_a[i++]; }
		else { arr[cardi++] = arr_a[i]; i++; j++; }
	}
	// remaining elements from a
	while(i < cardi_a) {
		arr[cardi++] = arr_a[i++];
	}
	// remaining elements from b
	while(j < cardi_b) {
		arr[cardi++] = arr_b[j++];
	}

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(intset_disjunct);

Datum
intset_disjunct(PG_FUNCTION_ARGS) {
	intset_ptr a = (intset_ptr) PG_GETARG_POINTER(0);
	intset_ptr b = (intset_ptr) PG_GETARG_POINTER(1);
	intset_ptr result;
	eintset_t *arr_a = ISET_ARR(a);
	eintset_t *arr_b = ISET_ARR(b);
	uint32_t cardi_a = ISET_CARDI(a);
	uint32_t cardi_b = ISET_CARDI(b);

	if(cardi_a == 0) {
		result = clone_intset(b);
		PG_RETURN_POINTER(result);
	}
	if(cardi_b == 0) {
		result = clone_intset(a);
		PG_RETURN_POINTER(result);
	}
	
	// precount disjunction cardinality
	// (trade time for space)
	uint32_t i=0, j=0, cardi=0;
	while(i < cardi_a && j < cardi_b) {
		if(arr_a[i] > arr_b[j]) { j++; cardi++; }
		else if(arr_a[i] < arr_b[j]) { i++; cardi++; }
		else { i++; j++; }
	}
	cardi += cardi_a - i; // remaining elements from a
	cardi += cardi_b - j; // remaining elements from b
	
	result = alloc_intset(cardi);
	eintset_t *arr = ISET_ARR(result);

	i=0, j=0, cardi=0;
	while(i < cardi_a && j < cardi_b) {
		if(arr_a[i] > arr_b[j]) { arr[cardi++] = arr_b[j++]; }
		else if(arr_a[i] < arr_b[j]) { arr[cardi++] = arr_a[i++]; }
		else { i++; j++; }
	}
	// remaining elements from a
	while(i < cardi_a) {
		arr[cardi++] = arr_a[i++];
	}
	// remaining elements from b
	while(j < cardi_b) {
		arr[cardi++] = arr_b[j++];
	}

	PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(intset_subtract);

Datum
intset_subtract(PG_FUNCTION_ARGS) {
	intset_ptr a = (intset_ptr) PG_GETARG_POINTER(0);
	intset_ptr b = (intset_ptr) PG_GETARG_POINTER(1);
	intset_ptr result;
	eintset_t *arr_a = ISET_ARR(a);
	eintset_t *arr_b = ISET_ARR(b);
	uint32_t cardi_a = ISET_CARDI(a);
	uint32_t cardi_b = ISET_CARDI(b);

	if(cardi_a == 0) {
		result = alloc_intset(0);
		PG_RETURN_POINTER(result);
	}
	if(cardi_b == 0) {
		result = clone_intset(a);
		PG_RETURN_POINTER(result);
	}
	
	// precount disjunction cardinality
	// (trade time for space)
	uint32_t i=0, j=0, cardi=0;
	while(i < cardi_a && j < cardi_b) {
		if(arr_a[i] > arr_b[j]) { j++; }
		else if(arr_a[i] < arr_b[j]) { i++; cardi++; }
		else { i++; j++; }
	}
	cardi += cardi_a - i; // remaining elements from a
	
	result = alloc_intset(cardi);
	eintset_t *arr = ISET_ARR(result);

	i=0, j=0, cardi=0;
	while(i < cardi_a && j < cardi_b) {
		if(arr_a[i] > arr_b[j]) { j++; }
		else if(arr_a[i] < arr_b[j]) { arr[cardi++] = arr_a[i++]; }
		else { i++; j++; }
	}
	// remaining elements from a
	while(i < cardi_a) {
		arr[cardi++] = arr_a[i++];
	}

	PG_RETURN_POINTER(result);
}

