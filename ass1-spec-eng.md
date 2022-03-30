Summary
-------

**Note** : It will take you quite some time to complete this assignment, therefore, we earnestly recommend that you start working as early as possible.

* Deadline: **2021-4-30   23:59**

* Marks: This assignment contributes **15 marks** toward your total mark for this course.

* Late Penalty: _0.089 marks_ off the ceiling mark for each hour late. After _7 days_ the assignment is worth _0 marks_.

Instructions
------------

- This notebook contains instructions for **Assignment 1**.

- You are required to write your implementation in two files: `intset.c` and `intset.source`.

- You are not allowed to print out too much unnecessary stuff to either screen or log. We will not consider any such output.

- You can submit your implementation via the following link: https://szudseg.cn/submit/

- For each part, we have provided you with detailed instructions. In case of any problem, you can post your query in the Wechat group.

- You are allowed to use any functions from the standard library, 3rd-party libraries are not allowed.

- We will provide **immediate feedback** on your submission. You can view the feedback using the online submission portal on the same day.

- You are allowed an **unlimited** number of Submission Attempts, we will use your **LAST** submission for Final Evaluation. 

- For this assignment, please use the source code of **Postgres 12.5**.

Assignment 1: Adding a Set Data Type to PostgreSQL
----------

This assignment aims to give you:

*   An understanding of how data is treated inside a DBMS
*   Practice in adding a new base type to PostgreSQL

The goal is to implement a new data type for PostgreSQL, complete with
input/output functions and a range of operations.

Make sure that you read this assignment specification _carefully_ and _completely_ before starting work on the assignment.  
Questions which indicate that you haven't done this will simply get the response
"Please read the spec".

We will be using the following names in the discussion below:

`PG_CODE`

The directory where your PostgreSQL source code is located  
(typically `/srvr/$USER/postgresql-12.5`)

`PG_HOME`

The directory where you have installed the PostgreSQL binaries  
(typically `/srvr/$USER/pgsql`)

`PG_DATA`

The directory where you have placed PostgreSQL's `data`  
(typically `/srvr/$USER/pgsql/data`)

`PG_LOG`

the file to where you send PostgreSQL's log output  
(typically `/srvr/$USER/pgsql/log`)

Introduction
------------

PostgreSQL has an extensibility model which, among other things, provides a
well-defined process for adding new data types into a PostgreSQL server. This
capability has led to the development by PostgreSQL users of a number of types
(such as polygons) which have become part of the standard distribution. It also
means that PostgreSQL is the database of choice in research projects which aim
to push the boundaries of what kind of data a DBMS can manage.

In this assignment, we will be adding a new data type for dealing with **sets of
integers**. You may implement the functions for the data type in any way you
like, _provided that_ they satisfy the semantics given below. (in the
[intSets](#the-intset-data-type) section)

Note that arrays in PostgreSQL have some properties and operations that make
them look a little bit like sets. However, they are _not_ sets and have quite
different semantics to the data type that we are asking you to implement.

The process for adding new base data types in PostgreSQL is described in the
following sections of the PostgreSQL documentation:

*   [37.10 C-Language Functions](https://www.postgresql.org/docs/12//xfunc-c.html)
*   [37.13 User-defined Types](https://www.postgresql.org/docs/12//xtypes.html)
*   [37.14 User-defined Operators](https://www.postgresql.org/docs/12//xoper.html)
*   [SQL: CREATE TYPE](https://www.postgresql.org/docs/12//sql-createtype.html)
*   [SQL: CREATE OPERATOR](https://www.postgresql.org/docs/12//sql-createoperator.html)
*   [SQL: CREATE OPERATOR CLASS](https://www.postgresql.org/docs/12//sql-createopclass.html)

Section 37.13 uses an example of a complex number type, which you can use as a
starting point for defining your `intSet` data type (see below). Note that the
complex type is a starting point _only_, to give an idea on how new data types
are added. Don't be fooled into thinking that this assignment just requires you
to change the name complex to intSet; the intSet type is more complex (no pun
intended) than the complex number type.

There are other examples of new data types under the tutorial and contrib
directories. These may or may not give you some useful ideas on how to implement
the `intSet` data type:

*   An auto-encrypted password datatype  
    `PG_CODE/contrib/chkpass/`
*   A case-insensitive character string datatype  
    `PG_CODE/contrib/citext/`
*   A confidence-interval datatype  
    `PG_CODE/contrib/seg/`

Setting Up
----------

You ought to start this assignment with a fresh copy of PostgreSQL, without any
changes that you might have made for the Prac exercises (unless these changes
are trivial). Note that you only need to configure, compile, and install your
PostgreSQL server once for this assignment. All subsequent compilation takes
place in the `src/tutorial` directory, and only requires modification of the
files there.

Once you have re-installed your PostgreSQL server, you should run the following
commands:

``` console
cd PG\_CODE/src/tutorial
cp complex.c intset.c
cp complex.source intset.source
```

Once you've made the `intset` files, you should also edit the `Makefile` in this
directory, and add the green text to the following lines:
``` console
MODULES = complex funcs intset
DATA_built = advanced.sql basics.sql complex.sql funcs.sql syscat.sql intset.sql
```

The rest of the work for this assignment involves editing the `intset.c` and `intset.source` files.  
In order for the `Makefile` to work properly, you must use the identifier
`_OBJWD_` in the `intset.source` file to refer to the directory holding the
compiled library. You should never directly modify the `intset.sql` file
produced by the `Makefile`.

If you want to use other `*.c` files along with `intset.c`, then you can do so,
but you will need to make further changes to the `Makefile` to ensure that they
are compiled and linked correctly into the library.

Note that your submitted versions of `intset.c` and `intset.source` should not
contain any references to the `complex` type (because that's not what you're
implementing). Make sure that the comments in the program describes the code
that _you_ wrote.

Also, _do not_ put testing queries in your intset.source; all it should do is
create the new data type. Put any testing you want to do in a separate \*.sql,
which you don't need to submit. And _do not_ drop the intSet type at the end of
intset.source. If you do, your data type will vanish before we have a chance to
test it.

The intSet Data Type
--------------------

We aim to define a new base type `intSet`, to store the notion of sets of
integer values. We also aim to define a useful collection of operations on the
`intSet` type.

How you represent `intSet` values, and implement functions to manipulate them,
is up to you. However, they must satisfy the requirements below

Once implemented correctly, you should be able to use your PostgreSQL server to
build the following kind of SQL applications:

``` sql
create table Features (
    id integer primary key,
    name text
);

create table DBSystems (
    name text primary key,
    features intSet
);

insert into Features (id, name) values
    (1, 'well designed'),
    (2, 'efficient'),
    (3, 'flexible'),
    (4, 'robust');

insert into DBSystems (name, features) values
    ('MySQL', '{}'),
    ('MongoDB', '{}'),
    ('Oracle', '{2,4}'),
    ('PostgreSQL', '{1,2,3,4}');

```

### intSet values

In mathematics, we represent a set as a curly-bracketed, comma-separated
collection of values: \(\{ 1, 2, 3, 4, 5 \}\) Such a set contains only
distinct values, and no particular ordering can be imposed.

Our `intSet` values can be represented similarly. We can have a comma-separated
list of non-negative integers, surrounded by a set of curly braces, which is
presented to and by PostgreSQL as a string. For example:


`'{ 1, 2, 3, 4, 5 }'`.

Whitespace should not matter, so `'{1,2,3}'` and `'{ 1, 2, 3 }'` are equivalent.
Similarly, a set contains distinct values, so `'{1,1,1,1,1}'` is equivalent to
`'{1}'`. And ordering is irrelevant, so `'{1,2,3}'` is equivalent to
`'{3,2,1}'`.

The integer values in the set are assumed to consist of a sequence of digits.
There are no + or \- signs. There can be leading zeroes, but they should
effectively be ignored, e.g. 0001 should be treated the same as 1.

You **may not assume a fixed limit** to the size of the set. It may contain zero
or more elements, bounded by the database's capacity to store values.

You **may assume** that each interger value will be less than INT\_MAX. ie. each
element in the set will be less than \(2^{31}-1\).

#### Valid intSets

``` sql

'{ }'
'{2,3,1}'
'{6,6,6,6,6,6}'
'{10, 9, 8, 7, 6,5,4,3,2,1}'
'{1, 999, 13, 666, 5}'
'{    1  ,  3  ,  5 , 7,9 }'
'{1, 01, 001, 0001}'  (same as '{1}')
'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}'
'{1,2,3,4,5,6,7,8,9,10,(and then all numbers to),9999,10000}'
'  {1,5,7,9}'
'{2,4,6, 8  }  '
'{0}' 
```

#### Invalid intSets

``` sql
'{a,b,c}'
'{ a, b, c }'
'{1, 2.0, 3}'
'{1, {2,3}, 4}'
'{1, 2, 3, 4, five}'
'{ 1 2 3 4 }'
'1 2 3 4'
'1 2 3}'
'{ -1 }'
'{1,2,3'
'1,2,3,4,5'
'{{1,2,3,5}'
'{7,17,,27,37}'
'{1,2,3,5,8,}' 

```

### Operations on intSets

You must implement **all** of the following operations for the `intSet` type.
(assuming \(A\), \(B\), and \(S\) are `intSet`s, and \(i\) is an
integer):

`i ? S`

intSet \(S\) contains the integer \(i\); That is, \(i \in S\).

`# S`

Give the _cardinality_, or number of distinct elements in, intSet \(S\); That
is, \(|S|\).

`A >@ B`

Does intSet \(A\) contain all the values in intSet \(B\)? ie, for every
element of \(B\), is it an element of \(A\)? That is, the improper superset
(\(A \supseteq B\))

`A @< B`

Does intSet \(A\) contain only values in intSet \(B\)? ie, for every element
of \(A\), is it an element of \(B\)? That is, the improper subset (\(A
\subseteq B\))

`A = B`

intSets \(A\) and \(B\) are equal; That is, intSet \(A\) contains all the
values of intSet \(B\) and intSet \(B\) contains all the values of intSet
\(A\); That is, every element in \(A\) can be found in \(B\), and vice
versa.

`A <> B`

intSets \(A\) and \(B\) are not equal; That is, intSet \(A\) doesn't
contain all the values of intSet \(B\) or intSet \(B\) doesn't contain all
the values of intSet \(A\); That is, some element in \(A\) cannot be found
in \(B\), or vice versa.

`A && B`

Takes the _set intersection_, and produces an intSet containing the elements
common to \(A\) and \(B\); That is, \(A \cap B\).

`A || B`

Takes the _set union_, and produces an intSet containing all the elements of
\(A\) and \(B\); That is, \(A \cup B\).

`A !! B`

Takes the _set disjunction_, and produces an intSet containing elements that are
in \(A\) and not in \(B\), or that are in \(B\) and not in \(A\).

`A - B`

Takes the _set difference_, and produces an intSet containing elements that are
in \(A\) and not in \(B\). Note that this is _not_ the same as `A !! B`.

Below are examples of how you might use `intSet`s, to illustrate the semantics.
You can use these as an initial test set; we will supply a more comprehensive
test suite later.

``` sql 

create table mySets (id integer primary key, iset intSet);
CREATE TABLE
insert into mySets values (1, '{1,2,3}');
INSERT 0 1
insert into mySets values (2, '{1,3,1,3,1}');
INSERT 0 1
insert into mySets values (3, '{3,4,5}');
INSERT 0 1
insert into mySets values (4, '{4,5}');
INSERT 0 1
select * from mySets order by id;
 id |  iset
----|---------
  1 | {1,2,3}
  2 | {1,3}
  3 | {3,4,5}
  4 | {4,5}
(4 rows)
-- get all pairs of tuples where the second iset is a subset of first iset
select a.*, b.* from mySets a, mySets b
where (b.iset @< a.iset) and a.id != b.id;
 id |  iset   | id | iset
----|---------|----|-------
  1 | {1,2,3} |  2 | {1,3}
  3 | {3,4,5} |  4 | {4,5}
(2 rows)
-- insert extra values into the iset in tuple #4 via union
update mySets set iset = iset || '{5,6,7,8}' where id = 4;
UPDATE 1
select * from mySets where id=4;
 id |    iset
----|-------------
  4 | {4,5,6,7,8}
(1 row)
-- tuple #4 is no longer a subset of tuple #3
select a.*, b.* from mySets a, mySets b
where (b.iset @< a.iset) and a.id != b.id;
 id |  iset   | id | iset
----|---------|----|-------
  1 | {1,2,3} |  2 | {1,3}
(1 row)
-- get the cardinality (size) of each intSet
select id, iset, (#iset) as card from mySets order by id;
 id |    iset     | card
----|-------------|------
  1 | {1,2,3}     |    3
  2 | {1,3}       |    2
  3 | {3,4,5}     |    3
  4 | {4,5,6,7,8} |    5
(4 rows)
-- form the intersection of each pair of sets
select a.iset, b.iset, a.iset && b.iset
from mySets a, mySets b where a.id < b.id;
  iset   |    iset     | ?column?
---------|-------------|----------
 {1,2,3} | {1,3}       | {1,3}
 {1,2,3} | {3,4,5}     | {3}
 {1,2,3} | {4,5,6,7,8} | {}
 {1,3}   | {3,4,5}     | {3}
 {1,3}   | {4,5,6,7,8} | {}
 {3,4,5} | {4,5,6,7,8} | {4,5}
(6 rows)
delete from mySets where iset @< '{1,2,3,4,5,6}';
DELETE 3
select * from mySets;
 id |    iset
----|-------------
  4 | {4,5,6,7,8}
(1 row)
-- etc. etc. etc.

```

You should think of some more tests of your own. In particular, make sure that
you check that your code works with large `intSet`s (e.g. cardinality ≥ 1000).
If you come up with any tests that you think are particularly clever, feel free
to post them in the comments section below.

### Representing intSets

The first thing you need to do is to decide on an internal representation for
your `intSet` data type. You should do this _after_ you have understood the
description of the operators above. Since what they require may affect how you
decide on the representation of your `intSet` values.

Note that because of the requirement that an `intSet` can be arbitrarily large
(see above), you **cannot** have a representation that uses a fixed-size object
to hold values of type `intSet`.

When you read strings representing `intSet` values, they are converted into your
internal form, stored in the database in this form, and operations on `intSet`
values are carried out using this data structure. When you display `intSet`
values, you should show them in a canonical form, regardless of how they were
entered or how they are stored. The canonical form for output (at least) should
include no spaces, and should have elements in ascending order.

The first functions you need to write are ones to read and display values of
type `intSet`. You should write analogues of the functions `complex_in()` and
`complex_out()` that are defined in the file `complex.c`. Suitable names for
these functions would be e.g. `intset_in()` and `intset_out()`. Make sure that
you use the `V1` style function interface (as is done in `complex.c`).

Note that the two input/output functions should be complementary, meaning that
any string displayed by the output function must be able to be read using the
input function. There is no requirement for you to retain the precise string
that was used for input (e.g. you could store the `intSet` value internally in
canonical form).

Note that you are _not_ required to define binary input/output functions called
`receive_function` and `send_function` in the PostgreSQL documentation, and
called `complex_send()` and `complex_recv()` in the `complex.c` file.

**Hint:** test out as many of your C functions as you can _outside_ PostgreSQL
(e.g., write a simple test driver) Before you try to install them in PostgreSQL.
This will make debugging much easier.

You should ensure that your definitions _capture the full semantics of the operators_ 
(e.g. specify commutativity if the operator is commutative).

Submission
----------

You need to submit two files: 

`intset.c` - containing the C functions that implement the internals of the `intSet` data type. 

`intset.source` - containing the template SQL commands to install the `intSet` data type into a PostgreSQL server.

Do _not_ submit the `intset.sql` file, since it contains absolute file names
which are not useful in our test environment. 

_Do not_ include:

*   `create table ...`
*   `insert into ...`
*   `select ...`
*   `drop type ...`

Or any other statements not directly needed for creating the `intSet` data type
in `intset.source`.


Marking
---------

The decomposition of the marks of this assignment is as follows,

- input output 
    - valid: 25 (handle valid input)
    - invalid: 5 (report error on invalid input)
- operators
    - contain: 5
    - cardinality: 5
    - subset superset: 10
    - equality: 10
    - intersection: 10
    - union: 10
    - disjunction: 10
    - difference: 10
- (Total: **100**)

**Note**: In the feedback system, the full mark of this task is `100` but your marks will be scaled down when added to the total marks of this course.