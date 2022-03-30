摘要
-------

**注意** : 本实验需要投入一些时间，请尽早开始工作。

* 截止日期: **2021-4-30   23:59**

* 分数: 在课程中占 **15 分** 。

* 迟交惩罚: 每小时扣除 _0.089 分_。迟交七天本实验分数登记为 _0 分_。

实验说明
------------

- 该文本包含 **实验 1** 的实验说明。

- 需要在 `intset.c` 和 `intset.source`文件中进行代码实现。

- 不允许在屏幕或日志文件中打印不需要的信息。考核中不会考虑类似的输出。

- 需要将你的实现提交到对应链接: https://szudseg.cn/submit/

- 我们提供每一部分的详细说明。如果有更多的问题，可以在微信群中进行提问。

- 仅允许使用标准库的函数，不允许使用第三方库。

- 对提交的文件我们会 **快速反馈** 。你可以在线上提交平台阅读反馈信息。

- 允许 **不限次数** 的提交, 我们会使用 **最后** 提交的文件进行评估。 

- 请使用 **Postgres 12.5** 完成本实验。

实验 1：在 PostgreSQL 添加 Set 数据类型 
----------

实验目标：

*   理解DBMS如何处理数据。
*   练习在 PostgreSQL 中添加新的基础数据类型。

实现新的数据类型，并完成 input/output 函数与一系列的 operations。

请在开始工作前 _认真_ 阅读 _全部_ 实验说明。没有阅读实验说明而导致的问题都会回复“请阅读实验说明”。

在说明中我们会使用下述名词指代：

`PG_CODE`

PostgreSQL 源码所在目录  
(例如 `/srvr/$USER/postgresql-12.5`)

`PG_HOME`

PostgreSQL 二进制文件安装目录  
(例如 `/srvr/$USER/pgsql`)

`PG_DATA`

存放 PostgreSQL's `data` 的目录
(例如 `/srvr/$USER/pgsql/data`)

`PG_LOG`

PostgreSQL 日志输出文件 
(例如 `/srvr/$USER/pgsql/log`)

引言
------------

PostgreSQL 有一个可扩展性模型，它提供一个能将新数据类型添加到 PostgreSQL 服务器的定义明确的过程。这能力允许 PostgreSQL 用户开发多种类型（例如多边形），且这些数据类型已成为标准版本的一部分。这也意味着 PostgreSQL 是研究突破 DBMS 管理的数据类型界限的相关项目的首选数据库。

在这个作业中，我们将添加一个新的数据类型来处理 **整数集合**。你可以以任何方式实现数据类型的功能，_前提是_ 它们满足下面给出的语义。（在 [intSets](#the-intset-data-type) 章节）

注意到 PostgreSQL 中的数组有一些属性和操作，使它们看起来有点像集合。但是，它们不是集合，并且与我们要求实现的数据类型具有完全不同的语义。

PostgreSQL 文档的以下部分描述了在 PostgreSQL 中添加新的基本数据类型的过程：

*   [37.10 C-Language Functions](https://www.postgresql.org/docs/12//xfunc-c.html)
*   [37.13 User-defined Types](https://www.postgresql.org/docs/12//xtypes.html)
*   [37.14 User-defined Operators](https://www.postgresql.org/docs/12//xoper.html)
*   [SQL: CREATE TYPE](https://www.postgresql.org/docs/12//sql-createtype.html)
*   [SQL: CREATE OPERATOR](https://www.postgresql.org/docs/12//sql-createoperator.html)
*   [SQL: CREATE OPERATOR CLASS](https://www.postgresql.org/docs/12//sql-createopclass.html)

第 37.13 节使用复数类型的示例，你可以将其用作定义 `intSet` 数据类型的起点（见下文）。 请注意，复数类型 _只是_ 一个起点，以了解如何添加新数据类型。不要误以为这个实验只需要将名称 complex 更改为 intSet；intSet 类型比复数类型更复杂。

在 tutorial 和 contrib 目录下还有其他新数据类型的示例。 这些可能会给你一些关于如何实现 `intSet` 数据类型的有用想法：

*   An auto-encrypted password datatype  
    `PG_CODE/contrib/chkpass/`
*   A case-insensitive character string datatype  
    `PG_CODE/contrib/citext/`
*   A confidence-interval datatype  
    `PG_CODE/contrib/seg/`

配置
----------

你应该从一个新的 PostgreSQL 副本开始这个作业，不要对 Prac 练习做任何更改（除非这些更改是无效的）。请注意，你只需为此配置、编译和安装一次 PostgreSQL 服务器。所有后续编译都在 `src/tutorial` 目录中进行，并且只需要修改那里的文件。

一旦重新安装了 PostgreSQL 服务器，你应该运行以下命令：
``` console
cd PG\_CODE/src/tutorial
cp complex.c intset.c
cp complex.source intset.source
```

一旦你制作了 `intset` 文件，你还应该编辑这个目录中的 `Makefile`，并将文本添加到以下行：
``` console
MODULES = complex funcs intset
DATA_built = advanced.sql basics.sql complex.sql funcs.sql syscat.sql intset.sql
```

该作业的其余工作涉及编辑 `intset.c` 和 `intset.source` 文件。
为了使 `Makefile` 正常工作，你必须使用 `intset.source` 文件中的标识符 `_OBJWD_` 来引用保存已编译库的目录。不要直接修改由 `Makefile` 生成的 `intset.sql` 文件。

你可以使用其他 `*.c` 文件和 `intset.c`，但你需要对 `Makefile` 做进一步的修改，以确保它们被编译并正确链接到编译库。

请注意，提交的 `intset.c` 和 `intset.source` 版本不应包含对 `complex` 类型的任何引用（因为这不是需要实现的）。请确保使用注释介绍 _自己_ 编写的代码。

此外，_不要_ 将测试查询放在 intset.source 中；它应该做的是创建新的数据类型。将你想要执行的任何测试放在一个单独的 \*.sql 中，无需提交。并且 _不要_ 将 intSet 类型放在 intset.source 的末尾。如果这样做，新的数据类型将在我们有机会测试之前消失。

intSet 数据类型
--------------------

我们的目标是定义一个新的基本类型“intSet”，来存储整数值集合的概念。我们还打算在 `intSet` 类型上定义一些有用的操作。

如何表示 `intSet` 值并实现操作它们的函数取决于你。但是，它们必须满足以下要求。

一旦正确实施，你应该能够使用 PostgreSQL 服务器来构建以下类型的 SQL 应用程序：

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

在数学中，我们将集合表示为花括号、逗号分隔的值集合：\(\{ 1, 2, 3, 4, 5 \}\) 这样的集合只包含不同的值，并且没有指定特定的顺序。

我们的 `intSet` 值可以类似地表示。 我们可以有一个逗号分隔的非负整数列表，由一组花括号包围，它以字符串的形式提供给 PostgreSQL 或由 PostgreSQL 展示。例如

`'{ 1, 2, 3, 4, 5 }'`.

空格无关紧要，所以 ''{1,2,3}'' 和 ''{ 1, 2, 3 }'' 是等价的。同样，一个集合包含不同的值，所以 ''{1,1,1,1,1}'' 等价于 ''{1}''。并且排序是无关紧要的，所以`'{1,2,3}'`等价于`'{3,2,1}'`。

假设集合中的整数值由一系列数字组成。没有 + 或 \- 符号。 可以有前置的零，但应该有效地忽略它们，例如 0001 应该和 1 一样对待。

你**可能不会假设集合的大小有固定限制**。它可能包含零个或者更多元素，这受数据库存储值的能力限制。

你**可以假设**每个整数值都小于 INT_MAX\(2^{31}-1\)。

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

你必须为 `intSet` 类型实现以下**所有**的操作。(假设 \(A\)、\(B\) 和 \(S\) 是 `intSet`，\(i\) 是整数)：

`i ? S`

\(S\)包含整数\(i\)。\(i \in S\)

`# S`

返回 _基数(cardinality)_，即\(S\)中不同元素的数量。\(|S|\)

`A >@ B`

\(A\) 是否包含 _所有_ \(B\)中的值？即\(B\)的每个元素是否也是\(A\)的元素？\(A \supseteq B\)

`A @< B`

\(A\)是否 _只_ 包含\(B\)中的值？即\(A\)的每个元素是否也是\(B\)的元素？\(A \subseteq B\)

`A = B`


\(A\)与\(B\)相等；\(A\)包含所有\(B\)中的值并且\(B\)包含所有\(A\)中的值；即，每个\(A\)中的元素都能在\(B\) 中找到，反之亦然。

`A <> B`

\(A\)与\(B\)不相等；\(A\)不包含所有\(B\)中的值或者\(B\)不包含所有\(A\)中的值；即有些\(A\)中的元素无法在 \(B\)中找到，反之亦然。

`A && B`

\(A\)与\(B\)的intersection，产生一个包含\(A\)与\(B\)共同元素的集合。\(A \cap B\)

`A || B`

\(A\)与\(B\)的union，产生一个包含\(A\)与\(B\)所有元素的集合。\(A \cup B\)

`A !! B`

\(A\)与\(B\)的disjunction，产生一个包含所有满足 在\(A\)中不在\(B\)中或在\(B\)中不在\(A\)中 的元素的集合。

`A - B`

\(A\)与\(B\)的 difference，产生一个包含所有 在\(A\)中不在\(B\)中的元素 的集合。注意与 `A !! B` _不同_。

下面是如何使用 `intSet` 来说明语义的示例。
你可以将它们用作初始测试集；稍后我们将提供更全面的测试方法。

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

你应该尝试更多测试样例。特别是，请确保检查你的代码是否适用于大型 `intSet`（例如，基数 ≥ 1000）。
如果你提出任何你认为很有价值的测试，请随时将它们发布在下面的评论部分。

### Representing intSets

你需要做的第一件事是确定 `intSet` 数据类型的内部表示。你应该在你理解了上述运算符的描述之后执行此操作。因为它们的要求可能会影响你决定 `intSet` 值的表示方式。

请注意，鉴于 `intSet` 可以任意大小的要求（见上文），**不能**使用拥有固定大小对象来保存 `intSet` 类型值。

当你读取表示 `intSet` 值的字符串时，它们会被转换成内部表示，以这种表示存储在数据库中，并使用这种数据结构对 `intSet` 值进行操作。当你打印 `intSet` 值时，你应该以规范的形式打印它们，无论它们是如何输入或如何存储的。输出的规范形式（至少）不应包含空格，并且元素应按升序排列。

首先，你需要编写函数是输入和输出类型为 `intSet` 的值的函数。你应该编写与文件 `complex.c` 中定义的函数 `complex_in()` 和 `complex_out()` 的类似函数。函数名称应是 `intset_in()` 和 `intset_out()`。 确保使用 `V1` 风格的函数接口（就像 `complex.c` 中所做的那样）。

请注意，两个 输入/输出 函数应该是互补的，这意味着输出函数显示的任何字符串都必须能够使用输入函数读取。你不需要保留用于输入的精确字符串（例如，你可以在内部以规范形式存储 `intSet` 值）。

请注意，你 _不需要_ 在 PostgreSQL 文档中定义称为 `receive_function` 和 `send_function` 的二进制输入/输出函数。二进制输入/输出函数在 `complex.c` 文件中称为 `complex_send()` 和 `complex_recv()`。

**提示：** 在你尝试在 PostgreSQL 中安装它们之前，请在 PostgreSQL _之外_ 尽可能多地测试你的 C 函数（例如，编写一个简单的测试程序）。这将使调试更容易。

你应该确保你的定义 _捕获运算符的完整语义_（例如，如果运算符是可交换的，则指定可交换性）。

提交
----------

你需要提交两个文件： 

`intset.c` - 包含实现了 `intSet` 数据类型的C语言函数。 

`intset.source` - 包含安装 `intSet` 数据类型到 PostgreSQL 服务器的SQL模板。

请 _不要_ 提交 `intset.sql` 文件，因为该文件包含文件的绝对路径，不会用在我们的测试环境中。

请 _不要_ 在文件 `intset.source` 包括:

*   `create table ...`
*   `insert into ...`
*   `select ...`
*   `drop type ...`

或者其他创建 `intSet` 数据类型中不需要的语句。


评分标准
---------

本实验的评分标准如下：

- 输入/输出 
    - 有效: 25 (处理有效输入)
    - 无效: 5 (识别错误或无效输入并反馈)
- 操作符
    - contain: 5
    - cardinality: 5
    - subset superset: 10
    - equality: 10
    - intersection: 10
    - union: 10
    - disjunction: 10
    - difference: 10
- (总分: **100**)

**注意**：在反馈系统中，该实验满分为`100`，在课程总分评估中会按比例缩小。

