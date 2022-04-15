-- ======================================
-- my implementation of the intset type.
-- Author: Miigon 2022-03-17
-- https://github.com/Miigon
-- ======================================

-- this script is used to test the correctness of `intset` type and it's operators
-- NOTE: it assumes the intset is implemented as a sorted array.


do $$ begin
    RAISE WARNING ' *********** THE FOLLOWING QUERIES SHOULD ALL SUCCEED (aka. show nothing) *********** ';
    RAISE NOTICE '';

    -- test input/output
    ASSERT text(intset(' {  } ')) = '{}', 'i/o test 1 failed';
    ASSERT text(intset('{0}')) <> '{}', 'i/o test 2 failed';
    ASSERT text(intset('{}')) <> '{0}', 'i/o test 3 failed';
    ASSERT text(intset('   {  2, 3 ,1, 4,5,0 }  ')) = '{0,1,2,3,4,5}', 'i/o test 4 failed';
    ASSERT text(intset('{2,3,1,15,1,3,2,3,15,3}')) = '{1,2,3,15}', 'i/o test 5 failed';
    ASSERT text(intset('{10,9,8,7,666666,5,4,4,3,2,11,1,111}')) = '{1,2,3,4,5,7,8,9,10,11,111,666666}', 'i/o test 6 failed';
    ASSERT text(intset('{1000}')) = '{1000}', 'i/o test 7 failed';
    ASSERT text(intset('{00000000000000000006}')) = '{6}', 'i/o test 8 failed';

    RAISE NOTICE '      passed input/output tests.';

    -- test INT_MAX
    ASSERT text(intset('{2147483647,2147483647,2147483646}')) = '{2147483646,2147483647}', 'INT_MAX test failed';
    RAISE NOTICE '      passed INT_MAX test.';

    -- test equal
    ASSERT intset('{2,1,3,5,5555,5,5,4,777,1,1,6}') = intset('{1,2,3,5555,4,5,6,777}'), 'equal test 1 failed';
    ASSERT NOT intset('{2,1,3,5,5555,5,5,4,777,1,1,6}') <> intset('{1,2,3,4,5555,5,6,777}'), 'equal test 2 failed';
    ASSERT intset('{}') = intset('{}'), 'equal test 3 failed';
    ASSERT intset('{}') <> intset('{0}'), 'equal test 4 failed';
    ASSERT intset('{0}') <> intset('{}'), 'equal test 5 failed';
    ASSERT intset('{1}') <> intset('{3}'), 'equal test 6 failed';
    ASSERT intset('{1}') = intset('{1}'), 'equal test 7 failed';
    RAISE NOTICE '      passed equal test.';
    
    -- test contain
    ASSERT 5555 ? intset('{2,1,3,5,5555,5,5,4,777,1,1,6}') , 'contain test 1 failed';
    ASSERT NOT 3333 ? intset('{2,1,3,5,5555,5,5,4,777,1,1,6}') , 'contain test 2 failed';
    ASSERT NOT 0 ? intset('{2,1,3,5,5555,5,5,4,777,1,1,6}') , 'contain test 3 failed';
    ASSERT NOT 99999999 ? intset('{2,1,3,5,5555,5,5,4,777,1,1,6,88888,10,98765,13}') , 'contain test 4 failed';
    ASSERT 88888 ? intset('{2,1,3,5,5555,5,5,4,777,1,1,6,88888,10,98765,13}') , 'contain test 5 failed';
    ASSERT 98765 ? intset('{2,1,3,5,5555,5,5,4,777,1,1,6,88888,10,98765,13}') , 'contain test 6 failed';
    RAISE NOTICE '      passed contain test.';
    
    -- test cardinality
    ASSERT # intset('{2,1,3,5,5555,5,5,4,777,1,1,6,88888,10,98765,13,2147483647}') = 13, 'cardinality test 1 failed';
    ASSERT # intset('{0,98765}') = 2, 'cardinality test 2 failed';
    ASSERT # intset('{0}') = 1, 'cardinality test 3 failed';
    ASSERT # intset('{1}') = 1, 'cardinality test 4 failed';
    ASSERT # intset('{0,0,0,0,0,0,0,0}') = 1, 'cardinality test 5 failed';
    ASSERT # intset('{1,0,1,1,1,1,1,1,11}') = 3, 'cardinality test 6 failed';
    ASSERT # intset('{2,1,3,5,5555,5,5,4,777,1,1,6,88888,10,98765,13}') = 12, 'cardinality test 7 failed';
    RAISE NOTICE '      passed cardinality test.';
    
    -- test improper_superset
    ASSERT intset('{2,1,3,5,5555,5,5,4,777,1,1,6,88888,10,98765,13,2147483647}') >@ intset('{2,5,6,4}'), 'improper_superset test 1 failed';
    ASSERT NOT intset('{2,1,3,5,5555,5,5,4,777,1,1,6,88888,10,98765,13,2147483647}') >@ intset('{2,5,6,4,9999}'), 'improper_superset test 2 failed';
    ASSERT intset('{0}') >@ intset('{0}'), 'improper_superset test 3 failed';
    ASSERT intset('{0,1,2}') >@ intset('{0}'), 'improper_superset test 4 failed';
    ASSERT intset('{0,1,2}') >@ intset('{2,1}'), 'improper_superset test 5 failed';
    ASSERT NOT intset('{0,1,3,4,10}') >@ intset('{0,1,5}'), 'improper_superset test 6 failed';
    ASSERT NOT intset('{1,3,4,10}') >@ intset('{0,1}'), 'improper_superset test 7 failed';
    ASSERT NOT intset('{1,3,4,10}') >@ intset('{4,100}'), 'improper_superset test 8 failed';
    ASSERT NOT intset('{1,3,4,10}') >@ intset('{5}'), 'improper_superset test 9 failed';
    ASSERT intset('{1,3,10,4,9999}') >@ intset('{1,9999,4,10}'), 'improper_superset test 10 failed';
    ASSERT intset('{1,3,10000,10,4,9999}') >@ intset('{1,9999,3,4,10}'), 'improper_superset test 11 failed';
    ASSERT intset('{1,3,10000,10,4,9999}') >@ intset('{}'), 'improper_superset test 12 failed';
    ASSERT intset('{}') >@ intset('{}'), 'improper_superset test 13 failed';
    ASSERT NOT intset('{}') >@ intset('{0}'), 'improper_superset test 14 failed';
    ASSERT NOT intset('{}') >@ intset('{1,6,5,4,6}'), 'improper_superset test 15 failed';
    RAISE NOTICE '      passed improper_superset test.';

    -- test improper_subset
    ASSERT intset('{2,5,6,4}') @< intset('{2,1,3,5,5555,5,5,4,777,1,1,6,88888,10,98765,13,2147483647}'), 'improper_subset test 1 failed';
    ASSERT NOT intset('{2,5,6,4,9999}') @< intset('{2,1,3,5,5555,5,5,4,777,1,1,6,88888,10,98765,13,2147483647}'), 'improper_subset test 2 failed';
    ASSERT intset('{0}') @< intset('{0}'), 'improper_subset test 3 failed';
    ASSERT intset('{0}') @< intset('{0,1,2}'), 'improper_subset test 4 failed';
    ASSERT intset('{2,1}') @< intset('{0,1,2}'), 'improper_subset test 5 failed';
    ASSERT NOT intset('{0,1,5}') @< intset('{0,1,3,4,10}'), 'improper_subset test 6 failed';
    ASSERT NOT intset('{0,1}') @< intset('{1,3,4,10}'), 'improper_subset test 7 failed';
    ASSERT NOT intset('{4,100}') @< intset('{1,3,4,10}'), 'improper_subset test 8 failed';
    ASSERT NOT intset('{5}') @< intset('{1,3,4,10}'), 'improper_subset test 9 failed';
    ASSERT intset('{1,9999,4,10}') @< intset('{1,3,10,4,9999}'), 'improper_subset test 10 failed';
    ASSERT intset('{1,9999,3,4,10}') @< intset('{1,3,10000,10,4,9999}'), 'improper_subset test 11 failed';
    ASSERT intset('{}') @< intset('{1,3,10000,10,4,9999}'), 'improper_subset test 12 failed';
    ASSERT intset('{}') @< intset('{}'), 'improper_subset test 13 failed';
    ASSERT NOT intset('{0}') @< intset('{}'), 'improper_subset test 14 failed';
    ASSERT NOT intset('{1,6,5,4,6}') @< intset('{}'), 'improper_subset test 15 failed';
    RAISE NOTICE '      passed improper_subset test.';

    -- test intersect
    ASSERT intset('{4,5,7,1}') && intset('{4,8,5,9,10}') = intset('{4,5}'), 'intersect test 1 failed';
    ASSERT intset('{0}') && intset('{}') = intset('{}'), 'intersect test 2 failed';
    ASSERT intset('{}') && intset('{}') = intset('{}'), 'intersect test 3 failed';
    ASSERT intset('{}') && intset('{0}') = intset('{}'), 'intersect test 4 failed';
    ASSERT intset('{4,5,7,1}') && intset('{9,100,299}') = intset('{}'), 'intersect test 5 failed';
    ASSERT intset('{4,5,7,1}') && intset('{4,5,7,1}') = intset('{4,5,7,1}'), 'intersect test 6 failed';
    ASSERT intset('{1,2,3,4,5,9,10,11,12}') && intset('{2,4,5,7,10,12}') && intset('{1,4,5,10,12}') = intset('{4,5,10,12}'), 'intersect test 7 failed';
    RAISE NOTICE '      passed intersect test.';

    -- test union
    ASSERT intset('{4,5,7,1}') || intset('{4,8,5,9,10}') = intset('{1,4,5,7,8,9,10}'), 'union test 1 failed';
    ASSERT intset('{0}') || intset('{}') = intset('{0}'), 'union test 2 failed';
    ASSERT intset('{}') || intset('{}') = intset('{}'), 'union test 3 failed';
    ASSERT intset('{}') || intset('{0}') = intset('{0}'), 'union test 4 failed';
    ASSERT intset('{4,5,7,1}') || intset('{9,100,299}') = intset('{4,5,7,1,9,100,299}'), 'union test 5 failed';
    ASSERT intset('{4,5,7,1}') || intset('{4,5,7,1}') = intset('{4,5,7,1}'), 'union test 6 failed';
    ASSERT intset('{4,5,7,1}') || intset('{4,8,5,9,10}') || intset('{4,5,7,1}') || intset('{4,8,5,9,10}') = intset('{1,4,5,7,8,9,10}'), 'union test 7 failed';
    ASSERT intset('{5,9}') || intset('{1,2,10}') || intset('{}') || intset('{4,5,9,11}') || intset('{2,3,12}') = intset('{1,2,3,4,5,9,10,11,12}'), 'union test 8 failed';
    RAISE NOTICE '      passed union test.';

    -- test disjunct
    ASSERT intset('{4,5,7,1}') !! intset('{4,8,5,9,10}') = intset('{1,7,8,9,10}'), 'disjunct test 1 failed';
    ASSERT intset('{0}') !! intset('{}') = intset('{0}'), 'disjunct test 2 failed';
    ASSERT intset('{}') !! intset('{}') = intset('{}'), 'disjunct test 3 failed';
    ASSERT intset('{}') !! intset('{0}') = intset('{0}'), 'disjunct test 4 failed';
    ASSERT intset('{4,5,7,1}') !! intset('{9,100,299}') = intset('{4,5,7,1,9,100,299}'), 'disjunct test 5 failed';
    ASSERT intset('{4,5,7,1}') !! intset('{4,5,7,1}') = intset('{}'), 'disjunct test 6 failed';
    ASSERT intset('{5,9}') !! intset('{1,2,10}') !! intset('{}') !! intset('{4,5,9,11}') !! intset('{2,3,12}') = intset('{1,3,4,10,11,12}'), 'union test 8 failed';
    RAISE NOTICE '      passed disjunct test.';

    -- test subtract
    ASSERT intset('{4,5,7,1}') - intset('{4,8,5,9,10}') = intset('{1,7}'), 'subtract test 1 failed';
    ASSERT intset('{0}') - intset('{}') = intset('{0}'), 'subtract test 2 failed';
    ASSERT intset('{}') - intset('{}') = intset('{}'), 'subtract test 3 failed';
    ASSERT intset('{}') - intset('{0}') = intset('{}'), 'subtract test 4 failed';
    ASSERT intset('{4,5,7,1}') - intset('{9,100,299}') = intset('{4,5,7,1}'), 'subtract test 5 failed';
    ASSERT intset('{4,5,7,1}') - intset('{4,5,7,1}') = intset('{}'), 'subtract test 6 failed';
    ASSERT intset('{1,2,3,4,5,9,10,11,12}') - intset('{4,5,7}') - intset('{10,15}') = intset('{1,2,3,9,11,12}'), 'subtract test 7 failed';
    RAISE NOTICE '      passed subtract test.';

    RAISE NOTICE '';
    RAISE NOTICE '      all good-case tests passed.';
    RAISE NOTICE '';
    RAISE NOTICE '      Note: test bad cases next, with `\i intset_test_badcase.source`.';

end; $$ LANGUAGE plpgsql;

do $$ begin
    RAISE WARNING ' *********** THE QUERIES ABOVE SHOULD ALL SUCCEED (aka. show nothing) *********** ';
end; $$ LANGUAGE plpgsql;