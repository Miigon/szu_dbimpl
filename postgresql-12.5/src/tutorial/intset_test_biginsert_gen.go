package main

import "fmt"

const N = 10000

func main() {

	fmt.Print("drop table if exists biginsert_testtable;")
	fmt.Print("create table biginsert_testtable (t intset);")
	fmt.Print("insert into biginsert_testtable values (intset('{")
	for i := 0; i < N; i++ {
		if i != 0 {
			fmt.Print(",")
		}
		fmt.Printf("%d", i)
	}
	fmt.Print("}'));")
	fmt.Printf(`do $$
	declare  
		cardi integer;
	begin
		select # t into cardi from biginsert_testtable;
		assert cardi = %d, CONCAT('Failed: Expected cardinality is %d, actual value: ', text(cardi));
		RAISE NOTICE 'Big insert test of %d passed!';
	end$$;
	drop table biginsert_testtable;
	`, N, N, N)
}
