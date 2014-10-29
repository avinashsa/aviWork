--declaration
CREATE OR REPLACE PACKAGE monitor_partition_sort AS
 TYPE partition_tmp_table_type IS RECORD (
    table_name VARCHAR2(50)
  );

  TYPE partition_tmp_tables IS TABLE OF partition_tmp_table_type ;

 FUNCTION did_partition_sort_fail RETURN VARCHAR2;
 FUNCTION get_tmp_partition_tables RETURN partition_tmp_tables PIPELINED ;
END monitor_partition_sort;
/
--Definition
CREATE OR REPLACE PACKAGE BODY monitor_partition_sort AS

FUNCTION get_tmp_partition_tables
RETURN  partition_tmp_tables 
PIPELINED IS 
tmp_tables partition_tmp_table_type ;
CURSOR table_csr IS 
   SELECT TABLE_NAME
   FROM ALL_TABLES
      WHERE OWNER = 'OPS$SVWPRDB'
         AND ( TABLE_NAME LIKE 'ZCHGTMP%' OR TABLE_NAME LIKE 'ZNORM%' );
BEGIN
   FOR tables IN table_csr
    LOOP 
      tmp_tables.table_name := tables.TABLE_NAME ;
      pipe row(tmp_tables);
    END LOOP;
END get_tmp_partition_tables;

FUNCTION did_partition_sort_fail 
RETURN VARCHAR2 is answer VARCHAR2(5);
BEGIN
  SELECT DECODE(COUNT_A, 0, 'FALSE','TRUE') INTO answer 
   FROM
   (
     SELECT COUNT(*) COUNT_A 
       FROM ALL_TABLES
      WHERE OWNER = 'OPS$SVWPRDB'
         AND ( TABLE_NAME LIKE 'ZCHGTMP%' OR TABLE_NAME LIKE 'ZNORM%' )
   );
  RETURN answer; 
END did_partition_sort_fail;

END monitor_partition_sort;
/
