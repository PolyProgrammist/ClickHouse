CREATE TABLE test_db_table_function_cluster_macros ON CLUSTER '{cluster}' (date DateTime, value UInt32) engine=MergeTree() ORDER BY date
SELECT * FROM cluster("{cluster}", default.test_db_table_function_cluster_macros);
