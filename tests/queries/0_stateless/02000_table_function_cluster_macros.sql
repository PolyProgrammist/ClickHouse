CREATE TABLE test_db_table_function_cluster_macros ON CLUSTER "{default_cluster}" (date DateTime, value UInt32) engine=MergeTree() ORDER BY date
SELECT * FROM cluster("{default_cluster}", default.test_db_table_function_cluster_macros)
SELECT * FROM clusterAllReplicas("{default_cluster}", default.test_db_table_function_cluster_macros)
