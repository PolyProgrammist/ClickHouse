ATTACH TABLE _ UUID 'e1cb0213-bcca-409f-b3dc-9aa10d803a43'
(
    `hehe` String
)
ENGINE = MergeTree
ORDER BY hehe
SETTINGS index_granularity = 8192
