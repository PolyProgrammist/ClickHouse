#include <Storages/StorageReplicatedMergeTree.h>

namespace DB 
{

/// Special metadata used during freeze table. Required for zero-copy
/// replication.
struct FreezeMetaData
{
public:
    void fill(const StorageReplicatedMergeTree & storage);

    void save(DiskPtr data_disk, const String & path) const;

    bool load(DiskPtr data_disk, const String & path);

    static void clean(DiskPtr data_disk, const String & path);

private:
    static String getFileName(const String & path);

public:
    int version = 1;
    bool is_replicated;
    bool is_remote;
    String replica_name;
    String zookeeper_name;
    String table_shared_id;
};

}
