#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>

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

class Unfreezer {
public:
    PartitionCommandsResultInfo unfreezePartitionsFromTableDirectory(MergeTreeData::MatcherFn matcher, const String & backup_name, Disks disks, fs::path table_directory, Poco::Logger * log, ContextPtr local_context);

private:
    bool removeSharedDetachedPart(DiskPtr disk, const String & path, const String & part_name, const String & table_uuid,
        const String &, const String & detached_replica_name, const String & detached_zookeeper_path, Poco::Logger * log, ContextPtr local_context);

    bool removeDetachedPart(DiskPtr disk, const String & path, const String &part_name, bool, Poco::Logger * log, ContextPtr local_context);
}

}
