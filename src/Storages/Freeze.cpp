#include <Storages/Freeze.h>

namespace DB 
{
void FreezeMetaData::fill(const StorageReplicatedMergeTree & storage)
{
    is_replicated = storage.supportsReplication();
    is_remote = storage.isRemote();
    replica_name = storage.getReplicaName();
    zookeeper_name = storage.getZooKeeperName();
    table_shared_id = storage.getTableSharedID();
}

void FreezeMetaData::save(DiskPtr data_disk, const String & path) const
{
    auto metadata_disk = data_disk->getMetadataDiskIfExistsOrSelf();

    auto file_path = getFileName(path);
    auto buffer = metadata_disk->writeFile(file_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    writeIntText(version, *buffer);
    buffer->write("\n", 1);
    writeBoolText(is_replicated, *buffer);
    buffer->write("\n", 1);
    writeBoolText(is_remote, *buffer);
    buffer->write("\n", 1);
    writeString(replica_name, *buffer);
    buffer->write("\n", 1);
    writeString(zookeeper_name, *buffer);
    buffer->write("\n", 1);
    writeString(table_shared_id, *buffer);
    buffer->write("\n", 1);
}

bool FreezeMetaData::load(DiskPtr data_disk, const String & path)
{
    auto metadata_disk = data_disk->getMetadataDiskIfExistsOrSelf();
    auto file_path = getFileName(path);

    if (!metadata_disk->exists(file_path))
        return false;
    auto buffer = metadata_disk->readFile(file_path, ReadSettings(), {});
    readIntText(version, *buffer);
    if (version != 1)
    {
        LOG_ERROR(&Poco::Logger::get("FreezeMetaData"), "Unknown freezed metadata version: {}", version);
        return false;
    }
    DB::assertChar('\n', *buffer);
    readBoolText(is_replicated, *buffer);
    DB::assertChar('\n', *buffer);
    readBoolText(is_remote, *buffer);
    DB::assertChar('\n', *buffer);
    readString(replica_name, *buffer);
    DB::assertChar('\n', *buffer);
    readString(zookeeper_name, *buffer);
    DB::assertChar('\n', *buffer);
    readString(table_shared_id, *buffer);
    DB::assertChar('\n', *buffer);
    return true;
}

void FreezeMetaData::clean(DiskPtr data_disk, const String & path)
{
    auto metadata_disk = data_disk->getMetadataDiskIfExistsOrSelf();
    metadata_disk->removeFileIfExists(getFileName(path));
}

String FreezeMetaData::getFileName(const String & path)
{
    return fs::path(path) / "frozen_metadata.txt";
}

bool Unfreezer::removeSharedDetachedPart(DiskPtr disk, const String & path, const String & part_name, const String & table_uuid,
        const String &, const String & detached_replica_name, const String & detached_zookeeper_path, ContextPtr local_context)
{
    bool keep_shared = false;

    zkutil::ZooKeeperPtr zookeeper = local_context->getZooKeeper();

    fs::path checksums = fs::path(path) / IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK;
    if (disk->exists(checksums))
    {
        if (disk->getRefCount(checksums) == 0)
        {
            String id = disk->getUniqueId(checksums);
            keep_shared = !StorageReplicatedMergeTree::unlockSharedDataByID(id, table_uuid, part_name,
                detached_replica_name, disk, zookeeper, local_context->getReplicatedMergeTreeSettings(), &Poco::Logger::get("Unfreezer"),
                detached_zookeeper_path);
        }
        else
            keep_shared = true;
    }

    disk->removeSharedRecursive(path, keep_shared);

    return keep_shared;
}

bool Unfreezer::removeDetachedPart(DiskPtr disk, const String & path, const String &part_name, bool, ContextPtr local_context)
{
    if (disk->supportZeroCopyReplication())
    {
        FreezeMetaData meta;
        if (meta.load(disk, path))
        {
            if (meta.is_replicated) {
                FreezeMetaData::clean(disk, path);
                return removeSharedDetachedPart(disk, path, part_name, meta.table_shared_id, meta.zookeeper_name, meta.replica_name, "", local_context);
            }
        }
    }

    disk->removeRecursive(path);

    return false;
}

PartitionCommandsResultInfo Unfreezer::unfreezePartitionsFromTableDirectory(MergeTreeData::MatcherFn matcher, const String & backup_name, Disks disks, fs::path table_directory, ContextPtr local_context) {
    PartitionCommandsResultInfo result;

    for (const auto & disk : disks)
    {
        if (!disk->exists(table_directory))
            continue;

        for (auto it = disk->iterateDirectory(table_directory); it->isValid(); it->next())
        {
            const auto & partition_directory = it->name();

            /// Partition ID is prefix of part directory name: <partition id>_<rest of part directory name>
            auto found = partition_directory.find('_');
            if (found == std::string::npos)
                continue;
            auto partition_id = partition_directory.substr(0, found);

            if (!matcher(partition_id))
                continue;

            const auto & path = it->path();

            bool keep_shared = removeDetachedPart(disk, path, partition_directory, true, local_context);

            result.push_back(PartitionCommandResultInfo{
                .partition_id = partition_id,
                .part_name = partition_directory,
                .backup_path = disk->getPath() + table_directory.generic_string(),
                .part_backup_path = disk->getPath() + path,
                .backup_name = backup_name,
            });

            LOG_DEBUG(&Poco::Logger::get("Unfreezer"), "Unfreezed part by path {}, keep shared data: {}", disk->getPath() + path, keep_shared);
        }
    }

    LOG_DEBUG(&Poco::Logger::get("Unfreezer"), "Unfreezed {} parts", result.size());

    return result;
}
}
