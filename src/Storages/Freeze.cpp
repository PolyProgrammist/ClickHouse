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
}