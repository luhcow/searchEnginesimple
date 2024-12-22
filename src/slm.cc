/**
 * 切换内存表，新建一个内存表，老的暂存起来
 */
private:
void switchIndex() {
  try {
    indexLock.writeLock().lock();
    // 切换内存表
    immutableIndex = index;
    index = new TreeMap<>();
    wal.close();
    // 切换内存表后也要切换WAL
    File tmpWal = new File(dataDir + WAL_TMP);
    if (tmpWal.exists()) {
      if (!tmpWal.delete()) {
        throw new RuntimeException("删除文件失败: walTmp");
      }
    }
    if (!walFile.renameTo(tmpWal)) {
      throw new RuntimeException("重命名文件失败: walTmp");
    }
    walFile = new File(dataDir + WAL);
    wal = new RandomAccessFile(walFile, RW_MODE);
  } catch (Throwable t) {
    throw new RuntimeException(t);
  } finally {
    indexLock.writeLock().unlock();
  }
}

/**
 * 保存数据到ssTable
 */
private
void storeToSsTable() {
  try {
    // ssTable按照时间命名，这样可以保证名称递增
    SsTable ssTable = SsTable.createFromIndex(
        dataDir + System.currentTimeMillis() + TABLE,
        partSize,
        immutableIndex);
    ssTables.addFirst(ssTable);
    // 持久化完成删除暂存的内存表和WAL_TMP
    immutableIndex = null;
    File tmpWal = new File(dataDir + WAL_TMP);
    if (tmpWal.exists()) {
      if (!tmpWal.delete()) {
        throw new RuntimeException("删除文件失败: walTmp");
      }
    }
  } catch (Throwable t) {
    throw new RuntimeException(t);
  }
}