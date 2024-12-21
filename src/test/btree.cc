#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

#include "bplus.hpp"
using namespace std;
using namespace std::chrono;
using boost::asio::thread_pool;

void insertFunction(unique_ptr<BPTree<int, int>>& tree,
                    int start,
                    int end) {
  for (int i = start; i < end; ++i) {
    tree->insert(i, i);
  }
}

void findFunction(unique_ptr<BPTree<int, int>>& tree,
                  int start,
                  int end) {
  for (int i = start; i < end; ++i) {
    tree->find(i);
  }
}

int main() {
  auto tree = make_unique<BPTree<int, int>>();
  const int num_operations = 1000000;  // 插入和查找操作的总数
  const int num_threads = 4;           // 使用的线程数

  thread_pool pool(num_threads);  // 创建拥有指定线程数的线程池

  auto start = high_resolution_clock::now();

  // 并发插入
  vector<shared_future<void>> insertFutures;
  int operationsPerThread = num_operations / num_threads;
  for (int i = 0; i < num_threads; ++i) {
    int startIdx = i * operationsPerThread;
    int endIdx = (i + 1) * operationsPerThread;
    packaged_task<void()> insertTask([&, startIdx, endIdx] {
      insertFunction(tree, startIdx, endIdx);
    });
    insertFutures.push_back(insertTask.get_future());
    boost::asio::post(pool, move(insertTask));
  }

  for (auto& future : insertFutures) {
    future.wait();  // 等待所有插入操作完成
  }

  auto mid = high_resolution_clock::now();

  // 并发查找
  vector<shared_future<void>> findFutures;
  for (int i = 0; i < num_threads; ++i) {
    int startIdx = i * operationsPerThread;
    int endIdx = (i + 1) * operationsPerThread;
    packaged_task<void()> findTask([&, startIdx, endIdx] {
      findFunction(tree, startIdx, endIdx);
    });
    findFutures.push_back(findTask.get_future());
    boost::asio::post(pool, move(findTask));
  }

  for (auto& future : findFutures) {
    future.wait();  // 等待所有查找操作完成
  }

  auto end = high_resolution_clock::now();

  auto insert_duration = duration_cast<milliseconds>(mid - start);
  auto find_duration = duration_cast<milliseconds>(end - mid);
  cout << "插入" << num_operations << "个元素花费了 "
       << insert_duration.count() << " 毫秒。" << endl;
  cout << "查找" << num_operations << "个元素花费了 "
       << find_duration.count() << " 毫秒。" << endl;

  return 0;
}