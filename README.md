# top100Url

## 问题复述
给一个100g的以换行符分割的url文件，在1g内存下寻找top100的url
## 思路
整体思路还是很自然的，就是将100g文件hash成可放入内存的小文件，然后对每一个求map再合并到一个堆里。
## 优化方向
主要的瓶颈在IO和多线程切换。
## 具体实现
### init url
1. 先生成100个带后缀ans的作为答案，方便判断结果是否正确。
2. 再为每一个线程，分配一个IO缓冲区（0.8G/threadNum）
3. 每一个线程都会向各自的缓冲区写url，缓冲区满后向文件写数据。其中，url生成策略是，以时间ms作为随机源，如果mod MASK的结果大于100则生成随机url，否则生成对应序号ans。
~~这里用了一个小技巧，用linux命令预先生成一个块文件，避免了每个线程生成小文件再复制的IO开销。~~实验证明直接以append模式打开，并且通过lseek即可达到效果。

### stream hash
1. IO读写，考虑到linux内核对大文件IO优化较好，所以只在主线程进行读写。
2. 缓冲区分配，考虑到读写和处理的平衡，将一半的内存作为读缓冲区，将四分之一的内存作为每个线程的local写缓冲区。
3. 主线程和子线程通信，在分配缓冲区后，用一个指针数组和一个判断是否已读文件的bool数组，以及一个标识本次读的唯一序号的数组进行维护，将三者封装成一个tuple加入线程待处理的队列。处理完成后将对应的序号压入另一个队列。
4. 为了加速读写，必不可少的按块去加载数据，但是这样由于本问题的url并不知道长度，所以需要对每次切分的头尾进行额外判断。
5. 考虑到很多时候由于hash的分布和文件块大小的原因，导致每次local缓冲区向文件写的时候只有一两个url造成了额外的开销，所以设置了一个全局缓冲区，如果local向文件写的数目低于阈值，则向全局缓冲区的对应部分进行写入。等到全局缓冲区对应部分满了之后再向文件写。
6. 综合hash碰撞和速度的选择，采用的是redis在用的murmur3的hash算法。
7. 在存储的时候为了下一步的方便，采取按块存储，多余的用0补齐。

### stream heap
1. 这部分思路相对简单，采取一样的双队列加缓冲区策略，然后遍历取出数据，存入以const char * 为key的map，读完再向一个大小为100的优先队列压入数据。如此反复即可。区别是只有一个处理线程和一个读线程。
2. 这里最开始考虑过是否要把所有的hash文件连接在一起，然后按块加载，按offset进行读取map，但是经过实验在固态硬盘IO的用时远远过于map的部分，于是放弃。
3. 结果存在ans.txt，然后和第一步生成的raw ans进行对比，如果完全一致输出accept。

## 改进
1. 考虑过是否采用folly的atomichashmap，对于每个加载的文件多线程进行插入，效果应该比目前的更好。但是时间所限还未进行这个改进。
2. 对于各个缓冲区内存分配和对齐，并未经过多次实验达到一个最优的状态。
3. 因为每次为了观察整个时间开销，就没有单独写单元测试和日志输出，这一点也需要改善。
4. 缓冲区配置和一些参数是写在代码内部而没有采取命令行参数或者json/ini文件的形式。

## 测试

### 测试一

> commit 94a31114e0ffe82c058847dd6cb8ead546aba862

#### 测试环境

- i7-8700 (12 threads)
- DDR4 16g (ulimit -v 1048576)
- 960evo256g (每次重新生成文件防止cache加速，关闭SLC cache模拟)
由于硬盘限制，没有进行100g的完整测试，最高只进行了10g的测试


#### 输出
```
dd if=/dev/zero of=text bs=71582788 count=151
记录了151+0 的读入
记录了151+0 的写出
10809000988 bytes (11 GB, 10 GiB) copied, 27.0752 s, 399 MB/s
Generate url: 335544324
Cost: 78.6359

Open thread num: 12
Hash grid done
Cost: 74.044

Get ans in ans.txt
Accept
Cost: 24.1211
```

#### 测试分析
从dd命令的结果可以看到，当前硬盘的连续写速度是399MB/s。

第一步操作需要预写，生成url和分块写，写两次，平均速度2*10*1024/78.6=260.56MB/s

第二步操作需要连续读，计算url的hash和分文件写，读写各一次，平均速度2*10*1024/74.044=276.6MB/s

第三步操作需要分文件读，读一次，平均速度10*1024/24.1211=424.5245MB/s


### 测试二

> commit ed8ddb13e1e546f6937751b481547b25f71b0e24

测试一的预分配文件并没有带来相应的提速，反而因为预分配文件和append的冲突带来额外的开销，移除预分配的测试如下

#### 测试环境

- i7-8700 (12 threads)
- DDR4 16g (ulimit -v 1048576)
- 960evo256g (每次重新生成文件防止cache加速，关闭SLC cache模拟)
由于硬盘限制，没有进行100g的完整测试，最高只进行了10g的测试

#### 输出

```
Generate url: 335544324

Cost: 44.7508

Open thread num: 12

Hash grid done

Cost: 60.9693

Get ans in ans.txt

Accept

Cost: 21.584
```


### 测试三

> commit 8b13d15c7f73c375934722b46ef130cef3c86644

调整了url生成的时候的hash，让每个文件的大小接近，并且尝试了20g的测试。

可以看到因为更加平均的hash，后期生成堆变成了瓶颈。

#### 输出

##### 10g
```
Generate url: 335544324
Cost: 59.0388

Open thread num: 12
Load done
Hash grid done
Write grid done
To hash url : 335544654
Cost: 57.1418

Get ans in ans.txt
Accept
Cost: 62.5969
```
##### 20g
```
Generate url: 671088648
Cost: 104.767

Open thread num: 12
Load done
Hash grid done
Write grid done
To hash url : 671089307
Cost: 123.099

Get ans in ans.txt
Accept
Cost: 142.324
```
### 测试四

> commit  92557616bdb88b2d2eeac9a41267a5eeb596f04c

尝试加入tcmalloc，发现速度反倒变慢;

采用google的sparsemap速度近似于c++11的unordered map，采用densemap速度优化明显，而且内存开销没有想象中的大。

调整了堆的策略。

#### 测试数据
```
Generate url: 335544324
Cost: 53.1305

Open thread num: 12
Load done
Hash grid done
Write grid done
To hash url : 335544588
Cost: 66.9775

Get ans in ans.txt
Accept
Cost: 48.7276
```
### 测试五

>  commit 2f52306ce2391041a01de94a832e0fa869dd8f76

将streamheap修改为类似前面两个的双缓冲区，有一定成效。尝试移除了忙等待，改为信号量，并没有明显改善。

#### 测试数据
```
Start init url file
Generate url: 335544324
Cost: 52.1674

Start stream to hash
Open thread num: 12
Load done
Hash grid done
Write grid done
To hash url : 335544324
Cost: 64.4286

Start stream to heap
Get ans in ans.txt
Accept
Cost: 34.2047
```


### 测试六

> commit e0584ece62377ad5b304d2843c6ea873adae65cf

找了一块2t的希捷机械用来测试100g规模的数据。可以看到，第一步因为是向连续大文件去写，所以和用固态硬盘的速度基本没差距，说明瓶颈在处理器。第二步和第三步，因为涉及到对多个文件的读写，磁盘寻道带来了额外的开销，有必要为机械硬盘的采取，将第二步的文件移动成一个大文件再处理的版本。

```
./topUrl -i text -o ans.txt -t temp -n 100  -m 7 -p 300
```

#### 100g
```
Start init url file
Generate url: 3355443204
Cost: 639.205

Start stream to hash
Open thread num: 12
Load done 100.00% 
Hash grid done
Write grid done
To hash url : 3355443204
Cost: 1688.54

Start stream to heap
Get ans in ans.txt
Accept
Cost: 714.784

Max memory : 964 MB
```