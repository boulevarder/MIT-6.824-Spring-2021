## 遇到的一些问题(曲线救国的方法)

### lab 2A
- Q: 当candidate获得大部分选票应立即向所有follower发送心跳?
- A: 这里有两个事件: 1) 选举超时, 2) 获得大部分选票, 这两个发生一个都要立即唤醒
    - 由于不熟悉go语言, 第一开始选择让主线程睡眠election time, 子线程发送*RequestVote RPC*, 当发现得到大部分选票时, 立即变为leader, 发送一次心跳, 而之后的心跳还要等主线程超时(只有当主线程苏醒时才能完成定期发送心跳) -> 这样的做法导致: 如果网络不好丢包, 会有其他线程超时参加选举(*Test (2A): initial election ...*测试用例出现*warning: term changed even though there were no failures*)
    - 把选举超时作为一个事件, 开启一个线程去等待超时, 当发生以上两个事件之一时, 唤醒主线程发送心跳