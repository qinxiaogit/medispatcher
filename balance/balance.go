package balance

import (
	"context"
	"encoding/json"
	"fmt"
	"medispatcher/config"
	"medispatcher/logger"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	doveclientCli "gitlab.int.jumei.com/JMArch/go-doveclient-cli"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

var balanceInst *Balance
var lock sync.Mutex

const (
	// 记录基础信息的etcd key(消息中心管理后台用到这个信息)
	baseInfoKey = "__mec.medis.info#%s"
	// 记录队列消费者信息的key的前缀
	queueConsumerKeyPrefix = "__mec.medis.queue_customer#"
	// 记录每个队列消费者的etcd key(为新启动的medis分配队列)
	queueConsumerKey = queueConsumerKeyPrefix + "%s#%s"
	// 全局锁对应的key(为新启动medis分配队列的时候要加分布式锁)
	lockKey = "__mec.medis.lock"
)

// NewBalance 返回Balance的单例
func NewBalance() *Balance {
	lock.Lock()
	defer lock.Unlock()

	if balanceInst != nil {
		return balanceInst
	}

	balanceInst = new(Balance)

	dc := doveclientCli.NewDoveClient("unix:////var/lib/doveclient/doveclient.sock")
	status, result, err := dc.Call("GetEtcdAddr", map[string]interface{}{})
	if err != nil {
		logger.GetLogger("DEBUG").Printf("获取当前环境etcd服务器列表时发生错误: %v", err)
		panic(fmt.Errorf("获取当前环境etcd服务器列表时发生错误: %v", err))
	}
	dc.Close()

	if status != "ok" {
		logger.GetLogger("DEBUG").Printf("调用doveclient接口失败: status=%s, result=%s", status, result)
		panic(fmt.Errorf("调用doveclient接口失败: status=%s, result=%s", status, result))
	}

	// 读取推送服务所处etcd环境的etcd服务器列表
	if err := json.Unmarshal(result, &balanceInst.endPoints); err != nil || len(balanceInst.endPoints) == 0 {
		logger.GetLogger("DEBUG").Printf("从doveclient api(GetEtcdAddr)返回结果中解析etcd地址失败: result=%s, err=%v", result, err)
		panic(fmt.Errorf("从doveclient api(GetEtcdAddr)返回结果中解析etcd地址失败: result=%s, err=%v", result, err))
	}

	// 获取当前环境的ip
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		logger.GetLogger("DEBUG").Printf("获取当前环境ip时发生错误:%v", err)
		panic(fmt.Errorf("获取当前环境ip时发生错误:%v", err))
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			balanceInst.ip = ipnet.IP.String()
			break
		}
	}

	if balanceInst.ip == "" {
		logger.GetLogger("DEBUG").Print("无法获取本机ip")
		panic("无法获取本机ip")
	}

	// 推送服务要注册到etcd的信息(消息中心管理后台需要这些信息)
	reginfo := map[string]interface{}{}
	if config.GetConfig().ListenAddr != "" && strings.Index(config.GetConfig().ListenAddr, ":") != -1 {
		reginfo["ListenAddr"] = balanceInst.ip + ":" + strings.Split(config.GetConfig().ListenAddr, ":")[1]
	}

	if config.GetConfig().DebugAddr != "" && strings.Index(config.GetConfig().DebugAddr, ":") != -1 {
		reginfo["DebugAddr"] = balanceInst.ip + ":" + strings.Split(config.GetConfig().DebugAddr, ":")[1]
	}

	if config.GetConfig().StatisticApiAddr != "" && strings.Index(config.GetConfig().StatisticApiAddr, ":") != -1 {
		reginfo["StatisticApiAddr"] = balanceInst.ip + ":" + strings.Split(config.GetConfig().StatisticApiAddr, ":")[1]
	}

	if config.GetConfig().PrometheusApiAddr != "" && strings.Index(config.GetConfig().PrometheusApiAddr, ":") != -1 {
		reginfo["PrometheusApiAddr"] = balanceInst.ip + ":" + strings.Split(config.GetConfig().PrometheusApiAddr, ":")[1]
	}

	reginfo["RECEPTION_ENV"] = config.GetConfig().RECEPTION_ENV
	reginfo["RunAtBench"] = config.GetConfig().RunAtBench

	balanceInst.reginfo, err = json.Marshal(reginfo)
	if err != nil {
		logger.GetLogger("DEBUG").Printf("编码推送服务配置信息失败:%v", err)
		panic(fmt.Errorf("编码推送服务配置信息失败:%v", err))
	}

	balanceInst.willExitChan = make(chan int)
	balanceInst.exitChan = make(chan int)

	return balanceInst
}

// Balance 负责从 队列实例池 中 为当前 推送服务 选择需要消费的 队列实例, 并在程序推出的时候, 释放队列资源.
type Balance struct {
	etcdCli   *clientv3.Client
	endPoints []string
	// 当前实例实际消费的队列实例
	consumerAddrs []string
	// 当前环境的本地ip
	ip           string
	reginfo      []byte
	willExitChan chan int
	exitChan     chan int
}

// Startup 上报当前 推送实例 的信息, 同时为当前 推送实例 分配需要消费的 队列实例.
func (b *Balance) Startup() {
	// 该函数会被调用2次
	// 第1次同步调用, 所有流程必须成功; 第1次调用入参leaseID == clientv3.NoLease
	// 第2次单独一个协程运行, 失败会重试; 第2次调用入参leaseID是第一次调用的返回值
	var bootstrap = func(etcdCli *clientv3.Client, leaseID clientv3.LeaseID) (*clientv3.Client, clientv3.LeaseID, error) {
		var leaseIDArg clientv3.LeaseID = leaseID
		var err error
		var keepChan <-chan *clientv3.LeaseKeepAliveResponse
		var grantResp *clientv3.LeaseGrantResponse
		var session *concurrency.Session
		var ctx context.Context
		var cancel context.CancelFunc

		// 第一次调用该函数时已经注册了推送服务的基本信息 和 分配了队列实例, 直接跳到续约的地方
		if leaseID != clientv3.NoLease {
			goto keepalive
		}

	retry:
		if etcdCli != nil {
			etcdCli.Close()
			logger.GetLogger("DEBUG").Printf("准备重建etcd连接...")
			time.Sleep(time.Second)
		}

		etcdCli, err = clientv3.New(clientv3.Config{
			Endpoints:            b.endPoints,
			DialTimeout:          3 * time.Second,
			DialKeepAliveTime:    5 * time.Second,
			DialKeepAliveTimeout: 3 * time.Second,
			AutoSyncInterval:     10 * time.Second,
		})
		if err != nil {
			if leaseID == clientv3.NoLease {
				return etcdCli, clientv3.NoLease, err
			}
			logger.GetLogger("DEBUG").Printf("操作失败:%v, 即将执行重试操作", err)
			goto retry
		}

		grantResp, err = etcdCli.Grant(context.TODO(), 10)
		if err != nil {
			if leaseID == clientv3.NoLease {
				return etcdCli, clientv3.NoLease, err
			}
			logger.GetLogger("DEBUG").Printf("操作失败:%v, 即将执行重试操作", err)
			goto retry
		}

		// 上报基本注册信息
		_, err = etcdCli.Put(context.TODO(), fmt.Sprintf(baseInfoKey, b.ip), string(b.reginfo), clientv3.WithLease(grantResp.ID))
		if err != nil {
			if leaseID == clientv3.NoLease {
				return etcdCli, clientv3.NoLease, err
			}
			logger.GetLogger("DEBUG").Printf("操作失败:%v, 即将执行重试操作", err)
			goto retry
		}

		if leaseIDArg == clientv3.NoLease {
			// 第一次调用该函数
			session, err = concurrency.NewSession(etcdCli)
			if err != nil {
				return etcdCli, clientv3.NoLease, err
			}
			defer session.Close()

			mu := concurrency.NewMutex(session, lockKey)

			ctx, cancel = context.WithTimeout(context.TODO(), time.Second*5)
			mu.Lock(ctx)

			b.consumerAddrs, err = b.selectQueue(etcdCli)
			if err != nil {
				mu.Unlock(ctx)
				cancel()
				return etcdCli, clientv3.NoLease, err
			}

			if err = b.createQueue2Medis(etcdCli, grantResp.ID); err != nil {
				etcdCli.Revoke(context.TODO(), grantResp.ID)
				mu.Unlock(ctx)
				cancel()
				return etcdCli, clientv3.NoLease, err
			}

			logger.GetLogger("DEBUG").Printf("全部队列实例列表:%s, 当前推送服务实例分配到的队列实例列表:%s", config.GetConfig().QueueServerAddr, strings.Join(b.consumerAddrs, ","))
			config.GetConfigPointer().QueueServerAddr = strings.Join(b.consumerAddrs, ",")

			mu.Unlock(ctx)
			cancel()
		} else {
			// 第二次调用该方法才会进入该流程
			if err = b.createQueue2Medis(etcdCli, grantResp.ID); err != nil {
				logger.GetLogger("DEBUG").Printf("操作失败:%v, 即将执行重试操作", err)
				goto retry
			}
		}

		leaseID = grantResp.ID

		// 首次进入该函数因为参数为clientv3.NoLease 所以不会直接走到这里
	keepalive:
		if leaseIDArg != clientv3.NoLease {
			keepChan, err = etcdCli.KeepAlive(context.TODO(), leaseID)
			if err != nil {
				logger.GetLogger("DEBUG").Printf("keepalive操作失败:%v, 即将执行重试操作", err)
				goto retry
			}

			for {
				select {
				case <-b.willExitChan:
					logger.GetLogger("DEBUG").Printf("收到进程退出信号, 即将结束keepalive循环...")
					if _, err = etcdCli.Revoke(context.TODO(), leaseID); err != nil {
						logger.GetLogger("DEBUG").Printf("Revoke操作失败:%v", err)
					}
					etcdCli.Close()
					close(balanceInst.exitChan)
					break
				case keepData := <-keepChan:
					if keepData == nil {
						logger.GetLogger("DEBUG").Printf("keepalive操作失败:%v, 即将执行重试操作", err)
						goto retry
					}
				}
			}
		}

		return etcdCli, leaseID, nil
	}

	etcdCli, leaseID, err := bootstrap(nil, clientv3.NoLease)
	if err != nil {
		logger.GetLogger("DEBUG").Printf("初始化环境失败:%v, 进程退出", err)
		panic(fmt.Errorf("初始化环境失败:%v, 进程退出", err))
	}
	go bootstrap(etcdCli, leaseID)
}

// Over 进程退出, 执行相应的清理操作
func (b *Balance) Over() {
	close(b.willExitChan)
	<-b.exitChan
	logger.GetLogger("DEBUG").Printf("balance流程结束")
}

// createQueue2Medis 建立queue inst到medis inst的关系
func (b *Balance) createQueue2Medis(etcdCli *clientv3.Client, leaseID clientv3.LeaseID) error {
	for _, addr := range b.consumerAddrs {
		if _, err := etcdCli.Put(context.TODO(), fmt.Sprintf(queueConsumerKey, addr, b.ip), time.Now().Format("2006-01-02 15:04:05"), clientv3.WithLease(leaseID)); err != nil {
			return err
		}
	}
	return nil
}

// selectQueue 为当前推送服务实例选择队列
// 一个quest inst可以有一个或多个medis inst, 将每个queue inst按medis inst数从小到大排序 并 为这些queue inst分配medis inst
func (b *Balance) selectQueue(etcdCli *clientv3.Client) ([]string, error) {
	resp, err := etcdCli.Get(context.TODO(), queueConsumerKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	queues := strings.Split(config.GetConfig().QueueServerAddr, ",")
	// 所有队列实例都没有消费者
	if resp.Count == 0 {
		if len(queues) < config.GetConfig().MedisPerMaxConsumerQueueNum {
			return queues, nil
		}

		return queues[:config.GetConfig().MedisPerMaxConsumerQueueNum], nil
	}

	// 检查etcd, 获得每个 队列实例 下 有多少个 推送服务 实例.
	// queue addr => medis addr => struct{}
	queue2medis := map[string]map[string]struct{}{}
	for _, kv := range resp.Kvs {
		arr := strings.Split(string(kv.Key), "#")
		if _, ok := queue2medis[arr[1]]; ok {
			queue2medis[arr[1]] = map[string]struct{}{}
		}

		// 异常数据
		if arr[2] == b.ip {
			continue
		}

		queue2medis[arr[1]][arr[2]] = struct{}{}
	}

	// 加入没有被任何 推送服务实例 消费的 队列实例
	for _, addr := range queues {
		if _, ok := queue2medis[addr]; !ok {
			queue2medis[addr] = map[string]struct{}{}
		}
	}

	// 将所有的 队列实例 按其绑定的 推送服务实例数 分组.
	// num2queue: 推送服务数量 => [队列实例1, 队列实例2]
	num2queue := map[int][]string{}
	medisnums := make([]int, 0, len(queue2medis))
	// queue2medis: queue addr => medis addr => struct{}
	for queue, medisInsts := range queue2medis {
		if _, ok := num2queue[len(medisInsts)]; !ok {
			num2queue[len(medisInsts)] = []string{}
			medisnums = append(medisnums, len(medisInsts))
		}
		num2queue[len(medisInsts)] = append(num2queue[len(medisInsts)], queue)
	}

	sort.Ints(medisnums)

	// 最终本推送服务将要使用的队列
	useQueues := make([]string, 0, config.GetConfig().MedisPerMaxConsumerQueueNum)
exit:
	for _, num := range medisnums {
		for _, queueAddr := range num2queue[num] {
			useQueues = append(useQueues, queueAddr)
			if len(useQueues) >= config.GetConfig().MedisPerMaxConsumerQueueNum {
				break exit
			}
		}
	}

	return useQueues, nil
}
