package balance

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/qinxiaogit/medispatcher/config"
	"github.com/qinxiaogit/medispatcher/logger"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	doveclientCli "github.com/qinxiaogit/medispatcher/doveclient"
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
	// 推送服务在etcd注册的租约的时间
	grantTime = 10
	// 控制medis重启的etcd key(medispatcher项目会watch这个key的变化决定是否重启medis)
	// 该变量被推送网关操控(参考mec-gateway项目代码)
	medisRebootKey = "__mec.medis.reboot"
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
	balanceInst.reginfo = map[string]interface{}{}
	if config.GetConfig().ListenAddr != "" && strings.Index(config.GetConfig().ListenAddr, ":") != -1 {
		balanceInst.reginfo["ListenAddr"] = balanceInst.ip + ":" + strings.Split(config.GetConfig().ListenAddr, ":")[1]
		balanceInst.listenAddr = balanceInst.ip + ":" + strings.Split(config.GetConfig().ListenAddr, ":")[1]
	}

	if config.GetConfig().DebugAddr != "" && strings.Index(config.GetConfig().DebugAddr, ":") != -1 {
		balanceInst.reginfo["DebugAddr"] = balanceInst.ip + ":" + strings.Split(config.GetConfig().DebugAddr, ":")[1]
	}

	if config.GetConfig().StatisticApiAddr != "" && strings.Index(config.GetConfig().StatisticApiAddr, ":") != -1 {
		balanceInst.reginfo["StatisticApiAddr"] = balanceInst.ip + ":" + strings.Split(config.GetConfig().StatisticApiAddr, ":")[1]
	}

	if config.GetConfig().PrometheusApiAddr != "" && strings.Index(config.GetConfig().PrometheusApiAddr, ":") != -1 {
		balanceInst.reginfo["PrometheusApiAddr"] = balanceInst.ip + ":" + strings.Split(config.GetConfig().PrometheusApiAddr, ":")[1]
	}

	balanceInst.reginfo["RECEPTION_ENV"] = config.GetConfig().RECEPTION_ENV
	balanceInst.reginfo["RunAtBench"] = config.GetConfig().RunAtBench
	balanceInst.reginfo["MedisPerMaxConsumerQueueNum"] = config.GetConfig().MedisPerMaxConsumerQueueNum

	balanceInst.willExitChan = make(chan int)
	balanceInst.exitChan = make(chan int)

	go balanceInst.rebalance()

	return balanceInst
}

// Balance 负责从 队列实例池 中 为当前 推送服务 选择需要消费的 队列实例, 并在程序推出的时候, 释放队列资源.
type Balance struct {
	etcdCli   *clientv3.Client
	endPoints []string
	// 当前实例实际消费的队列实例
	consumerAddrs []string
	// 当前环境的本地ip
	ip string
	// 需要上报到etcd的推送服务配置
	reginfo    map[string]interface{}
	listenAddr string
	// 退出
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

		grantResp, err = etcdCli.Grant(context.TODO(), grantTime)
		if err != nil {
			if leaseID == clientv3.NoLease {
				return etcdCli, clientv3.NoLease, err
			}
			logger.GetLogger("DEBUG").Printf("操作失败:%v, 即将执行重试操作", err)
			goto retry
		}

		// 上报基本注册信息
		_, err = etcdCli.Put(context.TODO(), fmt.Sprintf(baseInfoKey, b.listenAddr), string(b.MarshalRegInfo()), clientv3.WithLease(grantResp.ID))
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

// MarshalRegInfo 序列化注册信息
func (b *Balance) MarshalRegInfo() []byte {
	b.reginfo["ReportDate"] = time.Now().Format("2006-01-02 15:04:05")
	body, _ := json.Marshal(b.reginfo)
	return body
}

// createQueue2Medis 建立queue inst到medis inst的关系
func (b *Balance) createQueue2Medis(etcdCli *clientv3.Client, leaseID clientv3.LeaseID) error {
	for _, addr := range b.consumerAddrs {
		if _, err := etcdCli.Put(context.TODO(), fmt.Sprintf(queueConsumerKey, addr, b.listenAddr), string(b.MarshalRegInfo()), clientv3.WithLease(leaseID)); err != nil {
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

	// 检查etcd, 找到每个 队列实例 下 有多少个 <有效的>推送服务 实例.
	// queue addr => medis addr => struct{}
	queue2medis := map[string]map[string]struct{}{}
	for _, kv := range resp.Kvs {
		arr := strings.Split(string(kv.Key), "#")

		// 正常消息 和 bench消息 部署的是不同的beanstalkd实例 并 由不同的推送服务推送
		// 正常消息 和 bench消息 的推送服务 都会使用 "__mec.medis.queue_customer#队列ip+端口:推送服务rpc监听地址" 到etcd注册, 这时候必须保证 两种类型的推送服务在选择队列实例的时候不会出现混淆.
		exists := false
		for _, addr := range queues {
			if addr == arr[1] {
				exists = true
				break
			}
		}

		if !exists {
			continue
		}

		// 如果推送服务崩溃而不是正常退出, 那么对应推送服务注册的信息要等待grantTime后才过期, 可能会造成某个队列实例仍然在被推送服务消费的假象
		// 推送服务存活检测, 如果没有存活则认为特定的队列实例没有被特定的推送服务实例消费
		reginfo := map[string]interface{}{}
		if err := json.Unmarshal(kv.Value, &reginfo); err == nil {
			if listenAddr, ok := reginfo["ListenAddr"].(string); ok && listenAddr != b.reginfo["ListenAddr"].(string) {
				conn, err := net.DialTimeout("tcp", listenAddr, time.Second*2)
				// 特定的推送服务似乎已经崩溃, 因此并不能算作特定队列实例的有效消费者
				if err != nil {
					continue
				}
				conn.Close()
			}
		}

		// 尝试为当前推送服务实例分配队列实例的时候, 发现当前实例已经在消费特定的队列实例了, 这种情况不可能发生(可能是前一个实例崩溃, 而新启动的实例使用了相同的ip), 所以这时候认为特定的队列实例下的这个消费者并不是有效的.
		if arr[2] == b.listenAddr {
			continue
		}

		if _, ok := queue2medis[arr[1]]; !ok {
			queue2medis[arr[1]] = map[string]struct{}{}
		}

		queue2medis[arr[1]][arr[2]] = struct{}{}
	}

	// queues 在这里是所有有效的推送服务列表
	// queues 和上一步的数据queue2medis 合并, 最终得到每个队列实例下有效的推送服务数(为0或为N)
	for _, addr := range queues {
		if _, ok := queue2medis[addr]; !ok {
			queue2medis[addr] = map[string]struct{}{}
		}
	}

	// 将所有的 队列实例 按 有效推送服务数 进行分组.
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

	// 优先为当前新启动的推送服务实例(即本实例) 分配 有效消费者数 最少的队列实例.
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

// rebalance 在队列和推送服务 不均衡的时候, 退出推送服务以便重新分配.
func (b *Balance) rebalance() {
	var etcdCli *clientv3.Client
	var err error
retry:
	if etcdCli != nil {
		etcdCli.Close()
		time.Sleep(time.Second)
		logger.GetLogger("DEBUG").Printf("Reconnecting to the etcd server...")
	}
	etcdCli, err = clientv3.New(clientv3.Config{
		Endpoints:            b.endPoints,
		DialTimeout:          3 * time.Second,
		DialKeepAliveTime:    5 * time.Second,
		DialKeepAliveTimeout: 3 * time.Second,
		AutoSyncInterval:     10 * time.Second,
	})
	if err != nil {
		logger.GetLogger("DEBUG").Printf("Failed to connect to etcd server(%s):%v", strings.Join(b.endPoints, ","), err)
		goto retry
	}

	resp, err := etcdCli.Get(context.TODO(), medisRebootKey)
	if err != nil {
		logger.GetLogger("DEBUG").Printf("etcd except(GET %s):%v", medisRebootKey, err)
		goto retry
	}

	watchChan := etcdCli.Watch(context.TODO(), medisRebootKey, clientv3.WithRev(resp.Header.Revision))
done:
	for {
		select {
		case <-b.willExitChan:
			etcdCli.Close()
			break
		case e, ok := <-watchChan:
			if !ok {
				logger.GetLogger("DEBUG").Printf("etcd watch except:%s", medisRebootKey)
				goto retry
			}

			for _, event := range e.Events {
				if event.Type == clientv3.EventTypePut {
					if string(event.Kv.Value) == "bench" && config.GetConfig().RunAtBench {
						etcdCli.Close()
						logger.GetLogger("DEBUG").Printf("watch %s=%s, The process is about to exit", medisRebootKey, string(event.Kv.Value))
						syscall.Kill(os.Getpid(), syscall.SIGINT)
						break done
					}

					if string(event.Kv.Value) == "normal" && !config.GetConfig().RunAtBench {
						etcdCli.Close()
						logger.GetLogger("DEBUG").Printf("watch %s=%s, The process is about to exit", medisRebootKey, string(event.Kv.Value))
						syscall.Kill(os.Getpid(), syscall.SIGINT)
						break done
					}
				}
			}
		}
	}
}
