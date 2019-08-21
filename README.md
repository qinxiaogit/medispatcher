# MEDISPATCHER message/event dispatcher
medispatcher is the core component of message/event center. It dispatches the messages from the main incoming queue to topic channels, pushes messages from channels to subscribers.

## Configuration
refers to `Docs/etc/medispatcher.toml` .
## About "DATA" log    
data logs may hold very important data:

* __RECOVFAILBACK__  messages popped from recover list(redis list), failed being pushed to queue server and failed being pushed back to recover list.
* __REDISTFAIL__ messages popped from the main incoming queue on the queue server, but failed being redistributed to the sub queue (channel) for the subscriber, and they are not stored in queue anymore, should be recovered from logs.
* __RECOVFAILDECODE__  messages popped from recover list(redis list), failed being decoded.
* __RECOVFAILASSERT__  messages popped from recover list(redis list), decoded, but its type it not correct.
* __DELFAIL__  messages that failed to be deleted, and will lead to duplicated messages.   
* __DECODERR__ messages that failed to be decoded.
* __KICKFAIL__ messages that failed to be kicked to ready state from buried state. these messages wont be pushed to the subscribers, until being kicked to ready state manually.
* __RESENT__  _not so important._ message re-sent (to subscriber) logs.
* __SENT__  _not so important._ message sent (to subscriber for the first time) logs.

## About queue status 

* __READY__ messages that are to be processed.
* __BURIED__ messages that are being processing. if the dispatchers are stopped, and there're still messages on buried state, then they should be kicked to ready stat manually.
* __DELETED__ messages that have been processed whether successfully or not.  

## tools
### amender
* kick buried jobs to ready state, so processing can be continued. (buried messages are reserved from the ready state and to be processed by the workers, but for some reason they are not be deleted, .e.g.
the process was killed and all progresses were interrupted, or unable to connect to the queue server, so the delete command was not exected.), but be reminded, the amender may produces duplicated messages, 
on the other hand, if you donot run reminder, the subscribers may lose messages.

e.g.    
`./amender -f /path/to/medispatcher_config_file`

## Releases
### 2.0.0
First release of v2.    
#### Features    

* Multi-channel, parallel message push by channels. Push of subscriptions will no longer be blocked by other slow message receivers/consumers.
 
### 2.1.0
2.1.0 released(refers to the git tag).    

* Performance parameters can be controlled as to each subscription on the backend control panel.    
 parameters are: 
 <pre>
 count of concurrent push routines for the subscription.    
 count of concurrent retry-push routines for the subscription.     
 minimum interval between pushes.    
 message process timeout.     
 message process uri.     
 </pre>

* Above parameters take effects instantly. Do not requires dispatcher service restarting or reloading.

### 2.2.0 
2.2.0 released(refers to the git tag).    

* Alerts when message processing failures reaches certain thresholds.     
    * alert when the subscription processor fails on a certain frequency.
    * alert when a message processing failing times reached the threshold.
    * alert when the subscription channel queued message count reached the threshold.
* Fixed the optimization for rpc service exiting: the readings from client will no longer block exiting.    
* Customizable config file    
 e.g. medispatcher /path/to/config/file
* Multiple message processing worker url support. The urls can be multi-lined, each line represents a separate worker url. This achieves a soft load balance.
 
### 2.3.1
2.3.1 released(refers to the git tag). 

#### Features
* Added help commandline args.
* Added version display commandline args.


### 2.3.2
2.3.2 released(refers to the git tag). 

#### Features
* Log files can be configured to a single file.
* Added build date  to version info.
* Version info contains build time. 
 
 
## Milestone

### 2.1.0
#### Features    

* Performance parameters can be controlled as to each subscription on the backend control panel.    
 parameters are: count of concurrent push routines for the subscription, count of concurrent retry-push routines for the subscription, minimum interval between pushes.
 
### 2.2.0
#### Features    
* Alerts when message processing failures reaches certain thresholds.    
* Customizable config file    
 e.g. medispatcher /path/to/config/file
* Multiple message processing worker url support. The urls can be multi-lined, each line represents a separate worker url. This achieves a soft load balance. 

### 2.3.0
#### Features
* Performance and message server link utilization enhancement. A link can binds to multiple message pushing routines.
 
### 2.3.1
#### Features
* Added help commandline args.
* Added version display commandline args.

#### Fix
* fixed un-handled buried messages on exits.
* fixed service may not exits gracefully.
 
### 2.3.3
2.3.3 released(refers to the git tag).    
#### Enhancement 
* stack will be logged when broker client encounter an error.
#### Fix
* fixed random subscription url will be used when no tagged env url are found. after this fixs, when tagged env has no subscription urls, the messages to the env will be dropped and issues a warning log. 
 
### 2.4.0
#### Features

* Messages sent/received statistics

### 2.4.2
#### Features

* 接入统一告警平台.

### 2.4.2.1
#### Features

* 支持对没有设置报警接收人的订阅添加默认报警接收人.

### 2.4.2.3
#### Features

* 支持context上下文传递.

### 2.4.3
#### Features

* 支持订阅方配置是否接收压测环境消息.

### 2.4.3.3
#### Features

* 支持通过sendmail发送邮件(AlerterEmail.Gateway="sendmail://")

### 2.4.4
#### Enhancement 

* 对未设置处理地址的订阅，不发送报警消息.

### 2.4.6
#### Features

* 支持队列清理功能

### 2.4.7
#### Features

* 从redis中取出的job在写入beanstalkd时如果发生JOB_TOO_BIG错误, 则不重新放回到redis(防止死循环).
* 修复队列清理功能bug, 解决清理特定的通道时, 如果该通道一直有大量的数据流入, 导致清理操作无法终止的问题.

