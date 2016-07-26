#MEDISPATCHER message/event dispatcher
medispatcher is the core component of message/event center. It dispatches the messages from the main incoming queue to topic channels, pushes messages from channels to subscribers.

## About "DATA" log    
data logs may hold very important data:

* __RECOVFAILBACK__  messages popped from recover list(redis list), failed being pushed to queue server and failed being pushed back to recover list.
* __REDISTFAIL__ messages popped from the main incoming queue on the queue server, but failed being redistributed to the sub queue (channel) for the subscriber, and they are not stored in queue anymore, should be recovered from logs.
* __RECOVFAILDECODE__  messages popped from recover list(redis list), failed being decoded.
* __RECOVFAILASSERT__  messages popped from recover list(redis list), decoded, but its type it not correct.
* __DELFAIL__  messages that failed to be deleted, and will lead to duplicated messages.   
* __DECODERR__ messages that failed to be decoded.
  

* __RESENT__  _not so important._ message re-sent (to subscriber) logs.
* __SENT__  _not so important._ message sent (to subscriber for the first time) logs.

## About queue status 

* __READY__ messages that are to be processed.
* __BURIED__ messages that are being processing. if the dispatchers are stopped, and there're still messages on buried state, then they should be kicked to ready stat manually.
* __DELETED__ messages that have been processed whether successfully or not.  

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
 
## Milestone

### 2.1.0
####Features    

* Performance parameters can be controlled as to each subscription on the backend control panel.    
 parameters are: count of concurrent push routines for the subscription, count of concurrent retry-push routines for the subscription, minimum interval between pushes.
 
### 2.2.0
####Features    
* Alerts when message processing failures reaches certain thresholds.    
* Customizable config file    
 e.g. medispatcher /path/to/config/file
* Multiple message processing worker url support. The urls can be multi-lined, each line represents a separate worker url. This achieves a soft load balance. 

### 2.3.0
####Features
* Performance and message server link utilization enhancement. A link can binds to multiple message pushing routines.
 
### 2.4.0
####Features

* Messages sent/received statistics
