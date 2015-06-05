#MEDISPATCHER message/event dispatcher
medispatcher is the core component of message/event center. It dispatches the messages from the main incoming queue to topic channels, pushes messages from channels to subscribers.

## About "DATA" log    
data logs may hold very important data:

* __RECOVFAILBACK__  messages popped from recover list(redis list), failed being pushed to queue server and failed being pushed back to recover list.
* __REDISTFAIL__ messages popped from the main incoming queue on the queue server, but failed being redistributed to the sub queue (channel) for the subscriber, and they are not stored in queue anymore, should be recovered from logs.
* __RECOVFAILDECODE__  messages popped from recover list(redis list), failed being decoded.
* __RECOVFAILASSERT__  message popped from recover list(redis list), decoded, but its type it not correct.

* __RESENT__  _not so important._ message re-sent (to subscriber) logs.
* __SENT__  _not so important._ message sent (to subscriber for the first time) logs.

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
 
## Milestone

### 2.1.0
####Features    

* Performance parameters can be controlled as to each subscription on the backend control panel.    
 parameters are: count of concurrent push routines for the subscription, count of concurrent retry-push routines for the subscription, minimum interval between pushes.
 
### 2.2.0
####Features    
* Alerters when message processing failures reaches certain thresholds.    

### 2.3.0
####Features
* Performance and message server link utilization enhancement. A link can binds to multiple message pushing routines.
 
### 2.4.0
####Features

* Messages sent/received statistics
