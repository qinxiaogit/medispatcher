#MEDISPATCHER message/event dispatcher
medispatcher is the core component of message/event center. It dispatches the message from the main incoming queue to topic channels, pushes messages from channels to subscribers.

## About "DATA" log    
data logs may hold very important data:

* __RECOVFAILBACK__  message popped from recover list(redis list), failed being pushed to queue server and failed being pushed back to recover list.
* __REDISTFAIL__ message popped from the main incoming queue on the queue server, but failed being redistributed to the sub queue (channel) for the subscriber.
* __RECOVFAILDECODE__  message popped from recover list(redis list), failed being decoded.
* __RECOVFAILASSERT__  message popped from recover list(redis list), decoded, but type not correct.

* __RESENT__  _not so important._ message re-sent (to subscriber) logs.
* __SENT__  _not so important._ message sent (to subscriber for the first time) logs.