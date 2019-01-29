#beanstalkc    
Client for Beanstalk queue server.

###Example 
    c,err := Dial("127.0.0.1:11300")    
    if err == nil {
        // Consume message
        c.Watch("test_tube")
        jobId, jobBody, err := c.Reserve()
        fmt.Printf("%v\n%s\n", jobId, jobBody)
        c.Delete(jboId)
        
        // Publish message
        c.Use("test_tube")
        jobId, burried, err := c.Put(1024, 0, 60, []byte("Test Message"))
        fmt.Printf("%v\n%v\n%v", jobId, burried, err)
    }

