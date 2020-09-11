# Bodhi
中文名 菩提

名字起因来自于小伙伴 把 apache pulsar叫做阿帕奇·菩萨

这个项目是用阿帕奇·菩萨 这种信息单向mq，通过对client的魔改，变成ps架构+类HTTP响应式通信


### 入门

#### 接受并处理消息
```go
package main

import (
    "github.com/ZZINNO/bodhi/client"
)
var bod client.Bodhi
func main(){
    _ = bod.New(client.Config{
    		Url:      "pulsar://localhost:6650",
    		Topic:    "my-topic",// 别人给自己发消息的时候自己监听的topic
    		CallBack: callback,
    		TimeOut:  5,
    	})
    select{ }
}

func callback(byte2 client.RequestMsg) map[string]interface{} {
	return map[string]interface{}{"hello": 123}
}
```

#### 发送消息
```go
package main

import (
    "fmt"
    "github.com/ZZINNO/bodhi/client"
)
var bod client.Bodhi
func main(){
// New(Url string, Topic string, f func(byte2 []byte))
    _ = bod.New(client.Config{
    		Url:      "pulsar://localhost:6650",
    		Topic:    "my-topic",// 别人给自己发消息的时候自己监听的topic
    		CallBack: callback, // 接受查询消息的回调函数
    		TimeOut:  5,// 超时时间 5秒
    	})
    // ↓respond 这里的响应是自己给自己发了一个消息，然后调用响应
    resp, err := bod.SendMsgAndWaitReply(client.Data{
		Key:     "test-key",
		Columns: "/my-test/columns",
	}, "my-topic")
    if err!= nil{
        fmt.Println(err)
    }
    fmt.Println(resp)
    select{ }
}

func callback(byte2 client.RequestMsg) map[string]interface{} {
	return map[string]interface{}{"hello": 123}
}
```

#### 进阶 压力测试
```go
package main

import (
	"fmt"
	"github.com/ZZINNO/bodhi/client"
	"sync"
	"time"
)

var bod client.Bodhi
var errNum int

func main() {
	testNum := 10000
	du := make(chan int64, testNum)
	var wg sync.WaitGroup
	start := time.Now().UnixNano() / int64(time.Millisecond)
	// New(Url string, Topic string, f func(byte2 []byte))
	_ = bod.New(client.Config{
		Url:      "pulsar://localhost:6650",
		Topic:    "my-topic",
		CallBack: callback,
		TimeOut:  5,
	})

	var all int64
	var test int64
	for {
		wg.Add(1)
		test += 1
		go func(wg *sync.WaitGroup, du *chan int64) {
			defer wg.Done()
			err := sendMessage(du)
			if err != nil {
				return
			}
		}(&wg, &du)
		if test == int64(testNum) {
			break
		}
	}
	wg.Wait()
	end := time.Now().UnixNano() / int64(time.Millisecond)
	lent := len(du)
	count := len(du)
	for v := range du {
		lent -= 1
		all = all + v
		if lent <= 0 {
			break
		}
	}
	fmt.Println("request count:", testNum)
	fmt.Println("success rate:", count/testNum*100, "%")
	fmt.Println("per cost:", all/test, "ms")
	fmt.Println("avg cost:", (end-start)/test, "ms")
	fmt.Println("all cost:", end-start, "ms")
	fmt.Println("ERRNUM:", errNum)
	fmt.Println("rps:", int64(testNum)*1000/(end-start), "request/s")
}

func sendMessage(du *chan int64) error {
	start := time.Now().UnixNano() / int64(time.Millisecond)
	_, err := bod.SendMsgAndWaitReply(client.Data{
		Key:     "test-key",
		Columns: "/my-test/columns",
	}, "my-topic")
	if err != nil {
		errNum += 1
		return err
	}
	//fmt.Println(re.Data)
	end := time.Now().UnixNano() / int64(time.Millisecond)
	cost := end - start
	*du <- cost
	return nil
}

func callback(byte2 client.RequestMsg) map[string]interface{} {
	return map[string]interface{}{"hello": 123}
}
```


测试结果：
```
request count: 100000
success rate: 100 %
per cost: 2900 ms
avg cost: 0 ms
all cost: 5550 ms
ERRNUM: 0
rps: 18018 request/s
```


