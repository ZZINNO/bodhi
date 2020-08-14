# Bodhi
中文名 菩提

名字起因来自于小伙伴 把 apache pulsar叫做阿帕奇·菩萨

这个项目是用阿帕奇·菩萨 这种信息单向mq，通过对client的魔改，变成ps架构+类HTTP响应式通信


### 入门
```go
package main

import (

"fmt"
"github.com/ZZINNO/bodhi"
)

func main(){
    bod:= bodhi.Bodhi{}
// New(Url string, Topic string, f func(byte2 []byte))
    bod.New("pulsar://localhost:6650","my-topic",func(byte2 []byte) {
        fmt.Println(string(byte2))
    })
    select{ }
}
```
