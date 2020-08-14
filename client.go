package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogf/gf/frame/g"
	"github.com/gogf/gf/util/gconv"
	"github.com/google/uuid"
	"log"
	"time"
)

// 这里的msgMAP 就是为了开一个地方用于投递
var msgMap map[string]*chan g.Map

func init() {
	msgMap = make(map[string]*chan g.Map)
}

/**
关于标记消息来回的规则
g.map{
		"msg_id": id,
		"code":   1,
		"data":   data,
		"from_topic":  topic,
}

这里如果信息是发出的，则code需要赋值为1
如果是收到code为1 就是收到查询请求
回复的信息 code为2
回复的信息 code为2
回复的信息 code为2
重要的事情说三遍

如果code为0 消息会被直接跳过
*/

/**
表面oo的超级大类
*/
type Bodhi struct {
	uRL      string
	topic    string
	Client   pulsar.Client
	Consumer pulsar.Consumer
}

/**
@url 是broker的地址
@topic 是你自己接受消息的topic
@f 是接收到查询请求的回调函数
*/
func (b *Bodhi) New(Url string, Topic string, f func(byte2 []byte)) error {
	b.uRL = Url
	b.topic = Topic
	var err error
	b.Client, err = pulsar.NewClient(pulsar.ClientOptions{
		URL:               b.uRL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})

	if err != nil {
		log.Fatal(err)
		return err
	}
	b.Consumer, err = b.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:            b.topic,
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
		return err
	}
	// 开始loop
	go b.loop(f)

	return nil
}

/**
这个函数你调用不到的，
*/
func (b *Bodhi) loop(f func(byte2 []byte)) {
	for {
		msg, err := b.Consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		var m g.Map
		_ = json.Unmarshal(msg.Payload(), &m)
		code, ok := m["code"].(float64)
		if !ok {
			continue
		}
		c := gconv.Int(code)
		if c != 1 {
			// 处理信息投递
			post(&m)
			continue
		} else if c == 0 {
			b.Consumer.Ack(msg)
			continue
		}
		b.Consumer.Ack(msg)
		// 回调函数处理msg
		go f(msg.Payload())
	}
}

// 向管道推信息
func post(p *g.Map) {
	c, ok := msgMap[(*p)["msg_id"].(string)]
	if !ok {
		log.Println("map index error")
		return
	}
	if c != nil {
		*c <- *p
	}
}

/**
向某个topic发送信息 ，并且等待回复，返回值的map就是回复
@data 格式为string的数据
@topic 要发送的string
*/
func (b *Bodhi) SendMsgAndWaitReply(data string, topic string) (g.Map, error) {
	// 新建一个uuid
	id := uuid.New()
	// 构建payload
	payload := g.Map{
		"msg_id":     id.String(),
		"code":       1,
		"data":       data,
		"from_topic": b.topic,
	}
	// 将payload 转化为字节数组
	content, err := json.Marshal(payload)

	// 新建发送工具
	producer, err := b.Client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return nil, err
	}
	defer producer.Close()

	// 初始化一个ch用于接受消息
	ch := make(chan g.Map, 1)

	// 将消息接受通道注册到 消息全局map
	msgMap[id.String()] = &ch
	defer func() {
		delete(msgMap, id.String())
		close(ch)
	}()

	// 发送消息
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: content,
	})

	if err != nil {
		return nil, err
	}

	// 等待响应消息到来，3秒后超时
	select {
	case m := <-ch:
		{
			return m, nil
		}
	case <-time.After(3 * time.Second):
		{
			return nil, errors.New("reply time out")
		}

	}
}

/**
发送响应
@id 消息id
@data 消息体
@topic topic
*/
func (b *Bodhi) SendReply(id string, data string, topic string) error {
	payload := g.Map{
		"msg_id":     id,
		"code":       2,
		"data":       data,
		"from_topic": b.topic,
	}
	content, err := json.Marshal(payload)

	producer, err := b.Client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return err
	}
	defer producer.Close()

	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: content,
	})

	if err != nil {
		return err
	}

	return nil
}
