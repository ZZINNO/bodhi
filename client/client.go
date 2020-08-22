package client

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogf/gf/util/gconv"
	"github.com/google/uuid"
	"log"
	"time"
)

// 这里的msgMAP 就是为了开一个地方用于投递
var msgMap map[string]*chan RespondMsg

func init() {
	msgMap = make(map[string]*chan RespondMsg)
}

type RequestMsg struct {
	MagId     string  `json:"mag_id"`
	Code      float64 `json:"code"`
	Data      Data    `json:"data"`
	FromTopic string  `json:"from_topic"`
}

/**
Columns 是专门用在case 进行整体匹配的, 每个case需要针对性的约定开发
*/

type Data struct {
	Key     string `json:"key"`
	Columns string `json:"columns"`
	/**
	key = "1"
	*/
	/**
	columns = "/user/schoolId&username&schoolName"
	*/
}

type RespondMsg struct {
	MagId     string                 `json:"mag_id"`
	Code      float64                `json:"code"`
	Data      map[string]interface{} `json:"data"`
	FromTopic string                 `json:"from_topic"`
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
	TimeOut  time.Duration
}
type Config struct {
	Url      string
	Topic    string
	CallBack func(msg RequestMsg)
	TimeOut  time.Duration
}

/**
@url 是broker的地址
@topic 是你自己接受消息的topic
@f 是接收到查询请求的回调函数
*/
func (b *Bodhi) New(config Config) error {
	b.uRL = config.Url
	b.topic = config.Topic
	b.TimeOut = config.TimeOut
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
	go b.loop(config.CallBack)

	return nil
}

/**
这个函数你调用不到的，这个是bodhi自己维护的消息循环
*/
func (b *Bodhi) loop(f func(msg RequestMsg)) {
	for {
		msg, err := b.Consumer.Receive(context.Background())
		b.Consumer.Ack(msg)
		if err != nil {
			log.Fatal(err)
		}
		var m RespondMsg
		_ = json.Unmarshal(msg.Payload(), &m)
		code := m.Code
		c := gconv.Int(code)
		if c != 1 {
			// 处理信息投递
			post(&m)
			continue
		} else if c == 0 {
			b.Consumer.Ack(msg)
			continue
		}
		// 回调函数处理msg
		var rm RequestMsg
		_ = json.Unmarshal(msg.Payload(), &rm)
		go f(rm)
	}
}

// 向管道推信息
func post(p *RespondMsg) {
	c, ok := msgMap[(*p).MagId]
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
func (b *Bodhi) SendMsgAndWaitReply(data Data, topic string) (*RespondMsg, error) {
	// 新建一个uuid
	id := uuid.New()
	// 构建payload
	payload := RequestMsg{
		MagId:     id.String(),
		Code:      1,
		Data:      data,
		FromTopic: b.topic,
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
	ch := make(chan RespondMsg, 1)

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

	// 等待响应消息到来，TimeOut秒后超时

	select {
	case m := <-ch:
		{
			return &m, nil
		}
	case <-time.After(b.TimeOut):
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
func (b *Bodhi) SendReply(id string, data map[string]interface{}, topic string) error {
	payload := RespondMsg{
		MagId:     id,
		Code:      2,
		Data:      data,
		FromTopic: b.topic,
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
