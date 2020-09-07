package client

import (
	"context"
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

//
//func init() {
//	msgMap = make(map[string]*chan RespondMsg)
//}
type RequestMsg struct {
	MagId     string
	Data      Data
	FromTopic string
}

/**
Columns 是专门用在case 进行整体匹配的, 每个case需要针对性的约定开发
*/
type Data struct {
	Key     string
	Columns string
	/**
	key = "1"
	*/
	/**
	columns = "/user/schoolId&username&schoolName"
	*/
}

type RespondMsg struct {
	MagId     string
	Code      float64
	Data      map[string]interface{}
	FromTopic string
}

/**
表面oo的超级大类
*/
type Bodhi struct {
	uRL            string
	topic          string
	serviceClient  pulsar.Client
	replyClient    pulsar.Client
	serverConsumer pulsar.Consumer
	replyConsumer  pulsar.Consumer
	replyTopic     string
	CallBack       func(msg RequestMsg) map[string]interface{}
	timeOut        int
	msgMap         sync.Map
	producerMap    sync.Map
}
type Config struct {
	Url      string
	Topic    string
	CallBack func(msg RequestMsg) map[string]interface{}
	TimeOut  int
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

/*
随机字符串生成
*/
func (b *Bodhi) RandString(len int) string {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

/**
@url 是broker的地址
@topic 是你自己接受消息的topic
@f 是接收到查询请求的回调函数
*/
func (b *Bodhi) New(config Config) error {
	b.uRL = config.Url
	b.topic = config.Topic
	b.timeOut = config.TimeOut
	b.CallBack = config.CallBack
	b.replyTopic = config.Topic + b.RandString(6)
	SubscriptionName := b.RandString(6)
	var err error
	b.serviceClient, err = pulsar.NewClient(pulsar.ClientOptions{
		URL:               b.uRL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	b.replyClient, err = pulsar.NewClient(pulsar.ClientOptions{
		URL:               b.uRL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		logrus.Error(err)
		return err
	}
	b.replyConsumer, err = b.replyClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            b.replyTopic,
		SubscriptionName: SubscriptionName + "-reply",
		Type:             pulsar.Shared,
	})
	b.serverConsumer, err = b.serviceClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            b.topic,
		SubscriptionName: SubscriptionName + "-sender",
		Type:             pulsar.Shared,
	})
	if err != nil {
		logrus.Error(err)
		return err
	}
	// 开始loop
	go b.serverLoop()
	go b.replyLoop()
	return nil
}

/**
对外服务
*/
func (b *Bodhi) serverLoop() {
	for {
		msg, err := b.serverConsumer.Receive(context.Background())
		if err != nil {
			logrus.Error(err)
			continue
		}
		go b.dealMsg(msg)
	}
}
func (b *Bodhi) dealMsg(msg pulsar.Message) {
	go b.serverConsumer.Ack(msg)
	var rm RequestMsg
	_ = msgpack.Unmarshal(msg.Payload(), &rm)
	rep := b.CallBack(rm)
	err := b.sendReply(rm.MagId, rep, rm.FromTopic)
	if err != nil {
		logrus.Error(err)
	}
	return
}

/**
接收响应
*/
func (b *Bodhi) replyLoop() {
	for {
		msg, err := b.replyConsumer.Receive(context.Background())
		if err != nil {
			logrus.Error(err)
			continue
		}
		go b.replyMsg(msg)
	}
}

func (b *Bodhi) replyMsg(msg pulsar.Message) {
	go b.replyConsumer.Ack(msg)
	var m RespondMsg
	_ = msgpack.Unmarshal(msg.Payload(), &m)
	err := b.post(m)
	if err != nil {
		logrus.Error(err)
	}
	return
}

// 向管道推信息
func (b *Bodhi) post(p RespondMsg) error {
	//c, ok := msgMap[p.MagId]
	c, ok := b.msgMap.Load(p.MagId)
	if !ok {
		return errors.New("map index error")
	}
	if c != nil {
		*c.(*chan RespondMsg) <- p
	}
	return nil
}

/**
向某个topic发送信息 ，并且等待回复，返回值的map就是回复
@data 格式为string的数据
@topic 要发送的string
*/
func (b *Bodhi) SendMsgAndWaitReply(data Data, topic string) (RespondMsg, error) {
	// 新建一个uuid
	id := uuid.New()
	// 构建payload
	payload := RequestMsg{
		MagId:     id.String(),
		Data:      data,
		FromTopic: b.replyTopic,
	}
	// 将payload 转化为字节数组
	content, err := msgpack.Marshal(payload)
	var producer pulsar.Producer
	pload, ok := b.producerMap.Load(topic)
	if !ok {
		producer, _ = b.replyClient.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
		})
		b.producerMap.Store(topic, &producer)
	} else {
		producer = *pload.(*pulsar.Producer)
	}
	//defer producer.Close()
	// 这里就不在结束的时候关闭通道了，反正你也不可能建立那么多通道对吧
	// 初始化一个ch用于接受消息
	ch := make(chan RespondMsg, 1)
	timeOut := make(chan bool, 1)
	// 将消息接受通道注册到 消息全局map
	key := id.String()
	b.msgMap.Store(key, &ch)
	//msgMap[key] = &ch
	defer func() {
		//delete(msgMap, key)
		b.msgMap.Delete(key)
		close(ch)
	}()
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: content,
	})
	// 发送消息
	go func() {
		time.Sleep(time.Duration(b.timeOut) * time.Second) // 等待n秒钟
		timeOut <- true
	}()
	if err != nil {
		return RespondMsg{}, err
	}
	// 等待响应消息到来，TimeOut秒后超时
	select {
	case m := <-ch:
		{
			return m, nil
		}
	case <-timeOut:
		{
			return RespondMsg{}, errors.New("reply time out")
		}
	}
}

/**
发送响应
@id 消息id
@data 消息体
@topic topic
*/
func (b *Bodhi) sendReply(id string, data map[string]interface{}, topic string) error {
	payload := RespondMsg{
		MagId:     id,
		Code:      2,
		Data:      data,
		FromTopic: b.topic,
	}
	content, err := msgpack.Marshal(payload)
	var producer pulsar.Producer
	pload, ok := b.producerMap.Load(topic)
	if !ok {
		producer, _ = b.serviceClient.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
		})
		b.producerMap.Store(topic, &producer)
	} else {
		producer = *pload.(*pulsar.Producer)
	}
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: content,
	})
	if err != nil {
		return err
	}
	return nil
}
