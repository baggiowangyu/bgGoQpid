package Qpid

import (
	"context"
	"pack.ag/amqp"
	"time"
)

type QpidClient struct {
	amqp_client *amqp.Client
	amqp_session *amqp.Session
}

// 匿名登录
func (c *QpidClient) ConnSASLAnonymous(addr string) error {
	client, err := amqp.Dial(addr, amqp.ConnSASLAnonymous())
	if err != nil {
		return err
	}

	c.amqp_client = client
	return nil
}

// 账号密码登录
func (c *QpidClient) ConnSASLPlain(addr string, username string, password string) error {
	client, err := amqp.Dial(addr, amqp.ConnSASLPlain(username, password))
	if err != nil {
		return err
	}

	c.amqp_client = client
	return nil
}

// 打开会话
func (c *QpidClient) NewSession() error {
	session, err := c.amqp_client.NewSession()
	if err != nil {
		return err
	}

	c.amqp_session = session

	return nil
}

// 发送消息
func (c *QpidClient) Send(addr string, data []byte) error {
	// 创建一个发送者
	sender, err := c.amqp_session.NewSender(amqp.LinkTargetAddress(addr))
	if err != nil {
		return err
	}

	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithTimeout(ctx, 50 * time.Microsecond)

	// 发送消息
	message_object := amqp.NewMessage(data)
	err = sender.Send(ctx, message_object)
	if err != nil {
		return err
	}

	_ = message_object
	_ = sender.Close(ctx)
	cancel()
	return nil
}

// 接收消息
func (c *QpidClient) Recv(addr string) ([]byte, error) {

	return nil, nil
}
