package nsqt

import (
	"github.com/bitly/nsq/nsq"
	"testing"
)

type TestHander struct {
	t *testing.T
	q *nsq.Reader

	handleMessage chan *nsq.Message
}

func (handler TestHander) HandleMessage(message *nsq.Message) error {
	handler.handleMessage <- message
	return nil
}

func (handler TestHander) Stop() {
	handler.q.Stop()
	<-handler.q.ExitChan
}

func TestClientCanSendSomething(t *testing.T) {
	qClient1, err := createTestReader(t)
	qClient2, err := createTestReader(t)
	defer qClient1.Stop()
	defer qClient2.Stop()

	if err != nil {
		t.Fatalf("error while creating client: %v", err.Error())
	}
}

func stopClient(handler TestHander) {
	handler.q.Stop()
	<-handler.q.ExitChan
}

func createTestReader(t *testing.T) (*TestHander, error) {
	addr := "127.0.0.1:4150"
	topicName := "reader_test"
	q, _ := nsq.NewReader(topicName, "ch")
	q.VerboseLogging = true

	handler := &TestHander{
		t:             t,
		q:             q,
		handleMessage: make(chan *nsq.Message),
	}

	q.AddHandler(handler)

	err := q.ConnectToNSQ(addr)
	if err != nil {
		return nil, err
	}

	return handler, nil
}
