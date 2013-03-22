package nsqt

import (
	"github.com/bitly/nsq/nsq"
	"testing"
)

type TestHander struct {
	t      *testing.T
	reader *nsq.Reader
}

func (handler TestHander) HandleMessage(message *nsq.Message) error {
	handler.t.Logf("handling message: %s", message)
	return nil
}

func TestClientCanSendSomething(t *testing.T) {
	addr := "127.0.0.1:4150"
	topicName := "reader_test"
	q, _ := nsq.NewReader(topicName, "ch")
	q.VerboseLogging = true

	handler := TestHander{
		t:      t,
		reader: q,
	}

	q.AddHandler(handler)

	err := q.ConnectToNSQ(addr)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

// func setup() {
// 	nsq.Register("test", "chan")//

// }
