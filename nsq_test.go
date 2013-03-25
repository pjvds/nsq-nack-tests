package nsqt

import (
	"bytes"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"net/http"
	"testing"
	"time"
)

const (
	test_topic = "test_topic"
)

type CallbackHandler struct {
	t *testing.T
	q *nsq.Reader

	handleMsg func(message *nsq.Message, finish chan *nsq.FinishedMessage)
}

func (handler CallbackHandler) HandleMessage(message *nsq.Message, finish chan *nsq.FinishedMessage) {
	handler.handleMsg(message, finish)
}

func TestClientCanSendSomething(t *testing.T) {
	received := make([]*nsq.Message, 0)

	qClient1 := createTestReaderOrFail(t, func(msg *nsq.Message, finish chan *nsq.FinishedMessage) {
		t.Logf("c1 received %s", msg)

		received = append(received, msg)
	})

	SendMessage(t, 4151, test_topic, "put", []byte(`{"msg":"single"}`))

	time.Sleep(6 * 2 * time.Second)

	qClient1.q.Stop()

	for i := 0; i < len(received); i++ {
		t.Logf("msg %v has attemp %v", received[i].Id, received[i].Attempts)
	}
}

func SendMessage(t *testing.T, port int, topic string, method string, body []byte) {
	httpclient := &http.Client{}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/%s?topic=%s", port, method, topic)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
	resp, err := httpclient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	resp.Body.Close()
}

func createTestReaderOrFail(t *testing.T, handleMsg func(message *nsq.Message, finish chan *nsq.FinishedMessage)) *CallbackHandler {
	addr := "127.0.0.1:4150"
	q, err := nsq.NewReader(test_topic, "ch")

	if err != nil {
		t.Fatalf("couldn't create reader: %v", err.Error())
	}

	q.VerboseLogging = true

	handler := &CallbackHandler{
		t:         t,
		q:         q,
		handleMsg: handleMsg,
	}

	q.AddAsyncHandler(handler)

	err = q.ConnectToNSQ(addr)
	if err != nil {
		t.Fatal(err.Error())
	}

	return handler
}
