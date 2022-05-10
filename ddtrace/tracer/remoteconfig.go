package tracer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer/remoteconfigpb"
)

type remoteConfigClient struct {
	stop chan struct{} // closing this channel triggers shutdown
}

func NewRemoteConfigClient() *remoteConfigClient {
	return &remoteConfigClient{
		stop: make(chan struct{}),
	}
}

func (c *remoteConfigClient) Start() {
	fmt.Println("Starting remote config client")
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	select {
	case <-c.stop:
		return
	case <-ticker.C:
		c.updateState()
	}
}

func (c *remoteConfigClient) Stop() {
	close(c.stop)
}

var client http.Client

func (c *remoteConfigClient) updateState() {
	fmt.Println("doing an rc update")
	data := buildRequest()
	req, err := http.NewRequest("GET", "http://localhost:8162/v0.7/config", &data)
	if err != nil {
		log.Println(err)
		return
	}
	_, err = client.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
}

func buildRequest() bytes.Buffer {
	req := remoteconfigpb.ClientGetConfigsRequest{
		Client: &remoteconfigpb.Client{
			State: &remoteconfigpb.ClientState{
				RootVersion:    0,
				TargetsVersion: 0,
				HasError:       false,
			},
			Id:       "test-rc-go-client",
			Products: []string{"LIVE_DEBUGGING"},
			IsTracer: true,
			ClientTracer: &remoteconfigpb.ClientTracer{
				RuntimeId:     "myruntimeID",
				Language:      "go",
				TracerVersion: "myversion",
				Service:       "livedebugging",
				Env:           "myenv",
				AppVersion:    "myappVersion",
			},
			IsAgent: false,
		},
	}

	var b bytes.Buffer

	err := json.NewEncoder(&b).Encode(&req)
	if err != nil {
		panic(err)
	}

	return b
}
