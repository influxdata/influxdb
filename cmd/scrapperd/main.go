package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/gather"
	"github.com/influxdata/platform/nats"
)

const (
	subjectParse = "sub_parse"
	subjectStore = "sub_store"
)

func main() {
	logger := new(log.Logger)

	msg, influxURL := parseFlags()
	publisher := initNats(logger, influxURL)

	go scheduleGetMetrics(msg, publisher)

	<-make(chan struct{})
}

func parseFlags() (msg gather.ScrapperRequest, influxURL []string) {
	orgIDstr := flag.String("orgID", "", "the organization id")
	bucketIDstr := flag.String("bucketID", "", "the bucket id")
	hostStr := flag.String("pHost", "", "the promethus host")
	influxStr := flag.String("influxURLs", "", "comma seperated urls")
	flag.Parse()

	orgID, err := platform.IDFromString(*orgIDstr)
	if err != nil || orgID == nil || orgID.String() == "" {
		log.Fatal("Invalid orgID")
	}

	bucketID, err := platform.IDFromString(*bucketIDstr)
	if err != nil || bucketID == nil || bucketID.String() == "" {
		log.Fatal("Invalid bucketID")
	}

	if *hostStr == "" {
		log.Fatal("Invalid host")
	}
	pURL := *hostStr + "/metrics"

	influxURL = strings.Split(*influxStr, ",")
	if len(influxURL) == 0 {
		influxURL = []string{
			"http://localhost:8086",
		}
	}
	msg = gather.ScrapperRequest{
		HostURL:  pURL,
		OrgID:    *orgID,
		BucketID: *bucketID,
	}
	return msg, influxURL
}

func initNats(logger *log.Logger, influxURL []string) nats.Publisher {
	server := nats.NewServer(nats.Config{
		FilestoreDir: ".",
	})

	if err := server.Open(); err != nil {
		log.Fatalf("nats server fatal err %v", err)
	}

	subscriber := nats.NewQueueSubscriber("nats-subscriber")
	if err := subscriber.Open(); err != nil {
		log.Fatalf("nats parse subscriber open issue %v", err)
	}

	publisher := nats.NewAsyncPublisher("nats-publisher")
	if err := publisher.Open(); err != nil {
		log.Fatalf("nats parse publisher open issue %v", err)
	}

	if err := subscriber.Subscribe(subjectParse, "", &gather.ScrapperHandler{
		Scrapper: gather.NewPrometheusScrapper(
			gather.NewNatsStorage(
				gather.NewInfluxStorage(influxURL),
				subjectStore,
				logger,
				publisher,
				subscriber,
			),
		),
		Logger: logger,
	}); err != nil {
		log.Fatalf("nats subscribe error")
	}
	return publisher
}

// scheduleGetMetrics will send the scraperRequest to publisher
// for every 2 second
func scheduleGetMetrics(msg gather.ScrapperRequest, publisher nats.Publisher) {
	buf := new(bytes.Buffer)
	b, _ := json.Marshal(msg)
	buf.Write(b)
	publisher.Publish(subjectParse, buf)

	time.Sleep(2 * time.Second)
	scheduleGetMetrics(msg, publisher)
}
