package main

// listen - test/monitor notify/listen of postgres.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
)

func main() {
	flag.Parse()
	err := run(context.Background(), flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, dbname string) error {
	l := pq.NewListener(dbname, time.Second, 120*time.Second, monitor)
	defer l.Close()
	err := l.Listen("test")
	if err != nil {
		return err
	}
	log.Printf("start")
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			return fmt.Errorf("context done: %s", err)
		case n, ok := <-l.Notify:
			if !ok {
				return errors.New("notification channel closed")
			}
			if n == nil {
				log.Printf("nil notification")
				continue
			}
			if n.Channel != "test" {
				log.Printf("unknown channel: %s", n.Channel)
				continue
			}
			log.Printf("received: %s", n.Extra)
		}
	}
}

var evtyp2name = map[pq.ListenerEventType]string{
	pq.ListenerEventConnected:               "connected",
	pq.ListenerEventDisconnected:            "disconnected",
	pq.ListenerEventReconnected:             "reconnected",
	pq.ListenerEventConnectionAttemptFailed: "attempt_failed",
}

func monitor(ev pq.ListenerEventType, err error) {
	s, ok := evtyp2name[ev]
	if !ok {
		s = fmt.Sprintf("[unknown:%d]", ev)
	}
	log.Printf("monitor: %s event: %s", s, err)
}
