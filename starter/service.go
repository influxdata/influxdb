package starter

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/pkger"
)

type Service struct {
	scripts []ScriptTemplate

	eventConsumer chan influxdb.Event
	tmplSVC       pkger.SVC
}

type ScriptTemplate struct {
	Template string `yaml:"template"`
}

func NewService(tmplSVC pkger.SVC, scripts ...ScriptTemplate) *Service {
	return &Service{
		scripts:       scripts,
		eventConsumer: make(chan influxdb.Event, 1), // nonblocking sends to this consumer
		tmplSVC:       tmplSVC,
	}
}

func (s *Service) EventConsumer() chan<- influxdb.Event {
	return s.eventConsumer
}

func (s *Service) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-s.eventConsumer:
			s.handleEvent(ctx, ev)
		}
	}
}

func (s *Service) handleEvent(ctx context.Context, ev influxdb.Event) {
	switch ev.Type {
	case influxdb.EventSetupComplete:
		for _, scr := range s.scripts {
			tmpl, err := pkger.ParseFromURL(scr.Template)
			if err != nil {
				fmt.Println("error parsing template: ", err.Error())
				continue
			}

			ctx := icontext.SetAuthorizer(ctx, &ev.Auth)
			_, err = s.tmplSVC.Apply(ctx, ev.Auth.OrgID, ev.Auth.UserID, pkger.ApplyWithPkg(tmpl))
			if err != nil {
				fmt.Println("error applying template: ", err)
			}
		}
	}
}
