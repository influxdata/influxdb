# How to use this package

## Make sure nats is running. Both publisher and subscriber are open

```go
// NATS streaming server
m.natsServer = nats.NewServer(nats.Config{FilestoreDir: m.natsPath})
if err := m.natsServer.Open(); err != nil {
    m.logger.Error("Failed to start nats streaming server", zap.Error(err))
    return err
}

publisher := nats.NewAsyncPublisher("nats-publisher")
if err := publisher.Open(); err != nil {
    m.logger.Error("Failed to connect to streaming server", zap.Error(err))
    return err
}

subscriber := nats.NewQueueSubscriber("nats-subscriber")
if err := subscriber.Open(); err != nil {
    m.logger.Error("Failed to connect to streaming server", zap.Error(err))
    return err
}
```

## Make sure the scraperTargetStorageService is accessible

```go
scraperTargetSvc influxdb.ScraperTargetStoreService = m.boltClient
```

## Setup recorder, Make sure subscriber subscribes use the correct recorder with the correct write service

```go
recorder := gather.PlatformWriter{
    Timeout: time.Millisecond * 30,
    Writer: writer,
}
subscriber.Subscribe(MetricsSubject, "", &RecorderHandler{
    Logger:   logger,
    Recorder: recorder,
})
```

## Start the scheduler

```go
scraperScheduler, err := gather.NewScheduler(10, m.logger, scraperTargetSvc, publisher, subscriber, 0, 0)
if err != nil {
    m.logger.Error("Failed to create scraper subscriber", zap.Error(err))
    return err
}
```