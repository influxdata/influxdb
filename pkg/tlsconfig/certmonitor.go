package tlsconfig

import (
	"cmp"
	"maps"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	DefaultTriggerDelay = 5 * time.Second
)

// TLSCertMonitor implements periodic certificate monitoring.
//
//	It avoids logging about a single certificate / key pair
//
// multiple times, as well as the number of goroutines required to monitor
// certificates. There should be a single TLSCertMonitor in an application
// that is shared amongst all TLSConfigManager objects.
type TLSCertMonitor struct {
	// log is the logger to use.
	log *zap.Logger

	// startOnce is used to start the background monitor goroutine one time.
	startOnce sync.Once

	// triggerDelay is a timer used to send for delayed issue warning triggers.
	// This allows multiple certificate loads to be batched together when
	// certificate loading is complete.
	triggerDelay *time.Timer

	// triggerResetCh is a channel used to reset triggerDelay. This is required
	// because go does not reliably allow calling Reset() on a timer whose
	// .C is actively being waited upon in a select statement.
	triggerResetCh chan time.Duration

	// checkIntervalResetCh is a channel used to reset the check interval timer.
	// This is required because go does not reliably allow calling Reset() on a
	// timer whose .C is actively being waited upon in a select statement. It
	// also necessary since the ticker is itself only visible in the monitor
	// goroutine.
	checkIntervalResetCh chan time.Duration

	// closeOnce is used to stop the background monitor goroutine one time only.
	closeOnce sync.Once

	// closeCh is used to trigger closing the monitor.
	closeCh chan struct{}

	// monitorStartWg can be used to detect if the monitor goroutine has started.
	monitorStartWg sync.WaitGroup

	// monitorStopWg can be used to detect if the monitor goroutine has ended.
	monitorStopWg sync.WaitGroup

	// mu protects all members below.
	mu sync.RWMutex

	// certLoaders is a set of TLSCertLoader objects whose certificates we monitor.
	// certLoaders handle registering and unregistering themselves.
	certLoaders map[*TLSCertLoader]struct{}

	// queuedCertLoaders is a list of TLSCertLoader objects which have queued themselves
	// for issue warning when the next requested warn trigger activates. It is essentially
	// a filter for which CertLoaders will have issues logged, so that only TLSCertLoader
	// objects with newly loaded certificates will have their issues logged. It does not
	// apply for the regularly scheduled issue warning.
	queuedCertLoaders []*TLSCertLoader

	// certExpirationAdvanced determines how long before a certificate expires a warning is issued.
	certExpirationAdvanced time.Duration

	// certCheckInterval determines the duration between each certificate check.
	certCheckInterval time.Duration

	// triggerDelayInterval is the interval after starting triggerDelay to fire
	// the after function.
	triggerDelayInterval time.Duration
}

// TLSCertMonitorOpt is an option for NewTLSCertMonitor.
type TLSCertMonitorOpt func(*TLSCertMonitor)

// WithMonitorCheckInterval sets the initial check interval for the monitor.
// It can be changed later with SetCheckInterval.
func WithMonitorCheckInterval(d time.Duration) TLSCertMonitorOpt {
	return func(m *TLSCertMonitor) {
		m.certCheckInterval = d
	}
}

// WithMonitorExpirationWarn sets the initial expiration warn time for the
// monitor. It can be changed later with SetExpirationAdvanced.
func WithMonitorExpirationAdvanced(d time.Duration) TLSCertMonitorOpt {
	return func(m *TLSCertMonitor) {
		m.certExpirationAdvanced = d
	}
}

// WithMonitorLogger sets the logger for the monitor. It can be changed later
// with SetLogger.
func WithMonitorLogger(log *zap.Logger) TLSCertMonitorOpt {
	return func(m *TLSCertMonitor) {
		m.log = log
	}
}

// WithMonitorTriggerDelay sets the initial trigger delay interval. This
//
//	can be changed later with SetTriggerDelay.
func WithMonitorTriggerDelay(d time.Duration) TLSCertMonitorOpt {
	return func(m *TLSCertMonitor) {
		m.triggerDelayInterval = d
	}
}

// NewTLSCertMonitor creates a new TLSCertMonitor and starts its worker
// goroutine.
func NewTLSCertMonitor(opts ...TLSCertMonitorOpt) *TLSCertMonitor {
	// chanDepth should be high enough we don't need to block during startup.
	// It shouldn't matter as long as the monitor is running, but it'll
	// prevent any small pauses.
	const chanDepth = 20

	m := &TLSCertMonitor{
		closeCh:              make(chan struct{}),
		triggerResetCh:       make(chan time.Duration, chanDepth),
		checkIntervalResetCh: make(chan time.Duration, chanDepth),

		certLoaders:          make(map[*TLSCertLoader]struct{}),
		triggerDelayInterval: DefaultTriggerDelay,
	}

	for _, o := range opts {
		o(m)
	}

	if m.log == nil {
		m.log = zap.NewNop()
	}

	if m.certCheckInterval <= 0 {
		m.certCheckInterval = DefaultCertificateCheckTime
	}
	if m.certExpirationAdvanced <= 0 {
		m.certExpirationAdvanced = DefaultExpirationWarnTime
	}

	// Setup up the after function for trigger delay, but don't let it
	// fire.
	m.triggerDelay = time.NewTimer(m.triggerDelayInterval)
	m.triggerDelay.Stop()

	// Increment start wait group so that WaitForMonitorStart won't
	// return immediately if Open hasn't been called yet.
	m.monitorStartWg.Add(1)
	m.monitorStopWg.Add(1)

	return m
}

// Open starts the certificate monitor background goroutine. The certificate
// monitor won't do anything until this is called. It is safe to call
// multiple times, but it will only start one monitor goroutine.
func (m *TLSCertMonitor) Open() error {
	m.startOnce.Do(func() {
		go m.monitorCerts(&m.monitorStartWg, &m.monitorStopWg)
	})
	return nil
}

// Close stops the certificate monitor background routine. After calling
// this, certificates will no longer be monitored. It is safe to call
// multiple times, but it will only stop the monitor goroutine once.
func (m *TLSCertMonitor) Close() error {
	m.closeOnce.Do(func() {
		close(m.closeCh)
	})
	return nil
}

// WaitForMonitorStop waits for the certificate monitor goroutine to stop.
// This is mainly useful for tests to avoid race conditions. This will block
// forever if Open is not called before Close.
func (m *TLSCertMonitor) WaitForMonitorStop() {
	m.monitorStopWg.Wait()
}

// WaitForMonitorStart will wait for the certificate monitor goroutine to
// start. This is mainly useful for tests to avoid race conditions.
func (m *TLSCertMonitor) WaitForMonitorStart() {
	m.monitorStartWg.Wait()
}

func (m *TLSCertMonitor) logger() *zap.Logger {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.log
}

func (m *TLSCertMonitor) SetLogger(log *zap.Logger) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = log
}

// isClosed returns true if the monitor has been closed.
func (m *TLSCertMonitor) isClosed() bool {
	select {
	case <-m.closeCh:
		return true
	default:
		return false
	}
}

// resetTrigger resets the triggerDelay timer to d.
func (m *TLSCertMonitor) resetTrigger(d time.Duration) {
	if !m.isClosed() {
		m.triggerResetCh <- d
	}
}

// resetCheckInterval resets the check interval ticker to d.
func (m *TLSCertMonitor) resetCheckInterval(d time.Duration) {
	if !m.isClosed() {
		m.checkIntervalResetCh <- d
	}
}

func (m *TLSCertMonitor) SetCheckInterval(checkInterval time.Duration) {
	m.mu.Lock()
	m.certCheckInterval = checkInterval
	m.mu.Unlock()

	m.resetCheckInterval(checkInterval)
}

func (m *TLSCertMonitor) checkInterval() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.certCheckInterval
}

func (m *TLSCertMonitor) SetExpirationAdvanced(expirationAdvanced time.Duration) {
	m.mu.Lock()
	triggerDelay := m.triggerDelayInterval
	m.certExpirationAdvanced = expirationAdvanced
	m.mu.Unlock()

	// Send on channel outside of lock to prevent deadlocks.
	m.resetTrigger(triggerDelay)
}

func (m *TLSCertMonitor) expirationAdvanced() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.certExpirationAdvanced
}

// SetTriggerDelay sets the trigger delay interval to d.
func (m *TLSCertMonitor) SetTriggerDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.triggerDelayInterval = d
}

// QueueWarnIssues allows a TLSCertLoader to queue itself for issue warning.
// When the warn issues trigger occurs, all queued certificate loggers will
// have their issues logged. This also sets / resets the delayed trigger
// timer so that when a reload occurs issue warning won't occur until
// have certificates have been reloaded.
func (m *TLSCertMonitor) QueueWarnIssues(cl *TLSCertLoader) {
	m.mu.Lock()
	triggerDelay := m.triggerDelayInterval
	m.queuedCertLoaders = append(m.queuedCertLoaders, cl)
	m.mu.Unlock()

	// Send on channel outside of lock to prevent deadlocks.
	m.resetTrigger(triggerDelay)
}

// takeQueuedCertLoaders returns and clears TLSCertLoader objects queued
// through QueueWarnIssues since the last takeQueuedCertLoaders call.
func (m *TLSCertMonitor) takeQueuedCertLoaders() []*TLSCertLoader {
	m.mu.Lock()
	defer m.mu.Unlock()

	cls := m.queuedCertLoaders
	m.queuedCertLoaders = nil
	return cls
}

// registerCertLoader adds cl to the set of TLSCertLoader objects
// to monitor.
func (m *TLSCertMonitor) registerCertLoader(cl *TLSCertLoader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.certLoaders[cl] = struct{}{}
	// Don't use logger() here to avoid a deadlock.
	m.log.Info("Registered certificate loader", zap.String(logUsageContext, cl.Usage()))
}

// unregisterCertLoader removes cl from the set of TLSCertLoader objects
// to monitor.
func (m *TLSCertMonitor) unregisterCertLoader(cl *TLSCertLoader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.certLoaders[cl]; ok {
		// Don't use logger() here to avoid a deadlock.
		m.log.Info("Unregistered certificate loader", zap.String(logUsageContext, cl.Usage()))
	}
	delete(m.certLoaders, cl)
}

// logIssues logs issues found with lc.
func (m *TLSCertMonitor) logIssues(log *zap.Logger, lc LoadedCertificate) {
	if log == nil || lc.IsEmpty() {
		return
	}

	log = lc.WithLogContext(log)
	if xc, err := lc.GetLeaf(); err == nil {
		xc.logIssues(log, m.expirationAdvanced())
	} else {
		log.Error("error logging certificate issues", zap.Error(err))
	}
}

// logIssues logs issues with a given certLoaders
// gatherCertLoaders gathers all registered TLSCertLoader objects and
// returns them as a slice.
func (m *TLSCertMonitor) gatherCertLoaders() []*TLSCertLoader {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Collect(maps.Keys(m.certLoaders))
}

// checkCerts checks all registered TLSCertLoader objects for warnings on
// their certificates. Duplicate certificates are merged to avoid overly
// verbose output.
func (m *TLSCertMonitor) checkCerts(filter []*TLSCertLoader) {
	// checkCerts does not lock m.mu itself, but it does call functions
	// which take read locks.

	// Get the list of all cert loaders tracked by this monitor.
	certLoaders := m.gatherCertLoaders()

	// certKey is the key to identify identical certificates. Not only
	// must the paths match, but also the certificate serial number.
	type certKey struct {
		cert, key, serial string
	}
	makeKey := func(lc LoadedCertificate) certKey {
		return certKey{
			cert:   lc.CertificatePath,
			key:    lc.KeyPath,
			serial: lc.Serial(),
		}
	}

	// Create a set containing all registered cert loaders along with
	// all those listed in filter. This allows logging warnings on
	// cert loaders that queued a warning check before they registered
	// themselves. This is mainly a race condition found in tests where
	// the trigger delay may be set to 0.
	allCertLoaders := make(map[*TLSCertLoader]struct{}, len(certLoaders)+len(filter))
	for _, cl := range append(certLoaders, filter...) {
		allCertLoaders[cl] = struct{}{}
	}

	// Collect all described usages of a unique key.
	usageListMap := make(map[certKey][]string)
	lcMap := make(map[certKey]LoadedCertificate)
	for cl := range allCertLoaders {
		loaded := cl.LoadedCertificate()
		if !loaded.IsEmpty() {
			key := makeKey(loaded)
			usageListMap[key] = append(usageListMap[key], cl.Usage())
			lcMap[key] = loaded
		}
	}

	// Log issues with unique keys, including the described usages in the
	// log messages. Sort unique keys so output is in predictable order.
	// This is also where we decide on the filter. If filter is empty, then
	// all keys in usageListMap are used for the keys. Otherwise, the filter
	// list is processed to removed duplicate LoadedCertificates and then sorted.
	var keys []certKey
	if len(filter) == 0 {
		keys = slices.Collect(maps.Keys(usageListMap))
	} else {
		uniqueFilterKeys := make(map[certKey]struct{})
		for _, cl := range filter {
			loaded := cl.LoadedCertificate()
			if !loaded.IsEmpty() {
				uniqueFilterKeys[makeKey(loaded)] = struct{}{}
			}
		}
		keys = slices.Collect(maps.Keys(uniqueFilterKeys))
	}
	slices.SortFunc(keys,
		func(a, b certKey) int {
			if c := cmp.Compare(a.cert, b.cert); c != 0 {
				return c
			}
			if c := cmp.Compare(a.key, b.key); c != 0 {
				return c
			}
			return cmp.Compare(a.serial, b.serial)
		})

	log := m.logger()
	for _, k := range keys {
		usageList := usageListMap[k]
		// Sort to usages to present then in a predictable order.
		slices.Sort(usageList)
		lc := lcMap[k]
		m.logIssues(log.With(zap.Strings(logUsagesContext, usageList)), lc)
	}
}

// monitor periodically logs certificate errors with the currently registered
// TLSCertLoader objects.
func (m *TLSCertMonitor) monitorCerts(startWg *sync.WaitGroup, stopWg *sync.WaitGroup) {
	m.log.Info("Starting TLS certificate monitor")

	ticker := time.NewTicker(m.checkInterval())
	defer ticker.Stop()

	if startWg != nil {
		startWg.Done()
	}

	for {
		select {
		case <-ticker.C:
			m.checkCerts(nil)

		case d := <-m.checkIntervalResetCh:
			ticker.Reset(d)

		case <-m.triggerDelay.C:
			m.checkCerts(m.takeQueuedCertLoaders())

		case d := <-m.triggerResetCh:
			m.triggerDelay.Reset(d)

		case <-m.closeCh:
			m.log.Info("Stopping TLS certificate monitor")
			ticker.Stop()
			m.triggerDelay.Stop()
			if stopWg != nil {
				stopWg.Done()
			}
			return
		}
	}
}
