package chronograf

// AlertHandlers defines all possible kapacitor interactions with an alert.
type AlertHandlers struct {
	IsStateChangesOnly bool         `json:"stateChangesOnly"` // IsStateChangesOnly will only send alerts on state changes.
	UseFlapping        bool         `json:"useFlapping"`      // UseFlapping enables flapping detection. Flapping occurs when a service or host changes state too frequently, resulting in a storm of problem and recovery notification
	Message            string       `json:"message"`          // Message is a template for constructing a meaningful message for the alert.
	Details            string       `json:"details"`          // Details is a template for constructing a detailed HTML message for the alert.
	Posts              []*Post      `json:"post"`             // HTTPPost  will post the JSON alert data to the specified URLs.
	TCPs               []*TCP       `json:"tcp"`              // TCP  will send the JSON alert data to the specified endpoint via TCP.
	Email              []*Email     `json:"email"`            // Email  will send alert data to the specified emails.
	Exec               []*Exec      `json:"exec"`             // Exec  will run shell commandss when an alert triggers
	Log                []*Log       `json:"log"`              // Log  will log JSON alert data to files in JSON lines format.
	VictorOps          []*VictorOps `json:"victorOps"`        // VictorOps  will send alert to all VictorOps  .
	PagerDuty          []*PagerDuty `json:"pagerDuty"`        // PagerDuty  will send alert to all PagerDuty  .
	Pushover           []*Pushover  `json:"pushover"`         // Pushover  will send alert to all Pushover  .
	Sensu              []*Sensu     `json:"sensu"`            // Sensu  will send alert to all Sensu  .
	Slack              []*Slack     `json:"slack"`            // Slack  will send alert to Slack  .
	Telegram           []*Telegram  `json:"telegram"`         // Telegram  will send alert to all Telegram  .
	HipChat            []*HipChat   `json:"hipChat"`          // HipChat  will send alert to all HipChat  .
	Alerta             []*Alerta    `json:"alerta"`           // Alerta  will send alert to all Alerta  .
	OpsGenie           []*OpsGenie  `json:"opsGenie"`         // OpsGenie  will send alert to all OpsGenie  .
	Talk               []*Talk      `json:"talk"`             // Talk  will send alert to all Talk  .
}

// Post will POST alerts to a destination URL
type Post struct {
	URL     string            `json:"url"` // URL is the destination of the POST.
	Headers map[string]string `json:"headers"`
}

// Log sends the output of the alert to a file
type Log struct {
	FilePath string `json:"filePath"` // Absolute path the the log file. // It will be created if it does not exist.
}

// Alerta sends the output of the alert to an alerta service
type Alerta struct {
	Token       string   `json:"token"`       // Token is the authentication token that overrides the global configuration.
	Resource    string   `json:"resource"`    // Resource under alarm, deliberately not host-centric
	Event       string   `json:"event"`       // Event is the event name eg. NodeDown, QUEUE:LENGTH:EXCEEDED
	Environment string   `json:"environment"` // Environment is the effected environment; used to namespace the resource
	Group       string   `json:"group"`       // Group is an event group used to group events of similar type
	Value       string   `json:"value"`       // Value is the event value eg. 100%, Down, PingFail, 55ms, ORA-1664
	Origin      string   `json:"origin"`      // Origin is the name of monitoring component that generated the alert
	Service     []string `json:"service"`     // Service is the list of affected services
}

// Exec executes a shell command on an alert
type Exec struct {
	Command []string `json:"command"` // Command is the space separated command and args to execute.
}

// TCP sends the alert to the address
type TCP struct {
	Address string `json:"address"` // Endpoint is the Address and port to send the alert
}

// Email sends the alert to a list of email addresses
type Email struct {
	ToList []string `json:"to"` // ToList is the list of email recipients.
}

// VictorOps sends alerts to the victorops.com service
type VictorOps struct {
	RoutingKey string `json:"routingKey"` // RoutingKey is what is used to map the alert to a team
}

// PagerDuty sends alerts to the pagerduty.com service
type PagerDuty struct {
	ServiceKey string `json:"serviceKey"` // ServiceKey is the GUID of one of the "Generic API" integrations
}

// HipChat sends alerts to stride.com
type HipChat struct {
	Room  string `json:"room"`  // Room is the HipChat room to post messages.
	Token string `json:"token"` // Token is the HipChat authentication token.
}

// Sensu sends alerts to sensu or sensuapp.org
type Sensu struct {
	Source   string   `json:"source"`   // Source is the check source, used to create a proxy client for an external resource
	Handlers []string `json:"handlers"` // Handlers are Sensu event handlers are for taking action on events
}

// Pushover sends alerts to pushover.net
type Pushover struct {
	// UserKey is the User/Group key of your user (or you), viewable when logged
	// into the Pushover dashboard. Often referred to as USER_KEY
	// in the Pushover documentation.
	UserKey string `json:"userKey"`

	// Device is the users device name to send message directly to that device,
	// rather than all of a user's devices (multiple device names may
	// be separated by a comma)
	Device string `json:"device"`

	// Title is your message's title, otherwise your apps name is used
	Title string `json:"title"`

	// URL is a supplementary URL to show with your message
	URL string `json:"url"`

	// URLTitle is a title for your supplementary URL, otherwise just URL is shown
	URLTitle string `json:"urlTitle"`

	// Sound is the name of one of the sounds supported by the device clients to override
	// the user's default sound choice
	Sound string `json:"sound"`
}

// Slack sends alerts to a slack.com channel
type Slack struct {
	Channel   string `json:"channel"`   // Slack channel in which to post messages.
	Username  string `json:"username"`  // Username of the Slack bot.
	IconEmoji string `json:"iconEmoji"` // IconEmoji is an emoji name surrounded in ':' characters; The emoji image will replace the normal user icon for the slack bot.
}

// Telegram sends alerts to telegram.org
type Telegram struct {
	ChatID                string `json:"chatId"`                // ChatID is the Telegram user/group ID to post messages to.
	ParseMode             string `json:"parseMode"`             // ParseMode tells telegram how to render the message (Markdown or HTML)
	DisableWebPagePreview bool   `json:"disableWebPagePreview"` // IsDisableWebPagePreview will disables link previews in alert messages.
	DisableNotification   bool   `json:"disableNotification"`   // IsDisableNotification will disables notifications on iOS devices and disables sounds on Android devices. Android users continue to receive notifications.
}

// OpsGenie sends alerts to opsgenie.com
type OpsGenie struct {
	Teams      []string `json:"teams"`      // Teams that the alert will be routed to send notifications
	Recipients []string `json:"recipients"` // Recipients can be a single user, group, escalation, or schedule (https://docs.opsgenie.com/docs/alert-recipients-and-teams)
}

// Talk sends alerts to Jane Talk (https://jianliao.com/site)
type Talk struct{}
