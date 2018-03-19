import {QueryConfig} from "./"

export interface Kapacitor {
  id?: string
  url: string
  name: string
  username?: string
  password?: string
  active: boolean
  insecureSkipVerify: boolean
  links: {
    self: string
  }
}

export interface AlertRule {
  id?: string
  tickscript: TICKScript
  query: QueryConfig
  every: string
  alertNodes: AlertNodes
  message: string
  details: string
  trigger: string
  values: TriggerValues
  name: string
  type: string
  dbrps: DBRP[]
  status: string
  executing: boolean
  error: string
  created: string
  modified: string
  "last-enabled"?: string
}

type TICKScript = string

// AlertNodes defines all possible kapacitor interactions with an alert.
type AlertNodes = {
  stateChangesOnly: boolean
  useFlapping: boolean
  post: Post[]
  tcp: TCP[]
  email: Email[]
  exec: Exec[]
  log: Log[]
  victorOps: VictorOps[]
  pagerDuty: PagerDuty[]
  pushover: Pushover[]
  sensu: Sensu[]
  slack: Slack[]
  telegram: Telegram[]
  hipChat: HipChat[]
  alerta: Alerta[]
  opsGenie: OpsGenie[]
  talk: Talk[]
}

type Headers = {
  [key: string]: string
}

// Post will POST alerts to a destination URL
type Post = {
  url: string
  headers: Headers
}

// Log sends the output of the alert to a file
type Log = {
  filePath: string
}

// Alerta sends the output of the alert to an alerta service
type Alerta = {
  token: string
  resource: string
  event: string
  environment: string
  group: string
  value: string
  origin: string
  service: string[]
}

// Exec executes a shell command on an alert
type Exec = {
  command: string[]
}

// TCP sends the alert to the address
type TCP = {
  address: string
}

// Email sends the alert to a list of email addresses
type Email = {
  to: string[]
}

// VictorOps sends alerts to the victorops.com service
type VictorOps = {
  routingKey: string
}

// PagerDuty sends alerts to the pagerduty.com service
type PagerDuty = {
  serviceKey: string
}

// HipChat sends alerts to stride.com
type HipChat = {
  room: string
  token: string
}

// Sensu sends alerts to sensu or sensuapp.org
type Sensu = {
  source: string
  handlers: string[]
}

// Pushover sends alerts to pushover.net
type Pushover = {
  // UserKey is the User/Group key of your user (or you), viewable when logged
  // into the Pushover dashboard. Often referred to as USER_KEY
  // in the Pushover documentation.
  userKey: string

  // Device is the users device name to send message directly to that device,
  // rather than all of a user's devices (multiple device names may
  // be separated by a comma)
  device: string

  // Title is your message's title, otherwise your apps name is used
  title: string

  // URL is a supplementary URL to show with your message
  url: string

  // URLTitle is a title for your supplementary URL, otherwise just URL is shown
  urlTitle: string

  // Sound is the name of one of the sounds supported by the device clients to override
  // the user's default sound choice
  sound: string
}

// Slack sends alerts to a slack.com channel
type Slack = {
  channel: string
  username: string
  iconEmoji: string
}

// Telegram sends alerts to telegram.org
type Telegram = {
  chatId: string
  parseMode: string
  disableWebPagePreview: boolean
  disableNotification: boolean
}

// OpsGenie sends alerts to opsgenie.com
type OpsGenie = {
  teams: string[]
  recipients: string[]
}

// Talk sends alerts to Jane Talk (https://jianliao.com/site)
type Talk = {}

// TriggerValues specifies the alerting logic for a specific trigger type
type TriggerValues = {
  change?: string
  period?: string
  shift?: string
  operator?: string
  value?: string
  rangeValue: string
}

// DBRP represents a database and retention policy for a time series source
type DBRP = {
  db: string
  rp: string
}
