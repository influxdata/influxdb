export const defaultRuleConfigs = {
  deadman: {
    period: '10m',
  },
  relative: {
    change: 'change',
    shift: '1m',
    operator: 'greater than',
    value: '',
  },
  threshold: {
    operator: 'greater than',
    value: '',
    rangeValue: '',
    relation: 'once',
    percentile: '90',
  },
}

export const defaultEveryFrequency = '30s'

// constants taken from https://github.com/influxdata/chronograf/blob/870dbc72d1a8b784eaacad5eeea79fc54968b656/kapacitor/operators.go#L13
export const EQUAL_TO = 'equal to'
export const LESS_THAN = 'less than'
export const GREATER_THAN = 'greater than'
export const NOT_EQUAL_TO = 'not equal to'
export const INSIDE_RANGE = 'inside range'
export const OUTSIDE_RANGE = 'outside range'
export const EQUAL_TO_OR_GREATER_THAN = 'equal to or greater'
export const EQUAL_TO_OR_LESS_THAN = 'equal to or less than'

export const THRESHOLD_OPERATORS = [
  GREATER_THAN,
  EQUAL_TO_OR_GREATER_THAN,
  EQUAL_TO_OR_LESS_THAN,
  LESS_THAN,
  EQUAL_TO,
  NOT_EQUAL_TO,
  INSIDE_RANGE,
  OUTSIDE_RANGE,
]

export const RELATIVE_OPERATORS = [
  GREATER_THAN,
  EQUAL_TO_OR_GREATER_THAN,
  EQUAL_TO_OR_LESS_THAN,
  LESS_THAN,
  EQUAL_TO,
  NOT_EQUAL_TO,
]

// export const RELATIONS = ['once', 'more than ', 'less than'];
export const PERIODS = ['1m', '5m', '10m', '30m', '1h', '2h', '24h']
export const CHANGES = ['change', '% change']
export const SHIFTS = ['1m', '5m', '10m', '30m', '1h', '2h', '24h']

export const DEFAULT_RULE_ID = 'DEFAULT_RULE_ID'

export const RULE_MESSAGE_TEMPLATES = {
  id: {label: '{{.ID}}', text: 'The ID of the alert'},
  name: {label: '{{.Name}}', text: 'Measurement name'},
  taskName: {label: '{{.TaskName}}', text: 'The name of the task'},
  group: {
    label: '{{.Group}}',
    text:
      'Concatenation of all group-by tags of the form <code>&#91;key=value,&#93;+</code>. If no groupBy is performed equal to literal &quot;nil&quot;',
  },
  tags: {
    label: '{{.Tags}}',
    text:
      'Map of tags. Use <code>&#123;&#123; index .Tags &quot;key&quot; &#125;&#125;</code> to get a specific tag value',
  },
  level: {
    label: '{{.Level}}',
    text:
      'Alert Level, one of: <code>INFO</code><code>WARNING</code><code>CRITICAL</code>',
  },
  fields: {
    label: '{{ index .Fields "value" }}',
    text:
      'Map of fields. Use <code>&#123;&#123; index .Fields &quot;key&quot; &#125;&#125;</code> to get a specific field value',
  },
  time: {
    label: '{{.Time}}',
    text: 'The time of the point that triggered the event',
  },
}
// DEFAULT_HANDLERS are empty alert templates for handlers that don't exist in the kapacitor config
export const DEFAULT_HANDLERS = [
  {
    type: 'post',
    enabled: true,
    url: '',
    headers: {},
    headerKey: '',
    headerValue: '',
  },
  {type: 'tcp', enabled: true, address: ''},
  {type: 'exec', enabled: true, command: []},
  {type: 'log', enabled: true, filePath: ''},
]

export const MAP_KEYS_FROM_CONFIG = {
  hipchat: 'hipChat',
  opsgenie: 'opsGenie',
  opsgenie2: 'opsGenie2',
  pagerduty: 'pagerDuty',
  pagerduty2: 'pagerDuty2',
  smtp: 'email',
  victorops: 'victorOps',
}

// ALERTS_FROM_CONFIG the array of fields to accept from Kapacitor Config
export const ALERTS_FROM_CONFIG = {
  alerta: ['environment', 'origin', 'token'], // token = bool
  hipChat: ['url', 'room', 'token'], // token = bool
  kafka: [],
  opsGenie: ['api-key', 'teams', 'recipients'], // api-key = bool
  opsGenie2: ['api-key', 'teams', 'recipients'], // api-key = bool
  pagerDuty: ['service-key'], // service-key = bool
  pagerDuty2: ['routing-key'], // routing-key = bool
  pushover: ['token', 'user-key'], // token = bool, user-key = bool
  sensu: ['addr', 'source'],
  slack: ['url', 'channel'], // url = bool
  email: ['from', 'host', 'password', 'port', 'username'], // password = bool
  talk: ['url', 'author_name'], // url = bool
  telegram: [
    'token',
    'chat-id',
    'parse-mode',
    'disable-web-page-preview',
    'disable-notification',
  ], // token = bool
  victorOps: ['api-key', 'routing-key'], // api-key = bool
  // snmpTrap: ['trapOid', 'data'], // [oid/type/value]
  // influxdb:[],
  // mqtt:[]
}

export const MAP_FIELD_KEYS_FROM_CONFIG = {
  alerta: {},
  hipChat: {},
  opsGenie: {},
  opsGenie2: {},
  pagerDuty: {'service-key': 'serviceKey'},
  pagerDuty2: {'routing-key': 'routingKey'},
  pushover: {'user-key': 'userKey'},
  sensu: {},
  slack: {},
  email: {},
  talk: {},
  telegram: {
    'chat-id': 'chatId',
    'parse-mode': 'parseMode',
    'disable-web-page-preview': 'disableWebPagePreview',
    'disable-notification': 'disableNotification',
  },
  victorOps: {'routing-key': 'routingKey'},
  // snmpTrap: {},
  // influxd: {},
  // mqtt: {}
}

// HANDLERS_TO_RULE returns array of fields that may be updated for each alert on rule.
export const HANDLERS_TO_RULE = {
  alerta: [
    'resource',
    'event',
    'environment',
    'group',
    'value',
    'origin',
    'service',
  ],
  hipChat: ['room'],
  kafka: ['cluster', 'topic', 'template'],
  opsGenie: ['teams', 'recipients'],
  opsGenie2: ['teams', 'recipients'],
  pagerDuty: [],
  pagerDuty2: [],
  pushover: ['device', 'title', 'sound', 'url', 'urlTitle'],
  sensu: ['source', 'handlers'],
  slack: ['channel', 'username', 'iconEmoji'],
  email: ['to'],
  talk: [],
  telegram: [
    'chatId',
    'parseMode',
    'disableWebPagePreview',
    'disableNotification',
  ],
  victorOps: ['routingKey'],
  post: ['url', 'headers', 'captureResponse'],
  tcp: ['address'],
  exec: ['command'],
  log: ['filePath'],
  // snmpTrap: ['trapOid', 'data'], // [oid/type/value]
}
