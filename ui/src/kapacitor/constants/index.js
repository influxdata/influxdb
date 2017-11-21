import _ from 'lodash'

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
// DEFAULT_ALERTS are empty alert templates for handlers that don't exist in the kapacitor config
export const DEFAULT_ALERTS = [
  {
    type: 'post',
    enabled: true,
    url: '',
    headers: '',
  },
  {type: 'tcp', enabled: true, address: ''},
  {type: 'exec', enabled: true, command: []},
  {type: 'log', enabled: true, filePath: ''},
]

// ALERTS_FROM_CONFIG the array of fields to accept from Kapacitor Config
export const ALERTS_FROM_CONFIG = {
  alerta: ['environment', 'origin', 'token'], // token = bool
  hipchat: ['url', 'room', 'token'], // token = bool
  opsgenie: ['api-key', 'teams', 'recipients'], // api-key = bool
  pagerduty: ['service-key'], // service-key = bool
  pushover: ['token', 'user-key'], // token = bool, user-key = bool
  sensu: ['addr', 'source'],
  slack: ['url', 'channel'], // url = bool
  smtp: ['from', 'host', 'password', 'port', 'username'], // password = bool
  talk: ['url', 'author_name'], // url = bool
  telegram: [
    'token',
    'chat-id',
    'parse-mode',
    'disable-web-page-preview',
    'disable-notification',
  ], // token = bool
  victorops: ['api-key', 'routing-key'], // api-key = bool
  // snmpTrap: ['trapOid', 'data'], // [oid/type/value]
  // influxdb:[],
  // mqtt:[]
}

export const CONFIG_TO_RULE = {
  alerta: {},
  hipchat: {},
  opsgenie: {},
  pagerduty: {'service-key': 'serviceKey'},
  pushover: {'user-key': 'userKey'},
  sensu: {},
  slack: {},
  // smtp: 'email', // this won't work.
  smtp: {},
  talk: {},
  telegram: {
    'chat-id': 'chatId',
    'parse-mode': 'parseMode',
    'disable-web-page-preview': 'disableWebPagePreview',
    'disable-notification': 'disableNotification',
  },
  victorops: {'routing-key': 'routingKey'},
  // snmpTrap: {},
  // influxd: {},
  // mqtt: {}
}

// ALERTS_TO_RULE returns array of fields that may be updated for each alert on rule.
export const ALERTS_TO_RULE = {
  alerta: [
    'token',
    'resource',
    'event',
    'environment',
    'group',
    'value',
    'origin',
    'service',
  ],
  hipchat: ['room', 'token'],
  opsgenie: ['teams', 'recipients'],
  pagerduty: ['serviceKey'],
  pushover: ['userKey', 'device', 'title', 'url', 'urlTitle', 'sound'],
  sensu: ['source', 'handlers'],
  slack: ['channel', 'username', 'iconEmoji'],
  email: ['to'],
  snmpTrap: ['trapOid', 'data'], // [oid/type/value]
  talk: [],
  telegram: [
    'chatId',
    'parseMode',
    'disableWebPagePreview',
    'disableNotification',
  ],
  victorops: ['routingKey'],
  post: ['url', 'headers', 'captureResponse'],
  tcp: ['address'],
  exec: ['command'],
  log: ['filePath'],
}

export const RULE_ALERT_OPTIONS = {
  http: {
    args: {
      label: 'URL:',
      placeholder: 'Ex: http://example.com/api/alert',
    },
    // properties: [
    //   {name: 'endpoint', label: 'Endpoint:', placeholder: 'Endpoint'},
    //   {name: 'header', label: 'Headers:', placeholder: 'Headers (Delimited)'}, // TODO: determine how to delimit
    // ],
  },
  tcp: {
    args: {
      label: 'Address:',
      placeholder: 'Ex: exampleendpoint.com:5678',
    },
  },
  exec: {
    args: {
      label: 'Command (Arguments separated by Spaces):',
      placeholder: 'Ex: woogie boogie',
    },
  },
  log: {
    args: {
      label: 'File:',
      placeholder: 'Ex: /tmp/alerts.log',
    },
  },
  alerta: {
    args: {
      label: 'Paste Alerta TICKscript:', // TODO: remove this
      placeholder: 'alerta()',
    },
    // properties: [
    //   {name: 'token', label: 'Token:', placeholder: 'Token'},
    //   {name: 'resource', label: 'Resource:', placeholder: 'Resource'},
    //   {name: 'event', label: 'Event:', placeholder: 'Event'},
    //   {name: 'environment', label: 'Environment:', placeholder: 'Environment'},
    //   {name: 'group', label: 'Group:', placeholder: 'Group'},
    //   {name: 'value', label: 'Value:', placeholder: 'Value'},
    //   {name: 'origin', label: 'Origin:', placeholder: 'Origin'},
    //   {name: 'services', label: 'Services:', placeholder: 'Services'}, // TODO: what format?
    // ],
  },
  hipchat: {
    properties: [
      {name: 'room', label: 'Room:', placeholder: 'happy_place'},
      {name: 'token', label: 'Token:', placeholder: 'a_gilded_token'},
    ],
  },
  opsgenie: {
    // properties: [
    //   {name: 'recipients', label: 'Recipients:', placeholder: 'happy_place'}, // TODO: what format?
    //   {name: 'teams', label: 'Teams:', placeholder: 'blue,yellow,maroon'}, // TODO: what format?
    // ],
  },
  pagerduty: {
    properties: [
      {
        name: 'serviceKey',
        label: 'Service Key:',
        placeholder: 'one_rad_key',
      },
    ],
  },
  pushover: {
    properties: [
      {
        name: 'device',
        label: 'Device:',
        placeholder: 'dv1,dv2 (Comma Separated)', // TODO: do these need to be parsed before sent?
      },
      {name: 'title', label: 'Title:', placeholder: 'Important Message'},
      {name: 'URL', label: 'URL:', placeholder: 'https://influxdata.com'},
      {name: 'URLTitle', label: 'URL Title:', placeholder: 'InfluxData'},
      {name: 'sound', label: 'Sound:', placeholder: 'alien'},
    ],
  },
  sensu: {
    // TODO: apparently no args or properties, according to kapacitor/ast.go ?
  },
  slack: {
    properties: [
      {name: 'channel', label: 'Channel:', placeholder: '#cubeoctohedron'},
      {name: 'iconEmoji', label: 'Emoji:', placeholder: ':cubeapple:'},
      {name: 'username', label: 'Username:', placeholder: 'pineapple'},
    ],
  },
  smtp: {
    args: {
      label: 'Email Addresses (Separated by Spaces):',
      placeholder:
        'Ex: benedict@domain.com delaney@domain.com susan@domain.com',
    },
    details: {placeholder: 'Email body text goes here'},
  },
  talk: {},
  telegram: {
    properties: [
      {name: 'chatId', label: 'Chat ID:', placeholder: 'xxxxxxxxx'},
      {name: 'parseMode', label: 'Emoji:', placeholder: 'Markdown'},
      // {
      //   name: 'disableWebPagePreview',
      //   label: 'Disable Web Page Preview:',
      //   placeholder: 'true', // TODO: format to bool
      // },
      // {
      //   name: 'disableNotification',
      //   label: 'Disable Notification:',
      //   placeholder: 'false', // TODO: format to bool
      // },
    ],
  },
  victorops: {
    properties: [
      {name: 'routingKey', label: 'Channel:', placeholder: 'team_rocket'},
    ],
  },
}

export const ALERT_NODES_ACCESSORS = {
  http: rule => _.get(rule, 'alertNodes[0].args[0]', ''),
  tcp: rule => _.get(rule, 'alertNodes[0].args[0]', ''),
  exec: rule => _.get(rule, 'alertNodes[0].args', []).join(' '),
  log: rule => _.get(rule, 'alertNodes[0].args[0]', ''),
  smtp: rule => _.get(rule, 'alertNodes[0].args', []).join(' '),
  alerta: rule =>
    _.get(rule, 'alertNodes[0].properties', [])
      .reduce(
        (strs, item) => {
          strs.push(`${item.name}('${item.args.join(' ')}')`)
          return strs
        },
        ['alerta()']
      )
      .join('.'),
}
