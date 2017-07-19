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

export const OPERATORS = [
  'greater than',
  'equal to or greater',
  'equal to or less than',
  'less than',
  'equal to',
  'not equal to',
  'inside range',
  'outside range',
]
// export const RELATIONS = ['once', 'more than ', 'less than'];
export const PERIODS = ['1m', '5m', '10m', '30m', '1h', '2h', '24h']
export const CHANGES = ['change', '% change']
export const SHIFTS = ['1m', '5m', '10m', '30m', '1h', '2h', '24h']
export const ALERTS = [
  'alerta',
  'hipchat',
  'opsgenie',
  'pagerduty',
  'pushover',
  'sensu',
  'slack',
  'smtp',
  'talk',
  'telegram',
  'victorops',
]

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

export const DEFAULT_ALERTS = ['http', 'tcp', 'exec', 'log']

export const RULE_ALERT_OPTIONS = {
  http: {
    args: {
      label: 'URL:',
      placeholder: 'Ex: http://example.com/api/alert',
    },
  },
  tcp: {
    args: {
      label: 'Address:',
      placeholder: 'Ex: exampleendpoint.com:5678',
    },
  },
  exec: {
    args: {
      label: 'Add Command (Arguments separated by Spaces):',
      placeholder: 'Ex: woogie boogie',
    },
  },
  log: {
    args: {
      label: 'File:',
      placeholder: 'Ex: /tmp/alerts.log',
    },
  },
  smtp: {
    args: {
      label: 'Email Addresses (Separated by Spaces):',
      placeholder:
        'Ex: benedict@domain.com delaney@domain.com susan@domain.com',
    },
    details: {placeholder: 'Email body text goes here'},
  },
  slack: {
    args: {
      label: 'Send alerts to Slack channel:',
      placeholder: '#alerts',
    },
  },
  alerta: {
    args: {
      label: 'Paste Alerta TICKscript:',
      placeholder: 'alerta()',
    },
  },
  pushover: {
    args: {
      label: 'Devices (Separated by Commas, Optional):',
      placeholder: 'phone33,submarine,pineapple_tower',
    },
    properties: [
      {key: 'UserKey', label: 'User Key', placeholder: 'my_user_key'},
      {key: 'Device', label: 'Device', placeholder: 'my_device'},
      {key: 'Title', label: 'Title', placeholder: 'message_title'},
      {key: 'URL', label: 'URL', placeholder: 'http://reference.url'},
      {key: 'URLTitle', label: 'URL Title', placeholder: 'My URL Title'},
      {key: 'Sound', label: 'Sound', placeholder: 'pineapple_falling'},
    ],
  },
}

export const ALERT_NODES_ACCESSORS = {
  http: rule => _.get(rule, 'alertNodes[0].args[0]', ''),
  tcp: rule => _.get(rule, 'alertNodes[0].args[0]', ''),
  exec: rule => _.get(rule, 'alertNodes[0].args', []).join(' '),
  log: rule => _.get(rule, 'alertNodes[0].args[0]', ''),
  smtp: rule => _.get(rule, 'alertNodes[0].args', []).join(' '),
  slack: rule => _.get(rule, 'alertNodes[0].properties[0].args', ''),
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
  pushover: rule => _.get(rule, 'alertNodes[0].args'),
}
