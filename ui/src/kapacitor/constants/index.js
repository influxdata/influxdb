import _ from 'lodash';

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
};

export const OPERATORS = ['greater than', 'equal to or greater', 'equal to or less than', 'less than', 'equal to', 'not equal to', 'inside range', 'outside range'];
// export const RELATIONS = ['once', 'more than ', 'less than'];
export const PERIODS = ['1m', '5m', '10m', '30m', '1h', '2h', '24h'];
export const CHANGES = ['change', '% change'];
export const SHIFTS = ['1m', '5m', '10m', '30m', '1h', '2h', '24h'];
export const ALERTS = ['hipchat', 'opsgenie', 'pagerduty', 'sensu', 'slack', 'smtp', 'talk', 'telegram', 'victorops'];

export const DEFAULT_RULE_ID = 'DEFAULT_RULE_ID';

export const RULE_MESSAGE_TEMPLATES = {
  id: {label: "{{.ID}}", text: "The ID of the alert"},
  name: {label: "{{.Name}}", text: "Measurement name"},
  taskName: {label: "{{.TaskName}}", text: "The name of the task"},
  group: {label: "{{.Group}}", text: "Concatenation of all group-by tags of the form <code>&#91;key=value,&#93;+</code>. If no groupBy is performed equal to literal &quot;nil&quot;"},
  tags: {label: "{{.Tags}}", text: "Map of tags. Use <code>&#123;&#123; index .Tags &quot;key&quot; &#125;&#125;</code> to get a specific tag value"},
  level: {label: "{{.Level}}", text: "Alert Level, one of: <code>INFO</code><code>WARNING</code><code>CRITICAL</code>"},
  fields: {label: `{{ index .Fields "value" }}`, text: "Map of fields. Use <code>&#123;&#123; index .Fields &quot;key&quot; &#125;&#125;</code> to get a specific field value"},
  time: {label: "{{.Time}}", text: "The time of the point that triggered the event"},
};

export const DEFAULT_ALERTS = ['http', 'tcp', 'exec'];

export const DEFAULT_ALERT_PLACEHOLDERS = {
  http: 'URL',
  tcp: 'Address',
  exec: 'Add command with arguments separated by spaces',
  smtp: 'Add email addresses separated by spaces',
  alerta: 'Paste Alerta tick script here',
};

export const ALERT_NODES_ACCESSORS = {
  http: (rule) => _.get(rule, 'alertNodes[0].args[0]', ''),
  tcp: (rule) => _.get(rule, 'alertNodes[0].args[0]', ''),
  exec: (rule) => _.get(rule, 'alertNodes[0].args', []).join(' '),
  smtp: (rule) => _.get(rule, 'alertNodes[0].args', []).join(' '),
  alerta: (rule) => _.get(rule, 'alertNodes[0].properties', []).reduce((strs, item) => {
    strs.push(`${item.name}('${item.args.join(' ')}')`);
    return strs;
  }, ['alerta()']).join('.'),
};
