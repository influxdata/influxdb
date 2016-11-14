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
    relation: 'once',
    percentile: '90',
  },
};

export const OPERATORS = ['greater than', 'equal to or greater', 'equal to or less than', 'less than', 'equal to', 'not equal to'];
// export const RELATIONS = ['once', 'more than ', 'less than'];
export const PERIODS = ['1m', '5m', '10m', '30m', '1h', '2h', '24h'];
export const CHANGES = ['change', '% change'];
export const SHIFTS = ['1m', '5m', '10m', '30m', '1h', '2h', '24h'];
export const ALERTS = ['hipchat', 'opsgenie', 'pagerduty', 'sensu', 'slack', 'smtp', 'talk', 'telegram', 'victorops'];

export const DEFAULT_RULE_ID = 'DEFAULT_RULE_ID';
