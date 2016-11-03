export const defaultRuleConfigs = {
  deadman: {
    period: '10m',
  },
  relative: {
    change: 'change',
    period: '1m',
    shift: '1m',
    operator: 'greater than',
    value: '90',
  },
  threshold: {
    operator: 'greater than',
    value: '90',
    relation: 'more than',
    percentile: '90',
    period: '1m',
  },
};

export const OPERATORS = ['greater than', 'equal to or greater', 'equal to or less than', 'less than', 'equal to', 'not equal to'];
export const RELATIONS = ['once', 'more than ', 'less than'];
export const PERIODS = ['1m', '5m', '10m', '30m', '1h', '2h', '1d'];
export const CHANGES = ['change', '% change'];
export const SHIFTS = ['1m', '5m', '10m', '30m', '1h', '2h', '1d'];
