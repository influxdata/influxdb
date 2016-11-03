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
