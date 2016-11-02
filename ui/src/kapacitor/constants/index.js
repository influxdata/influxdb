export const defaultRuleConfigs = {
  deadman: {
    duration: '10m',
  },
  relative: {
    func: 'average',
    change: 'change',
    duration: '1m',
    compareDuration: '1m',
    operator: 'greater than',
    value: '90',
  },
  threshold: {
    operator: 'greater than',
    value: '90',
    relation: 'more than',
    percentile: '90',
    duration: '1m',
  },
};
