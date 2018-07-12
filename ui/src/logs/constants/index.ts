export const SEVERITY_COLORS = [
  {
    hex: '#BF3D5E',
    name: 'ruby',
  },
  {
    hex: '#DC4E58',
    name: 'fire',
  },
  {
    hex: '#F95F53',
    name: 'curacao',
  },
  {
    hex: '#F48D38',
    name: 'tiger',
  },
  {
    hex: '#FFB94A',
    name: 'pineapple',
  },
  {
    hex: '#FFD255',
    name: 'thunder',
  },
  {
    hex: '#FFE480',
    name: 'sulfur',
  },
  {
    hex: '#32B08C',
    name: 'viridian',
  },
  {
    hex: '#4ED8A0',
    name: 'rainforest',
  },
  {
    hex: '#7CE490',
    name: 'honeydew',
  },
  {
    hex: '#4591ED',
    name: 'ocean',
  },
  {
    hex: '#22ADF6',
    name: 'pool',
  },
  {
    hex: '#00C9FF',
    name: 'laser',
  },
  {
    hex: '#513CC6',
    name: 'planet',
  },
  {
    hex: '#7A65F2',
    name: 'star',
  },
  {
    hex: '#9394FF',
    name: 'comet',
  },
  {
    hex: '#545667',
    name: 'graphite',
  },
  {
    hex: '#8E91A1',
    name: 'wolf',
  },
  {
    hex: '#BEC2CC',
    name: 'mist',
  },
  {
    hex: '#E7E8EB',
    name: 'pearl',
  },
]

export const DEFAULT_SEVERITY_LEVELS = [
  {
    severity: 'emergency',
    default: SEVERITY_COLORS.find(c => c.name === 'ruby'),
    override: null,
  },
  {
    severity: 'alert',
    default: SEVERITY_COLORS.find(c => c.name === 'fire'),
    override: null,
  },
  {
    severity: 'critical',
    default: SEVERITY_COLORS.find(c => c.name === 'curacao'),
    override: null,
  },
  {
    severity: 'error',
    default: SEVERITY_COLORS.find(c => c.name === 'tiger'),
    override: null,
  },
  {
    severity: 'warning',
    default: SEVERITY_COLORS.find(c => c.name === 'pineapple'),
    override: null,
  },
  {
    severity: 'notice',
    default: SEVERITY_COLORS.find(c => c.name === 'rainforest'),
    override: null,
  },
  {
    severity: 'info',
    default: SEVERITY_COLORS.find(c => c.name === 'star'),
    override: null,
  },
  {
    severity: 'debug',
    default: SEVERITY_COLORS.find(c => c.name === 'wolf'),
    override: null,
  },
]

export enum SeverityFormatOptions {
  dot = 'dot',
  dotText = 'dotText',
  text = 'text',
}

export enum EncodingTypes {
  visibility = 'visibility',
  display = 'displayName',
  label = 'label',
}

export enum EncodingLabelOptions {
  text = 'text',
  icon = 'icon',
}

export enum EncodingVisibilityOptions {
  visible = 'visible',
  hidden = 'hidden',
}

export const TIME_RANGE_VALUES = [
  {text: '1m', seconds: 60},
  {text: '5m', seconds: 300},
  {text: '10m', seconds: 600},
  {text: '15m', seconds: 900},
]

export const SECONDS_TO_MS = 1000
