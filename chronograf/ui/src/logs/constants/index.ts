export enum SeverityColorOptions {
  ruby = 'ruby',
  fire = 'fire',
  curacao = 'curacao',
  tiger = 'tiger',
  pineapple = 'pineapple',
  thunder = 'thunder',
  sulfur = 'sulfur',
  viridian = 'viridian',
  rainforest = 'rainforest',
  honeydew = 'honeydew',
  ocean = 'ocean',
  pool = 'pool',
  laser = 'laser',
  planet = 'planet',
  star = 'star',
  comet = 'comet',
  graphite = 'graphite',
  wolf = 'wolf',
  mist = 'mist',
  pearl = 'pearl',
}

export const SeverityColorValues = {
  [SeverityColorOptions.ruby]: '#BF3D5E',
  [SeverityColorOptions.fire]: '#DC4E58',
  [SeverityColorOptions.curacao]: '#F95F53',
  [SeverityColorOptions.tiger]: '#F48D38',
  [SeverityColorOptions.pineapple]: '#FFB94A',
  [SeverityColorOptions.thunder]: '#FFD255',
  [SeverityColorOptions.sulfur]: '#FFE480',
  [SeverityColorOptions.viridian]: '#32B08C',
  [SeverityColorOptions.rainforest]: '#4ED8A0',
  [SeverityColorOptions.honeydew]: '#7CE490',
  [SeverityColorOptions.ocean]: '#4591ED',
  [SeverityColorOptions.pool]: '#22ADF6',
  [SeverityColorOptions.laser]: '#00C9FF',
  [SeverityColorOptions.planet]: '#513CC6',
  [SeverityColorOptions.star]: '#7A65F2',
  [SeverityColorOptions.comet]: '#9394FF',
  [SeverityColorOptions.graphite]: '#545667',
  [SeverityColorOptions.wolf]: '#8E91A1',
  [SeverityColorOptions.mist]: '#BEC2CC',
  [SeverityColorOptions.pearl]: '#E7E8EB',
}

export const SEVERITY_COLORS = [
  {
    hex: SeverityColorValues[SeverityColorOptions.ruby],
    name: SeverityColorOptions.ruby,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.fire],
    name: SeverityColorOptions.fire,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.curacao],
    name: SeverityColorOptions.curacao,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.tiger],
    name: SeverityColorOptions.tiger,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.pineapple],
    name: SeverityColorOptions.pineapple,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.thunder],
    name: SeverityColorOptions.thunder,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.sulfur],
    name: SeverityColorOptions.sulfur,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.viridian],
    name: SeverityColorOptions.viridian,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.rainforest],
    name: SeverityColorOptions.rainforest,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.honeydew],
    name: SeverityColorOptions.honeydew,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.ocean],
    name: SeverityColorOptions.ocean,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.pool],
    name: SeverityColorOptions.pool,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.laser],
    name: SeverityColorOptions.laser,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.planet],
    name: SeverityColorOptions.planet,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.star],
    name: SeverityColorOptions.star,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.comet],
    name: SeverityColorOptions.comet,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.graphite],
    name: SeverityColorOptions.graphite,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.wolf],
    name: SeverityColorOptions.wolf,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.mist],
    name: SeverityColorOptions.mist,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.pearl],
    name: SeverityColorOptions.pearl,
  },
]

export enum SeverityLevelOptions {
  emerg = 'emerg',
  alert = 'alert',
  crit = 'crit',
  err = 'err',
  warning = 'warning',
  notice = 'notice',
  info = 'info',
  debug = 'debug',
}

export const SEVERITY_SORTING_ORDER = {
  [SeverityLevelOptions.emerg]: 1,
  [SeverityLevelOptions.alert]: 2,
  [SeverityLevelOptions.crit]: 3,
  [SeverityLevelOptions.err]: 4,
  [SeverityLevelOptions.warning]: 5,
  [SeverityLevelOptions.notice]: 6,
  [SeverityLevelOptions.info]: 7,
  [SeverityLevelOptions.debug]: 8,
}

export const DEFAULT_SEVERITY_LEVELS = {
  [SeverityLevelOptions.emerg]: SeverityColorOptions.ruby,
  [SeverityLevelOptions.alert]: SeverityColorOptions.fire,
  [SeverityLevelOptions.crit]: SeverityColorOptions.curacao,
  [SeverityLevelOptions.err]: SeverityColorOptions.tiger,
  [SeverityLevelOptions.warning]: SeverityColorOptions.pineapple,
  [SeverityLevelOptions.notice]: SeverityColorOptions.rainforest,
  [SeverityLevelOptions.info]: SeverityColorOptions.star,
  [SeverityLevelOptions.debug]: SeverityColorOptions.wolf,
}

export enum SeverityFormatOptions {
  dot = 'dot',
  dotText = 'dotText',
  text = 'text',
}

export enum EncodingTypes {
  visibility = 'visibility',
  display = 'displayName',
  label = 'label',
  color = 'color',
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
