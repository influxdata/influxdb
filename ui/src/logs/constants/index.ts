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

export const SEVERITY_COLORS = [
  {
    hex: '#BF3D5E',
    name: SeverityColorOptions.ruby,
  },
  {
    hex: '#DC4E58',
    name: SeverityColorOptions.fire,
  },
  {
    hex: '#F95F53',
    name: SeverityColorOptions.curacao,
  },
  {
    hex: '#F48D38',
    name: SeverityColorOptions.tiger,
  },
  {
    hex: '#FFB94A',
    name: SeverityColorOptions.pineapple,
  },
  {
    hex: '#FFD255',
    name: SeverityColorOptions.thunder,
  },
  {
    hex: '#FFE480',
    name: SeverityColorOptions.sulfur,
  },
  {
    hex: '#32B08C',
    name: SeverityColorOptions.viridian,
  },
  {
    hex: '#4ED8A0',
    name: SeverityColorOptions.rainforest,
  },
  {
    hex: '#7CE490',
    name: SeverityColorOptions.honeydew,
  },
  {
    hex: '#4591ED',
    name: SeverityColorOptions.ocean,
  },
  {
    hex: '#22ADF6',
    name: SeverityColorOptions.pool,
  },
  {
    hex: '#00C9FF',
    name: SeverityColorOptions.laser,
  },
  {
    hex: '#513CC6',
    name: SeverityColorOptions.planet,
  },
  {
    hex: '#7A65F2',
    name: SeverityColorOptions.star,
  },
  {
    hex: '#9394FF',
    name: SeverityColorOptions.comet,
  },
  {
    hex: '#545667',
    name: SeverityColorOptions.graphite,
  },
  {
    hex: '#8E91A1',
    name: SeverityColorOptions.wolf,
  },
  {
    hex: '#BEC2CC',
    name: SeverityColorOptions.mist,
  },
  {
    hex: '#E7E8EB',
    name: SeverityColorOptions.pearl,
  },
]

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
