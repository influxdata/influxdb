export enum ComponentColor {
  Default = 'default',
  Primary = 'primary',
  Secondary = 'secondary',
  Success = 'success',
  Warning = 'warning',
  Danger = 'danger',
  Alert = 'alert',
}

export enum ComponentSize {
  ExtraSmall = 'xs',
  Small = 'sm',
  Medium = 'md',
  Large = 'lg',
}

export enum ComponentStatus {
  Default = 'default',
  Loading = 'loading',
  Error = 'error',
  Valid = 'valid',
  Disabled = 'disabled',
}

export enum DropdownMenuColors {
  Amethyst = 'amethyst',
  Malachite = 'malachite',
  Sapphire = 'sapphire',
  Onyx = 'onyx',
}

export type DropdownChild =
  | Array<string | JSX.Element | Element>
  | string
  | JSX.Element
  | Element

export enum ButtonShape {
  Default = 'none',
  Square = 'square',
  StretchToFit = 'stretch',
}

export enum ButtonType {
  Button = 'button',
  Submit = 'submit',
}

export enum Greys {
  Obsidian = '#0f0e15',
  Raven = '#1c1c21',
  Kevlar = '#202028',
  Castle = '#292933',
  Onyx = '#31313d',
  Pepper = '#383846',
  Smoke = '#434453',
  Graphite = '#545667',
  Storm = '#676978',
  Mountain = '#757888',
  Wolf = '#8e91a1',
  Sidewalk = '#999dab',
  Forge = '#a4a8b6',
  Mist = '#bec2cc',
  Chromium = '#c6cad3',
  Platinum = '#d4d7dd',
  Pearl = '#e7e8eb',
  Whisper = '#eeeff2',
  Cloud = '#f6f6f8',
  Ghost = '#fafafc',
  White = '#ffffff',
}

export enum IconFont {
  AddCell = 'add-cell',
  AlertTriangle = 'alert-triangle',
  Alerts = 'alerts',
  Annotate = 'annotate',
  AnnotatePlus = 'annotate-plus',
  AuthZero = 'authzero',
  BarChart = 'bar-chart',
  Capacitor = 'capacitor2',
  CaretDown = 'caret-down',
  CaretLeft = 'caret-left',
  CaretRight = 'caret-right',
  CaretUp = 'caret-up',
  Chat = 'chat',
  Checkmark = 'checkmark',
  Circle = 'circle',
  CircleThick = 'circle-thick',
  Clock = 'clock',
  CogOutline = 'cog-outline',
  CogThick = 'cog-thick',
  Collapse = 'collapse',
  CrownOutline = 'crown-outline',
  CrownSolid = 'crown2',
  Cube = 'cube',
  Cubouniform = 'cubo-uniform',
  DashF = 'dash-f',
  DashH = 'dash-h',
  DashJ = 'dash-j',
  Disks = 'disks',
  Download = 'download',
  Duplicate = 'duplicate',
  ExpandA = 'expand-a',
  ExpandB = 'expand-b',
  Export = 'export',
  Eye = 'eye',
  EyeClosed = 'eye-closed',
  EyeOpen = 'eye-open',
  GitHub = 'github',
  Google = 'google',
  GraphLine = 'graphline2',
  Group = 'group',
  Heroku = 'heroku',
  HerokuSimple = '',
  Import = 'import',
  Link = 'link',
  OAuth = 'oauth',
  Octagon = 'octagon',
  Okta = 'okta',
  Pause = 'pause',
  Pencil = 'pencil',
  Play = 'play',
  Plus = 'plus',
  PlusSkinny = 'plus-skinny',
  Pulse = 'pulse-c',
  Refresh = 'refresh',
  Remove = 'remove',
  Search = 'search',
  Server = 'server2',
  Shuffle = 'shuffle',
  Square = 'square',
  TextBlock = 'text-block',
  Trash = 'trash',
  Triangle = 'triangle',
  User = 'user',
  UserAdd = 'user-add',
  UserOutline = 'user-outline',
  UserRemove = 'user-remove',
  Wood = 'wood',
  Wrench = 'wrench',
  Star = 'star',
  Stop = 'stop',
  Zap = 'zap',
}

export enum Columns {
  One = 1,
  Two = 2,
  Three = 3,
  Four = 4,
  Five = 5,
  Six = 6,
  Seven = 7,
  Eight = 8,
  Nine = 9,
  Ten = 10,
  Eleven = 11,
  Twelve = 12,
}

export enum Alignment {
  Left = 'left',
  Center = 'center',
  Right = 'right',
}
