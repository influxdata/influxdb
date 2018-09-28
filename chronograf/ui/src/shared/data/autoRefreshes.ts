export enum AutoRefreshOptionType {
  Option = 'option',
  Header = 'header',
}

export interface AutoRefreshOption {
  id: string
  milliseconds: number
  label: string
  type: AutoRefreshOptionType
}

const autoRefreshOptions: AutoRefreshOption[] = [
  {
    id: 'auto-refresh-header',
    milliseconds: 0,
    label: 'Refresh',
    type: AutoRefreshOptionType.Header,
  },
  {
    id: 'auto-refresh-paused',
    milliseconds: 0,
    label: 'Paused',
    type: AutoRefreshOptionType.Option,
  },
  {
    id: 'auto-refresh-5s',
    milliseconds: 5000,
    label: '5s',
    type: AutoRefreshOptionType.Option,
  },
  {
    id: 'auto-refresh-10s',
    milliseconds: 10000,
    label: '10s',
    type: AutoRefreshOptionType.Option,
  },
  {
    id: 'auto-refresh-15s',
    milliseconds: 15000,
    label: '15s',
    type: AutoRefreshOptionType.Option,
  },
  {
    id: 'auto-refresh-30s',
    milliseconds: 30000,
    label: '30s',
    type: AutoRefreshOptionType.Option,
  },
  {
    id: 'auto-refresh-60s',
    milliseconds: 60000,
    label: '60s',
    type: AutoRefreshOptionType.Option,
  },
]

export default autoRefreshOptions
