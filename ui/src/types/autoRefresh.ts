export enum AutoRefreshStatus {
  Active = 'active',
  Disabled = 'disabled',
  Paused = 'paused',
}

export interface AutoRefresh {
  status: AutoRefreshStatus
  interval: number
}
