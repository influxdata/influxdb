export interface ErrorDescription {
  status: number
  auth: {
    links: {
      me: string
    }
  }
}

export enum AlertType {
  Info = 'info',
  Warning = 'warning',
}
