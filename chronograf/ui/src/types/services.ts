export interface NewService {
  url: string
  name: string
  type: string
  username?: string
  password?: string
  active: boolean
  insecureSkipVerify: boolean
}

export interface Service {
  id?: string
  sourceID: string
  url: string
  name: string
  type: string
  username?: string
  password?: string
  active: boolean
  insecureSkipVerify: boolean
  metadata: {
    [x: string]: any
  }
  links: {
    source: string
    self: string
    proxy: string
  }
}
