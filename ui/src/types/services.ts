export interface Service {
  id?: string
  url: string
  name: string
  type: string
  username?: string
  password?: string
  active: boolean
  insecureSkipVerify: boolean
  links: {
    source: string
    self: string
    proxy: string
  }
}
