interface Service {
  id?: string
  url: string
  name: string
  type: string
  username?: string
  password?: string
  active: boolean
  insecureSkipVerify: boolean
  links: {
    self: string
    proxy: string
  }
}
