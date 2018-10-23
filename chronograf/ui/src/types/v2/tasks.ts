export interface Task {
  id: string
  name: string
  organizationId: string
  organization: {
    id: string
    name: string
  }
  owner: {
    id: string
    name: string
  }
  flux?: string
}
