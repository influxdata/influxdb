export interface Task {
  id: string
  name: string
  status: TaskStatus
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

export enum TaskStatus {
  Active = 'active',
  Inactive = 'inactive',
}
