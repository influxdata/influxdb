export interface Task {
  every?: string
  cron?: string
  delay?: string
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
