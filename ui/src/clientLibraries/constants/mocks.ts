export interface ClientLibrary {
  id: string
  name: string
  description?: string
}

export const mockClientLibraries: ClientLibrary[] = [
  {
    id: 'client-lib-js',
    name: 'Javascript',
    description: 'Write to InfuxDB using Javascript',
  },
  {
    id: 'client-lib-node',
    name: 'Node',
    description: 'Write to InfuxDB using Node',
  },
  {
    id: 'client-lib-go',
    name: 'GO',
    description: 'Write to InfuxDB using GO',
  },
  {
    id: 'client-lib-python',
    name: 'Python',
    description: 'Write to InfuxDB using Python',
  },
]
