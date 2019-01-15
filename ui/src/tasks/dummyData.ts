import {Task} from 'src/api'

export const dummyTasks: Task[] = [
  {
    id: '02ef9deff2141000',
    orgID: '02ee9e2a29d73000',
    name: 'pasdlak',
    status: Task.StatusEnum.Active,
    owner: null,
    flux:
      'option task = {\n  name: "pasdlak",\n  cron: "2 0 * * *"\n}\nfrom(bucket: "inbucket") \n|> range(start: -1h)',
    cron: '2 0 * * *',
  },
  {
    id: '02f12c50dba72000',
    orgID: '02ee9e2a29d73000',
    name: 'somename',
    status: Task.StatusEnum.Active,
    owner: null,
    flux:
      'option task = {\n  name: "somename",\n  every: 1m,\n}\nfrom(bucket: "inbucket") \n|> range(start: -task.every)',
    every: '1m0s',
  },
]
