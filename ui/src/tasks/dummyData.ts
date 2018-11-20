import {Task, TaskStatus} from 'src/types/v2/tasks'

export const dummyTasks: Task[] = [
  {
    id: '02ef9deff2141000',
    organizationId: '02ee9e2a29d73000',
    name: 'pasdlak',
    status: TaskStatus.Active,
    owner: {id: '02ee9e2a19d73000', name: ''},
    flux:
      'option task = {\n  name: "pasdlak",\n  cron: "2 0 * * *"\n}\nfrom(bucket: "inbucket") \n|> range(start: -1h)',
    cron: '2 0 * * *',
    organization: {
      links: {
        buckets: '/api/v2/buckets?org=RadicalOrganization',
        dashboards: '/api/v2/dashboards?org=RadicalOrganization',
        log: '/api/v2/orgs/02ee9e2a29d73000/log',
        members: '/api/v2/orgs/02ee9e2a29d73000/members',
        self: '/api/v2/orgs/02ee9e2a29d73000',
        tasks: '/api/v2/tasks?org=RadicalOrganization',
      },
      id: '02ee9e2a29d73000',
      name: 'RadicalOrganization',
    },
  },
  {
    id: '02f12c50dba72000',
    organizationId: '02ee9e2a29d73000',
    name: 'somename',
    status: TaskStatus.Active,
    owner: {id: '02ee9e2a19d73000', name: ''},
    flux:
      'option task = {\n  name: "somename",\n  every: 1m,\n}\nfrom(bucket: "inbucket") \n|> range(start: -task.every)',
    every: '1m0s',
    organization: {
      links: {
        buckets: '/api/v2/buckets?org=RadicalOrganization',
        dashboards: '/api/v2/dashboards?org=RadicalOrganization',
        log: '/api/v2/orgs/02ee9e2a29d73000/log',
        members: '/api/v2/orgs/02ee9e2a29d73000/members',
        self: '/api/v2/orgs/02ee9e2a29d73000',
        tasks: '/api/v2/tasks?org=RadicalOrganization',
      },
      id: '02ee9e2a29d73000',
      name: 'RadicalOrganization',
    },
  },
]
