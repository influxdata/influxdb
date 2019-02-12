import {Run} from '@influxdata/influx'

export const taskRuns: Run[] = [
  {
    id: '40002242',
    taskID: 'string',
    status: Run.StatusEnum.Scheduled,
    scheduledFor: new Date('2019-02-11T22:37:25.985Z'),
    startedAt: new Date('2019-02-11T22:37:25.985Z'),
    finishedAt: new Date('2019-02-11T22:37:25.985Z'),
    requestedAt: new Date('2019-02-11T22:37:25.985Z'),
    links: {
      self: '/api/v2/tasks/1/runs/1',
      task: '/arequei/v2/tasks/1',
      retry: '/api/v2/tasks/1/runs/1/retry',
      logs: '/api/v2/tasks/1/runs/1/logs',
    },
  },
]
