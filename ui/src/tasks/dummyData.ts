import {Run, Logs} from '@influxdata/influx'

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
  {
    id: '40002342',
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

export const runLogs: Logs = {
  events: [
    {
      time: new Date('2019-02-12T22:00:09.572Z'),
      message: 'Halt and catch fire',
    },
    {
      time: new Date('2019-02-12T22:00:09.572Z'),
      message: 'Catch fire and Halt',
    },
  ],
}
