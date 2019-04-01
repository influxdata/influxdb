import tasksReducer, {
  defaultState,
  defaultTaskOptions,
} from 'src/tasks/reducers'
import {setTaskOption} from 'src/tasks/actions'
import {TaskSchedule} from 'src/utils/taskOptionsToFluxScript'

describe('tasksReducer', () => {
  describe('setTaskOption', () => {
    it('clears the cron property from the task options when interval is selected', () => {
      const initialState = defaultState
      initialState.taskOptions = {...defaultTaskOptions, cron: '0 2 * * *'}

      const actual = tasksReducer(
        initialState,
        setTaskOption({key: 'taskScheduleType', value: TaskSchedule.interval})
      )

      const expected = {
        ...defaultState,
        taskOptions: {
          ...defaultTaskOptions,
          taskScheduleType: TaskSchedule.interval,
          cron: '',
        },
      }

      expect(actual).toEqual(expected)
    })

    it('clears the interval property from the task options when cron is selected', () => {
      const initialState = defaultState
      initialState.taskOptions = {...defaultTaskOptions, interval: '24h'} // todo(docmerlin): allow for time units larger than 1d, right now h is the longest unit our s

      const actual = tasksReducer(
        initialState,
        setTaskOption({key: 'taskScheduleType', value: TaskSchedule.cron})
      )

      const expected = {
        ...defaultState,
        taskOptions: {
          ...defaultTaskOptions,
          taskScheduleType: TaskSchedule.cron,
          interval: '',
        },
      }

      expect(actual).toEqual(expected)
    })
  })
})
