import tasksReducer from 'src/tasks/reducers'
import {setTaskOption} from 'src/tasks/actions/creators'

// Helpers
import {initialState, defaultOptions} from 'src/tasks/reducers/helpers'

// Types
import {TaskSchedule} from 'src/types'

describe('tasksReducer', () => {
  describe('setTaskOption', () => {
    it('should not clear the cron property from the task options when interval is selected', () => {
      const state = initialState()
      const cron = '0 2 * * *'
      state.taskOptions = {...defaultOptions, cron}

      const actual = tasksReducer(
        state,
        setTaskOption({key: 'taskScheduleType', value: TaskSchedule.interval})
      )

      const expected = {
        ...state,
        taskOptions: {
          ...defaultOptions,
          taskScheduleType: TaskSchedule.interval,
          cron,
        },
      }

      expect(actual).toEqual(expected)
    })

    it('should not clear the interval property from the task options when cron is selected', () => {
      const state = initialState()
      const interval = '24h'
      state.taskOptions = {...defaultOptions, interval} // todo(docmerlin): allow for time units larger than 1d, right now h is the longest unit our s

      const actual = tasksReducer(
        state,
        setTaskOption({key: 'taskScheduleType', value: TaskSchedule.cron})
      )

      const expected = {
        ...state,
        taskOptions: {
          ...defaultOptions,
          taskScheduleType: TaskSchedule.cron,
          interval,
        },
      }

      expect(actual).toEqual(expected)
    })
  })
})
