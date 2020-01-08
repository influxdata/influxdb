// Libraries
import {produce} from 'immer'

// Types
import {
  Action,
  ADD_TASK,
  SET_TASKS,
  CLEAR_TASK,
  CLEAR_CURRENT_TASK,
  SET_RUNS,
  SET_TASK_OPTION,
  SET_ALL_TASK_OPTIONS,
  SET_NEW_SCRIPT,
  SET_CURRENT_SCRIPT,
  SET_CURRENT_TASK,
  SET_SEARCH_TERM,
  SET_SHOW_INACTIVE,
  SET_LOGS,
  EDIT_TASK,
  REMOVE_TASK,
} from 'src/tasks/actions/creators'
import {ResourceType, ResourceState, TaskSchedule, Task} from 'src/types'

// Utils
import {initialState, defaultOptions} from 'src/tasks/reducers/helpers'
import {
  setResource,
  editResource,
  removeResource,
  addResource,
} from 'src/resources/reducers/helpers'

type TasksState = ResourceState['tasks']

export default (
  state: TasksState = initialState(),
  action: Action
): TasksState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_TASKS: {
        setResource<Task>(draftState, action, ResourceType.Tasks)

        return
      }

      case EDIT_TASK: {
        editResource<Task>(draftState, action, ResourceType.Tasks)

        return
      }

      case REMOVE_TASK: {
        removeResource<Task>(draftState, action)

        return
      }

      case ADD_TASK: {
        addResource<Task>(draftState, action, ResourceType.Tasks)

        return
      }

      case CLEAR_TASK: {
        draftState.taskOptions = defaultOptions
        draftState.currentScript = ''
        draftState.newScript = ''

        return
      }

      case CLEAR_CURRENT_TASK: {
        draftState.currentScript = ''
        draftState.currentTask = null

        return
      }

      case SET_ALL_TASK_OPTIONS: {
        const {schema} = action
        const {entities, result} = schema
        const {name, every, cron, orgID, offset} = entities.tasks[result]
        let taskScheduleType = TaskSchedule.interval

        if (cron) {
          taskScheduleType = TaskSchedule.cron
        }

        draftState.taskOptions = {
          ...state.taskOptions,
          name,
          cron,
          interval: every,
          orgID,
          taskScheduleType,
          offset,
        }

        return
      }

      case SET_TASK_OPTION: {
        const {key, value} = action

        draftState.taskOptions[`${key}`] = value

        return
      }

      case SET_NEW_SCRIPT: {
        draftState.newScript = action.script

        return
      }

      case SET_CURRENT_SCRIPT: {
        draftState.currentScript = action.script

        return
      }

      case SET_CURRENT_TASK: {
        const {schema} = action
        const {entities, result} = schema

        const task = entities.tasks[result]

        const currentScript = task.flux || ''

        draftState.currentScript = currentScript
        draftState.currentTask = task

        return
      }

      case SET_SEARCH_TERM: {
        const {searchTerm} = action

        draftState.searchTerm = searchTerm

        return
      }

      case SET_SHOW_INACTIVE: {
        draftState.showInactive = !state.showInactive

        return
      }

      case SET_RUNS: {
        const {runs, runStatus} = action

        draftState.runs = runs
        draftState.runStatus = runStatus

        return
      }

      case SET_LOGS: {
        draftState.logs = action.logs

        return
      }
    }
  })
