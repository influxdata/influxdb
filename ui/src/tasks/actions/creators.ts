// Types
import {
  Run,
  LogEvent,
  TaskOptionKeys,
  RemoteDataState,
  TaskEntities,
} from 'src/types'
import {NormalizedSchema} from 'normalizr'

export const SET_TASKS = 'SET_TASKS'
export const EDIT_TASK = 'EDIT_TASK'
export const SET_TASK_OPTION = 'SET_TASK_OPTION'
export const SET_ALL_TASK_OPTIONS = 'SET_ALL_TASK_OPTIONS'
export const CLEAR_TASK = 'CLEAR_TASK'
export const CLEAR_CURRENT_TASK = 'CLEAR_CURRENT_TASK'
export const SET_NEW_SCRIPT = 'SET_NEW_SCRIPT'
export const SET_CURRENT_SCRIPT = 'SET_CURRENT_SCRIPT'
export const SET_CURRENT_TASK = 'SET_CURRENT_TASK'
export const SET_SEARCH_TERM = 'SET_SEARCH_TERM'
export const SET_SHOW_INACTIVE = 'SET_SHOW_INACTIVE'
export const SET_RUNS = 'SET_RUNS'
export const SET_LOGS = 'SET_LOGS'
export const REMOVE_TASK = 'REMOVE_TASK'
export const ADD_TASK = 'ADD_TASK'

export type Action =
  | ReturnType<typeof setTasks>
  | ReturnType<typeof editTask>
  | ReturnType<typeof setTaskOption>
  | ReturnType<typeof setAllTaskOptions>
  | ReturnType<typeof setSearchTerm>
  | ReturnType<typeof setCurrentScript>
  | ReturnType<typeof setCurrentTask>
  | ReturnType<typeof setShowInactive>
  | ReturnType<typeof clearTask>
  | ReturnType<typeof setRuns>
  | ReturnType<typeof setLogs>
  | ReturnType<typeof editTask>
  | ReturnType<typeof setNewScript>
  | ReturnType<typeof clearCurrentTask>
  | ReturnType<typeof removeTask>
  | ReturnType<typeof addTask>

// R is the type of the value of the "result" key in normalizr's normalization
type TasksSchema<R extends string | string[]> = NormalizedSchema<
  TaskEntities,
  R
>
export const setTasks = (
  status: RemoteDataState,
  schema?: TasksSchema<string[]>
) =>
  ({
    type: SET_TASKS,
    status,
    schema,
  } as const)

export const addTask = (schema: TasksSchema<string>) =>
  ({
    type: ADD_TASK,
    schema,
  } as const)

export const editTask = (schema: TasksSchema<string>) =>
  ({
    type: EDIT_TASK,
    schema,
  } as const)

export const removeTask = (id: string) =>
  ({
    type: REMOVE_TASK,
    id,
  } as const)

export const setCurrentTask = (schema: TasksSchema<string>) =>
  ({
    type: SET_CURRENT_TASK,
    schema,
  } as const)

export const clearCurrentTask = () =>
  ({
    type: CLEAR_CURRENT_TASK,
  } as const)

export const setTaskOption = (taskOption: {
  key: TaskOptionKeys
  value: string
}) =>
  ({
    type: SET_TASK_OPTION,
    ...taskOption,
  } as const)

export const setAllTaskOptions = (schema: TasksSchema<string>) =>
  ({
    type: SET_ALL_TASK_OPTIONS,
    schema,
  } as const)

export const clearTask = () =>
  ({
    type: CLEAR_TASK,
  } as const)

export const setNewScript = (script: string) =>
  ({
    type: SET_NEW_SCRIPT,
    script,
  } as const)

export const setCurrentScript = (script: string) =>
  ({
    type: SET_CURRENT_SCRIPT,
    script,
  } as const)

export const setSearchTerm = (searchTerm: string) =>
  ({
    type: SET_SEARCH_TERM,
    searchTerm,
  } as const)

export const setShowInactive = () =>
  ({
    type: SET_SHOW_INACTIVE,
  } as const)

export const setRuns = (runs: Run[], runStatus: RemoteDataState) =>
  ({
    type: SET_RUNS,
    runs,
    runStatus,
  } as const)

export const setLogs = (logs: LogEvent[]) =>
  ({
    type: SET_LOGS,
    logs,
  } as const)
