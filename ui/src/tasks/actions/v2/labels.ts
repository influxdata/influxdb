// API
import {client} from 'src/utils/api'

// Types
import {AppState, Label, Task} from 'src/types/v2'

export enum ActionTypes {
  AddTaskLabels = 'ADD_TASK_LABELS',
  RemoveTaskLabels = 'REMOVE_TASK_LABELS',
}

export type Action = AddTaskLabels | RemoveTaskLabels

type GetStateFunc = () => AppState

export interface AddTaskLabels {
  type: ActionTypes.AddTaskLabels
  payload: {
    taskID: string
    labels: Label[]
  }
}

export interface RemoveTaskLabels {
  type: ActionTypes.RemoveTaskLabels
  payload: {
    taskID: string
    labels: Label[]
  }
}

export const addTaskLabels = (
  taskID: string,
  labels: Label[]
): AddTaskLabels => ({
  type: ActionTypes.AddTaskLabels,
  payload: {
    taskID,
    labels,
  },
})

export const removeTaskLabels = (
  taskID: string,
  labels: Label[]
): RemoveTaskLabels => ({
  type: ActionTypes.RemoveTaskLabels,
  payload: {
    taskID,
    labels,
  },
})

type TasksSelector = (state: AppState) => Task[]

export const addTaskLabelsFactoryAsync = (getTasks: TasksSelector) => (
  taskID: string,
  labels: Label[]
) => async (dispatch, getState: GetStateFunc): Promise<void> => {
  try {
    const state = await getState()
    const tasks = getTasks(state)

    const task = tasks.find(t => {
      return t.id === taskID
    })

    const labelsToAdd = labels.filter(label => {
      if (!task.labels.find(l => l.name === label.name)) {
        return label
      }
    })

    const newLabels = await client.tasks.addLabels(taskID, labelsToAdd)

    dispatch(addTaskLabels(taskID, newLabels))
  } catch (error) {
    console.error(error)
  }
}

export const removeTaskLabelsAsync = (
  taskID: string,
  labels: Label[]
) => async (dispatch): Promise<void> => {
  try {
    await client.tasks.removeLabels(taskID, labels)
    dispatch(removeTaskLabels(taskID, labels))
  } catch (error) {
    console.error(error)
  }
}
