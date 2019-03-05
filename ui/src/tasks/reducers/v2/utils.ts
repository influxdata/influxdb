import {ILabel} from '@influxdata/influx'

import {Task} from 'src/types/v2'

export const addTaskLabels = (
  tasks: Task[],
  taskID: string,
  labels: ILabel[]
) => {
  return tasks.map(t => {
    if (t.id === taskID) {
      return {...t, labels: [...t.labels, ...labels]}
    }
    return t
  })
}

export const removeTaskLabels = (
  tasks: Task[],
  taskID: string,
  labels: ILabel[]
) => {
  return tasks.map(t => {
    if (t.id === taskID) {
      const updatedLabels = t.labels.filter(l => {
        if (!labels.find(label => label.name === l.name)) {
          return l
        }
      })

      return {...t, labels: updatedLabels}
    }
    return t
  })
}
