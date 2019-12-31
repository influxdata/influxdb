import {Task as ITask} from 'src/client'
import {Label} from 'src/types'

export interface Task extends ITask {
  labels?: Label[]
}
