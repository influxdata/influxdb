// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'
import {labelSchema} from './labels'

/* Tasks */

// Defines the schema for the tasks resource
export const taskSchema = new schema.Entity(ResourceType.Tasks, {
  labels: [labelSchema],
})

export const arrayOfTasks = [taskSchema]
