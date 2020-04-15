// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'

// Schemas
import {labelSchema} from './labels'

// Defines the schema for the "templates" resource
export const templateSchema = new schema.Entity(ResourceType.Templates, {
  labels: [labelSchema],
})

export const arrayOfTemplates = [templateSchema]
