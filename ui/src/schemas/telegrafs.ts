// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType, Telegraf} from 'src/types'

// Schemas
import {labelSchema} from './labels'

// Defines the schema for the "telegrafs" resource
export const telegrafSchema = new schema.Entity(
  ResourceType.Telegrafs,
  {
    labels: [labelSchema],
  },
  {
    // add buckets to metadata if not present
    processStrategy: (t: Telegraf): Telegraf => {
      if (!t.metadata) {
        return {
          ...t,
          metadata: {
            buckets: [],
          },
        }
      }

      if (!t.metadata.buckets) {
        return {
          ...t,
          metadata: {
            ...t.metadata,
            buckets: [],
          },
        }
      }

      return t
    },
  }
)

export const arrayOfTelegrafs = [telegrafSchema]
