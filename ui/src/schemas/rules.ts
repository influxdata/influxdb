// Libraries
import {schema} from 'normalizr'

// Schemas
import {labelSchema} from './labels'

// Utils
import {ruleToDraftRule} from 'src/notifications/rules/utils'

// Types
import {ResourceType, GenRule, NotificationRuleDraft} from 'src/types'

/* Rules */
export const ruleSchema = new schema.Entity(
  ResourceType.NotificationRules,
  {
    labels: [labelSchema],
  },
  {
    processStrategy: (rule: GenRule): NotificationRuleDraft => ({
      ...ruleToDraftRule(rule),
    }),
  }
)

export const arrayOfRules = [ruleSchema]
