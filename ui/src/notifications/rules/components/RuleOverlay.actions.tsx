// Types
import {
  NotificationRuleDraft,
  StatusRuleDraft,
  TagRuleDraft,
  RuleStatusLevel,
} from 'src/types'

export type LevelType = 'currentLevel' | 'previousLevel'

export type Action =
  | {type: 'UPDATE_RULE'; rule: NotificationRuleDraft}
  | {
      type: 'UPDATE_STATUS_LEVEL'
      statusID: string
      levelType: LevelType
      level: RuleStatusLevel
    }
  | {type: 'SET_ACTIVE_SCHEDULE'; schedule: 'cron' | 'every'}
  | {type: 'UPDATE_STATUS_RULES'; statusRule: StatusRuleDraft}
  | {type: 'ADD_TAG_RULE'; tagRule: TagRuleDraft}
  | {type: 'DELETE_STATUS_RULE'; statusRuleID: string}
  | {type: 'UPDATE_TAG_RULES'; tagRule: TagRuleDraft}
  | {type: 'DELETE_TAG_RULE'; tagRuleID: string}
  | {
      type: 'SET_TAG_RULE_OPERATOR'
      tagRuleID: string
      operator: TagRuleDraft['value']['operator']
    }
