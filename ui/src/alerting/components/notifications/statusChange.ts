import {omit} from 'lodash'
import {StatusRuleItem, LevelRule} from 'src/types'

type Changes = 'changes from' | 'equal' | 'notequal'
export const changes: Changes[] = ['changes from', 'equal', 'notequal']

export const activeChange = (status: StatusRuleItem) => {
  const {currentLevel, previousLevel} = status.value

  if (!!previousLevel) {
    return 'changes from'
  }

  if (currentLevel.operation === 'equal') {
    return 'equal'
  }

  if (currentLevel.operation === 'notequal') {
    return 'notequal'
  }

  throw new Error(
    'Changed statusRule.currentLevel.operation to unknown operator'
  )
}

export const previousLevel: LevelRule = {level: 'OK'}
export const changeStatusRule = (
  status: StatusRuleItem,
  change: Changes
): StatusRuleItem => {
  if (change === 'equal' || change === 'notequal') {
    return omit(status, 'value.previousLevel') as StatusRuleItem
  }

  const {value} = status
  const newValue = {...value, previousLevel}

  return {...status, value: newValue}
}
