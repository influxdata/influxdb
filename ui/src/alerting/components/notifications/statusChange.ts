import {omit} from 'lodash'
import {StatusRuleDraft, LevelRule} from 'src/types'

type Changes = 'changes from' | 'equal'
export const changes: Changes[] = ['changes from', 'equal']

export const activeChange = (status: StatusRuleDraft) => {
  const {currentLevel, previousLevel} = status.value

  if (!!previousLevel) {
    return 'changes from'
  }

  if (currentLevel.operation === 'equal') {
    return 'equal'
  }

  throw new Error(
    'Changed statusRule.currentLevel.operation to unknown operator'
  )
}

export const previousLevel: LevelRule = {level: 'OK'}
export const changeStatusRule = (
  status: StatusRuleDraft,
  change: Changes
): StatusRuleDraft => {
  if (change === 'equal') {
    return omit(status, 'value.previousLevel') as StatusRuleDraft
  }

  const {value} = status
  const newValue = {...value, previousLevel}

  return {...status, value: newValue}
}
