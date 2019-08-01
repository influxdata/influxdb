import {omit} from 'lodash'
import {StatusRuleItem} from 'src/types'

export const changes = ['changes from', 'equal', 'notequal']
export const previousLevel = {level: 'OK'}

export const activeChange = (status: StatusRuleItem) => {
  const {value} = status
  if (!!value.previousLevel) {
    return 'changes from'
  }

  if (value.currentLevel.operation === 'equal') {
    return 'equal'
  }

  if (value.currentLevel.operation === 'notequal') {
    return 'notequal'
  }

  throw new Error(
    'Changed statusRule.currentLevel.operation to unknown operator'
  )
}

export const changeStatusRule = (status, change) => {
  if (change === 'equals' && status.value.previousLevel) {
    return omit(status, 'value.previousLevel')
  }

  const {value} = status
  const newValue = {...value, previousLevel}

  return {...status, value: newValue}
}
