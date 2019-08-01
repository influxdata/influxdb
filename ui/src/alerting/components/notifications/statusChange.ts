import {omit} from 'lodash'

export const changes = ['changes from', 'equals']
export const previousLevel = {level: 'OK'}

export const activeChange = status => {
  if (!!status.value.previousLevel) {
    return 'changes from'
  }

  return 'equals'
}

export const changeStatusRule = (status, change) => {
  if (change === 'equals' && status.value.previousLevel) {
    return omit(status, 'value.previousLevel')
  }

  const {value} = status
  const newValue = {...value, previousLevel}

  return {...status, value: newValue}
}
