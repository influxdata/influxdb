import {
  ApplyFuncsToFieldArgs,
  Field,
  Namespace,
  Tag,
  TimeShift,
} from 'src/types'

interface ChooseNamespaceAction {
  type: 'KAPA_CHOOSE_NAMESPACE'
  payload: {
    queryID: string
    database: string
    retentionPolicy: string
  }
}
export const chooseNamespace = (
  queryID: string,
  {database, retentionPolicy}: Namespace
): ChooseNamespaceAction => ({
  type: 'KAPA_CHOOSE_NAMESPACE',
  payload: {
    queryID,
    database,
    retentionPolicy,
  },
})

interface ChooseMeasurementAction {
  type: 'KAPA_CHOOSE_MEASUREMENT'
  payload: {
    queryID: string
    measurement: string
  }
}
export const chooseMeasurement = (
  queryID: string,
  measurement: string
): ChooseMeasurementAction => ({
  type: 'KAPA_CHOOSE_MEASUREMENT',
  payload: {
    queryID,
    measurement,
  },
})

interface ChooseTagAction {
  type: 'KAPA_CHOOSE_TAG'
  payload: {
    queryID: string
    tag: Tag
  }
}
export const chooseTag = (queryID: string, tag: Tag): ChooseTagAction => ({
  type: 'KAPA_CHOOSE_TAG',
  payload: {
    queryID,
    tag,
  },
})

interface GroupByTagAction {
  type: 'KAPA_GROUP_BY_TAG'
  payload: {
    queryID: string
    tagKey: string
  }
}
export const groupByTag = (
  queryID: string,
  tagKey: string
): GroupByTagAction => ({
  type: 'KAPA_GROUP_BY_TAG',
  payload: {
    queryID,
    tagKey,
  },
})

interface ToggleTagAcceptanceAction {
  type: 'KAPA_TOGGLE_TAG_ACCEPTANCE'
  payload: {
    queryID: string
  }
}
export const toggleTagAcceptance = (
  queryID: string
): ToggleTagAcceptanceAction => ({
  type: 'KAPA_TOGGLE_TAG_ACCEPTANCE',
  payload: {
    queryID,
  },
})

interface ToggleFieldAction {
  type: 'KAPA_TOGGLE_FIELD'
  payload: {
    queryID: string
    fieldFunc: Field
  }
}
export const toggleField = (
  queryID: string,
  fieldFunc: Field
): ToggleFieldAction => ({
  type: 'KAPA_TOGGLE_FIELD',
  payload: {
    queryID,
    fieldFunc,
  },
})

interface ApplyFuncsToFieldAction {
  type: 'KAPA_APPLY_FUNCS_TO_FIELD'
  payload: {
    queryID: string
    fieldFunc: ApplyFuncsToFieldArgs
  }
}
export const applyFuncsToField = (
  queryID: string,
  fieldFunc: ApplyFuncsToFieldArgs
): ApplyFuncsToFieldAction => ({
  type: 'KAPA_APPLY_FUNCS_TO_FIELD',
  payload: {
    queryID,
    fieldFunc,
  },
})

interface GroupByTimeAction {
  type: 'KAPA_GROUP_BY_TIME'
  payload: {
    queryID: string
    time: string
  }
}
export const groupByTime = (
  queryID: string,
  time: string
): GroupByTimeAction => ({
  type: 'KAPA_GROUP_BY_TIME',
  payload: {
    queryID,
    time,
  },
})

interface RemoveFuncsAction {
  type: 'KAPA_REMOVE_FUNCS'
  payload: {
    queryID: string
    fields: Field[]
  }
}
export const removeFuncs = (
  queryID: string,
  fields: Field[]
): RemoveFuncsAction => ({
  type: 'KAPA_REMOVE_FUNCS',
  payload: {
    queryID,
    fields,
  },
})

// TODO: shift may not be string
interface TimeShiftAction {
  type: 'KAPA_TIME_SHIFT'
  payload: {
    queryID: string
    shift: TimeShift
  }
}
// TODO: shift may not be string
export const timeShift = (
  queryID: string,
  shift: TimeShift
): TimeShiftAction => ({
  type: 'KAPA_TIME_SHIFT',
  payload: {
    queryID,
    shift,
  },
})
