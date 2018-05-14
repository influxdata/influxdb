import uuid from 'uuid'

import {getQueryConfigAndStatus} from 'src/shared/apis'

import {errorThrown} from 'src/shared/actions/errors'
import {
  QueryConfig,
  Status,
  Field,
  GroupBy,
  Tag,
  TimeRange,
  TimeShift,
  ApplyFuncsToFieldArgs,
} from 'src/types'

export type Action =
  | ActionAddQuery
  | ActionDeleteQuery
  | ActionToggleField
  | ActionGroupByTime
  | ActionFill
  | ActionRemoveFuncs
  | ActionApplyFuncsToField
  | ActionChooseTag
  | ActionChooseNamspace
  | ActionChooseMeasurement
  | ActionEditRawText
  | ActionSetTimeRange
  | ActionGroupByTime
  | ActionToggleField
  | ActionUpdateRawQuery
  | ActionQueryConfig
  | ActionTimeShift
  | ActionToggleTagAcceptance
  | ActionToggleField
  | ActionGroupByTag
  | ActionEditQueryStatus
  | ActionAddInitialField

export interface ActionAddQuery {
  type: 'DE_ADD_QUERY'
  payload: {
    queryID: string
  }
}

export const addQuery = (queryID: string = uuid.v4()): ActionAddQuery => ({
  type: 'DE_ADD_QUERY',
  payload: {
    queryID,
  },
})

interface ActionDeleteQuery {
  type: 'DE_DELETE_QUERY'
  payload: {
    queryID: string
  }
}

export const deleteQuery = (queryID: string): ActionDeleteQuery => ({
  type: 'DE_DELETE_QUERY',
  payload: {
    queryID,
  },
})

interface ActionToggleField {
  type: 'DE_TOGGLE_FIELD'
  payload: {
    queryID: string
    fieldFunc: Field
  }
}

export const toggleField = (
  queryID: string,
  fieldFunc: Field
): ActionToggleField => ({
  type: 'DE_TOGGLE_FIELD',
  payload: {
    queryID,
    fieldFunc,
  },
})

interface ActionGroupByTime {
  type: 'DE_GROUP_BY_TIME'
  payload: {
    queryID: string
    time: string
  }
}

export const groupByTime = (
  queryID: string,
  time: string
): ActionGroupByTime => ({
  type: 'DE_GROUP_BY_TIME',
  payload: {
    queryID,
    time,
  },
})

interface ActionFill {
  type: 'DE_FILL'
  payload: {
    queryID: string
    value: string
  }
}

export const fill = (queryID: string, value: string): ActionFill => ({
  type: 'DE_FILL',
  payload: {
    queryID,
    value,
  },
})

interface ActionRemoveFuncs {
  type: 'DE_REMOVE_FUNCS'
  payload: {
    queryID: string
    fields: Field[]
    groupBy: GroupBy
  }
}

export const removeFuncs = (
  queryID: string,
  fields: Field[],
  groupBy: GroupBy
): ActionRemoveFuncs => ({
  type: 'DE_REMOVE_FUNCS',
  payload: {
    queryID,
    fields,
    groupBy,
  },
})

interface ActionApplyFuncsToField {
  type: 'DE_APPLY_FUNCS_TO_FIELD'
  payload: {
    queryID: string
    fieldFunc: ApplyFuncsToFieldArgs
    groupBy: GroupBy
  }
}

export const applyFuncsToField = (
  queryID: string,
  fieldFunc: ApplyFuncsToFieldArgs,
  groupBy?: GroupBy
): ActionApplyFuncsToField => ({
  type: 'DE_APPLY_FUNCS_TO_FIELD',
  payload: {
    queryID,
    fieldFunc,
    groupBy,
  },
})

interface ActionChooseTag {
  type: 'DE_CHOOSE_TAG'
  payload: {
    queryID: string
    tag: Tag
  }
}

export const chooseTag = (queryID: string, tag: Tag): ActionChooseTag => ({
  type: 'DE_CHOOSE_TAG',
  payload: {
    queryID,
    tag,
  },
})

interface ActionChooseNamspace {
  type: 'DE_CHOOSE_NAMESPACE'
  payload: {
    queryID: string
    database: string
    retentionPolicy: string
  }
}

interface DBRP {
  database: string
  retentionPolicy: string
}

export const chooseNamespace = (
  queryID: string,
  {database, retentionPolicy}: DBRP
): ActionChooseNamspace => ({
  type: 'DE_CHOOSE_NAMESPACE',
  payload: {
    queryID,
    database,
    retentionPolicy,
  },
})

interface ActionChooseMeasurement {
  type: 'DE_CHOOSE_MEASUREMENT'
  payload: {
    queryID: string
    measurement: string
  }
}

export const chooseMeasurement = (
  queryID: string,
  measurement: string
): ActionChooseMeasurement => ({
  type: 'DE_CHOOSE_MEASUREMENT',
  payload: {
    queryID,
    measurement,
  },
})

interface ActionEditRawText {
  type: 'DE_EDIT_RAW_TEXT'
  payload: {
    queryID: string
    rawText: string
  }
}

export const editRawText = (
  queryID: string,
  rawText: string
): ActionEditRawText => ({
  type: 'DE_EDIT_RAW_TEXT',
  payload: {
    queryID,
    rawText,
  },
})

interface ActionSetTimeRange {
  type: 'DE_SET_TIME_RANGE'
  payload: {
    bounds: TimeRange
  }
}

export const setTimeRange = (bounds: TimeRange): ActionSetTimeRange => ({
  type: 'DE_SET_TIME_RANGE',
  payload: {
    bounds,
  },
})

interface ActionGroupByTag {
  type: 'DE_GROUP_BY_TAG'
  payload: {
    queryID: string
    tagKey: string
  }
}

export const groupByTag = (
  queryID: string,
  tagKey: string
): ActionGroupByTag => ({
  type: 'DE_GROUP_BY_TAG',
  payload: {
    queryID,
    tagKey,
  },
})

interface ActionToggleTagAcceptance {
  type: 'DE_TOGGLE_TAG_ACCEPTANCE'
  payload: {
    queryID: string
  }
}

export const toggleTagAcceptance = (
  queryID: string
): ActionToggleTagAcceptance => ({
  type: 'DE_TOGGLE_TAG_ACCEPTANCE',
  payload: {
    queryID,
  },
})

interface ActionUpdateRawQuery {
  type: 'DE_UPDATE_RAW_QUERY'
  payload: {
    queryID: string
    text: string
  }
}

export const updateRawQuery = (
  queryID: string,
  text: string
): ActionUpdateRawQuery => ({
  type: 'DE_UPDATE_RAW_QUERY',
  payload: {
    queryID,
    text,
  },
})

interface ActionQueryConfig {
  type: 'DE_UPDATE_QUERY_CONFIG'
  payload: {
    config: QueryConfig
  }
}

export const updateQueryConfig = (config: QueryConfig): ActionQueryConfig => ({
  type: 'DE_UPDATE_QUERY_CONFIG',
  payload: {
    config,
  },
})

interface ActionAddInitialField {
  type: 'DE_ADD_INITIAL_FIELD'
  payload: {
    queryID: string
    field: Field
    groupBy?: GroupBy
  }
}

export const addInitialField = (
  queryID: string,
  field: Field,
  groupBy: GroupBy
): ActionAddInitialField => ({
  type: 'DE_ADD_INITIAL_FIELD',
  payload: {
    queryID,
    field,
    groupBy,
  },
})

interface ActionEditQueryStatus {
  type: 'DE_EDIT_QUERY_STATUS'
  payload: {
    queryID: string
    status: Status
  }
}

export const editQueryStatus = (
  queryID: string,
  status: Status
): ActionEditQueryStatus => ({
  type: 'DE_EDIT_QUERY_STATUS',
  payload: {
    queryID,
    status,
  },
})

interface ActionTimeShift {
  type: 'DE_TIME_SHIFT'
  payload: {
    queryID: string
    shift: TimeShift
  }
}

export const timeShift = (
  queryID: string,
  shift: TimeShift
): ActionTimeShift => ({
  type: 'DE_TIME_SHIFT',
  payload: {
    queryID,
    shift,
  },
})

// Async actions
export const editRawTextAsync = (
  url: string,
  id: string,
  text: string
) => async (dispatch): Promise<void> => {
  try {
    const {data} = await getQueryConfigAndStatus(url, [
      {
        query: text,
        id,
      },
    ])
    const config = data.queries.find(q => q.id === id)
    dispatch(updateQueryConfig(config.queryConfig))
  } catch (error) {
    dispatch(errorThrown(error))
  }
}
