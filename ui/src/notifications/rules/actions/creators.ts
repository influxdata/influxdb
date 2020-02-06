// Types
import {RemoteDataState, RuleEntities} from 'src/types'
import {NormalizedSchema} from 'normalizr'
import {setLabelOnResource} from 'src/labels/actions/creators'

export type Action =
  | ReturnType<typeof setRules>
  | ReturnType<typeof setRule>
  | ReturnType<typeof setCurrentRule>
  | ReturnType<typeof removeRule>
  | ReturnType<typeof removeLabelFromRule>
  | ReturnType<typeof setLabelOnResource>

export const SET_RULES = 'SET_RULES'
export const SET_RULE = 'SET_RULE'
export const SET_CURRENT_RULE = 'SET_CURRENT_RULE'
export const REMOVE_RULE = 'REMOVE_RULE'
export const REMOVE_LABEL_FROM_RULE = 'REMOVE_LABEL_FROM_RULE'

type RulesSchema<R extends string | string[]> = NormalizedSchema<
  RuleEntities,
  R
>
export const setRules = (
  status: RemoteDataState,
  schema?: RulesSchema<string[]>
) =>
  ({
    type: SET_RULES,
    status,
    schema,
  } as const)

export const setRule = (
  id: string,
  status: RemoteDataState,
  schema?: RulesSchema<string>
) =>
  ({
    type: SET_RULE,
    id,
    status,
    schema,
  } as const)

export const setCurrentRule = (
  status: RemoteDataState,
  schema?: RulesSchema<string>
) =>
  ({
    type: SET_CURRENT_RULE,
    status,
    schema,
  } as const)

export const removeRule = (id: string) =>
  ({
    type: REMOVE_RULE,
    id,
  } as const)

export const removeLabelFromRule = (ruleID: string, labelID: string) =>
  ({
    type: REMOVE_LABEL_FROM_RULE,
    ruleID,
    labelID,
  } as const)
