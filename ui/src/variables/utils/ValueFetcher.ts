// APIs
import {runQuery} from 'src/shared/apis/query'

// Utils
import {resolveSelectedKey} from 'src/variables/utils/resolveSelectedValue'
import {formatVarsOption} from 'src/variables/utils/formatVarsOption'
import {parseResponse} from 'src/shared/parsing/flux/response'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'

// Types
import {VariableAssignment, VariableValues, FluxColumnType} from 'src/types'
import {CancelBox} from 'src/types/promises'

const cacheKey = (
  url: string,
  orgID: string,
  query: string,
  variables: VariableAssignment[]
): string => {
  return `${query}\n\n${formatVarsOption(variables)}\n\n${orgID}\n\n${url}`
}

/*
  Given the CSV response for a Flux query, get the set of values from the first
  `_value` column in the response, as well as the column type of these values
  and a choice of selected value.

  The selected value must exist in the returned values for the response. We
  will first try to use the `prevSelection`, then the `defaultSelection`,
  before finally falling back to the first value returned in the response.
*/
export const extractValues = (
  csv: string,
  prevSelection?: string,
  defaultSelection?: string
): VariableValues => {
  const [table] = parseResponse(csv)

  if (!table || !table.data.length) {
    return {
      values: [],
      valueType: 'string',
      selected: [],
    }
  }

  const [headerRow] = table.data
  const valueColIndex = headerRow.indexOf('_value')

  if (valueColIndex === -1) {
    throw new Error("variable response does not contain a '_value' column")
  }

  let values = table.data.slice(1).map(row => row[valueColIndex])

  values = [...new Set(values)]
  values.sort()

  return {
    values,
    valueType: table.dataTypes._value as FluxColumnType,
    selected: [resolveSelectedKey(values, prevSelection, defaultSelection)],
  }
}

export interface ValueFetcher {
  fetch: (
    url: string,
    orgID: string,
    query: string,
    variables: VariableAssignment[],
    prevSelection: string,
    defaultSelection: string,
    skipCache: boolean
  ) => CancelBox<VariableValues>
}

export class DefaultValueFetcher implements ValueFetcher {
  private cache: {[cacheKey: string]: VariableValues} = {}

  public fetch(
    url,
    orgID,
    query,
    variables,
    prevSelection,
    defaultSelection,
    skipCache
  ) {
    const key = cacheKey(url, orgID, query, variables)
    if (!skipCache) {
      const cachedValues = this.cachedValues(
        key,
        prevSelection,
        defaultSelection
      )

      if (cachedValues) {
        return {promise: Promise.resolve(cachedValues), cancel: () => {}}
      }
    }

    const extern = buildVarsOption(variables)
    const request = runQuery(orgID, query, extern)

    const promise = request.promise.then(result => {
      if (result.type !== 'SUCCESS') {
        return Promise.reject(result.message)
      }

      const values = extractValues(result.csv, prevSelection, defaultSelection)

      this.cache[key] = values

      return values
    })

    return {
      promise,
      cancel: request.cancel,
    }
  }

  private cachedValues(
    key: string,
    prevSelection: string,
    defaultSelection: string
  ): VariableValues {
    const cachedValues = this.cache[key]

    if (!cachedValues) {
      return null
    }

    return {
      ...cachedValues,
      selected: [
        resolveSelectedKey(
          cachedValues.values as string[],
          prevSelection,
          defaultSelection
        ),
      ],
    }
  }
}

export const valueFetcher = new DefaultValueFetcher()
