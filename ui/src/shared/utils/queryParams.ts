import {browserHistory} from 'react-router'
import qs from 'qs'
import {pickBy} from 'lodash'

export const readQueryParams = (): {[key: string]: any} => {
  return qs.parse(window.location.search, {ignoreQueryPrefix: true})
}

/*
  Given an object of query parameter keys and values, updates any corresponding
  query parameters in the URL to match. If the supplied object has a null value
  for a key, that query parameter will be removed from the URL altogether.
*/
export const updateQueryParams = (updatedQueryParams: object): void => {
  const currentQueryString = window.location.search
  const newQueryParams = pickBy(
    {
      ...qs.parse(currentQueryString, {ignoreQueryPrefix: true}),
      ...updatedQueryParams,
    },
    v => !!v
  )

  const newQueryString = qs.stringify(newQueryParams)

  browserHistory.replace(`${window.location.pathname}?${newQueryString}`)
}

// NOTE: qs was giving me issues of not properly parsing objects
// in the search params string, so i made this thing using host
// vars to do the same thing

// Util function that parses out a shallow object / array from
// the url search params
export function parseURLVariables(searchString: string): {[key: string]: any} {
  if (!searchString) {
    return {}
  }

  const urlSearch = new URLSearchParams(searchString) as any
  const output = {}
  let ni, breakup, varKey

  for (ni of urlSearch.entries()) {
    if (!/([^\[])+\[.*\]\s*$/.test(ni[0])) {
      output[ni[0]] = ni[1]
      continue
    }

    breakup = /(([^\[])+)\[(.*)\]\s*$/.exec(ni[0])
    varKey = breakup[1]

    if (!output.hasOwnProperty(varKey)) {
      if (!breakup[3]) {
        output[varKey] = []
      } else {
        output[varKey] = {}
      }
    }

    if (breakup[3]) {
      // had a case of empty object property being first
      if (Array.isArray(output[varKey])) {
        output[varKey] = {
          '': output[varKey],
        }
      }

      output[varKey][breakup[3]] = ni[1]
      continue
    }

    // got a blank object property
    if (!Array.isArray(output[varKey])) {
      output[varKey][''] = ni[1]
      continue
    }

    output[varKey].push(ni[1])
  }

  return output
}

export function stringifyURLVariables(varObject) {
  if (!varObject) {
    return ''
  }

  return Object.keys(varObject)
    .reduce((prev, curr) => {
      if (Array.isArray(varObject[curr])) {
        return prev.concat(
          varObject[curr].map(v => ({key: `${curr}[]`, value: v}))
        )
      }

      if (typeof varObject[curr] === 'object') {
        return prev.concat(
          Object.entries(varObject[curr]).map(([k, v]) => ({
            key: `${curr}[${k}]`,
            value: v,
          }))
        )
      }

      return prev.concat([{key: curr, value: varObject[curr]}])
    }, [])
    .reduce((prev, curr) => {
      prev.append(curr.key, curr.value)
      return prev
    }, new URLSearchParams())
    .toString()
}
