import uuid from 'uuid'
import _ from 'lodash'

import Walker from 'src/flux/ast/walker'

import {FlatBody, Func, Suggestion} from 'src/types/flux'

interface Body extends FlatBody {
  id: string
}

export const bodyNodes = (ast, suggestions: Suggestion[]): Body[] => {
  if (!ast) {
    return []
  }

  const walker = new Walker(ast)

  const body = walker.body.map(b => {
    const {type} = b
    const id = uuid.v4()
    if (type.includes('Variable')) {
      const declarations = b.declarations.map(d => {
        if (!d.funcs) {
          return {...d, id: uuid.v4()}
        }

        return {
          ...d,
          id: uuid.v4(),
          funcs: functions(d.funcs, suggestions),
        }
      })

      return {...b, type, id, declarations}
    }

    const {funcs, source} = b

    return {
      id,
      funcs: functions(funcs, suggestions),
      declarations: [],
      type,
      source,
    }
  })

  return body
}

const functions = (funcs: Func[], suggestions: Suggestion[]): Func[] => {
  const funcList = funcs.map(func => {
    const suggestion = suggestions.find(f => f.name === func.name)
    if (!suggestion) {
      return {
        type: func.type,
        id: uuid.v4(),
        source: func.source,
        name: func.name,
        args: func.args,
      }
    }

    const {params, name} = suggestion
    const args = Object.entries(params).map(([key, type]) => {
      const argWithKey = func.args.find(arg => arg.key === key)
      const value = _.get(argWithKey, 'value', '')

      return {
        key,
        value,
        type,
      }
    })

    return {
      type: func.type,
      id: uuid.v4(),
      source: func.source,
      name,
      args,
    }
  })

  return funcList
}
