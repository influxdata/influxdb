import uuid from 'uuid'
import _ from 'lodash'
import Walker from 'src/ifql/ast/walker'
import {FlatBody, Func} from 'src/types/ifql'

interface Body extends FlatBody {
  id: string
}

export const bodyNodes = (ast, suggestions): Body[] => {
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

const functions = (funcs, suggestions): Func[] => {
  const funcList = funcs.map(func => {
    const {params, name} = suggestions.find(f => f.name === func.name)
    const args = Object.entries(params).map(([key, type]) => {
      const value = _.get(func.args.find(arg => arg.key === key), 'value', '')

      return {
        key,
        value,
        type,
      }
    })

    return {
      id: uuid.v4(),
      source: func.source,
      name,
      args,
    }
  })

  return funcList
}
