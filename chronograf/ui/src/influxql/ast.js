import _ from 'lodash'

// TODO make recursive
const exprStr = ({expr, val, type}) => {
  if (expr === 'reference') {
    if (val === 'time') {
      return val
    }
    return `"${val}"`
  } else if (expr === 'literal' || expr === '"string"') {
    if (type === 'regex') {
      return `${val}` // TODO add slashes `/${val}/`
    } else if (type === 'list') {
      throw new Error() // TODO list
    } else if (type === 'string') {
      return `'${val}'`
    } else {
      // types: boolean, number, integer, duration, time
      return val
    }
  } else if (expr === 'wildcard') {
    return val
  }
}

const recurse = root => {
  const {expr} = root

  if (expr === 'binary') {
    const {op, lhs, rhs} = root
    return `${recurse(lhs)} ${op} ${recurse(rhs)}`
  } else if (expr === 'call') {
    const {name, args} = root
    if (!args) {
      return `${name}()`
    }
    return `${name}(${args.map(recurse).join(', ')})`
  }

  return exprStr(root)
}

export const toString = ast => {
  const {fields, sources, condition, groupBy, orderbys, limits} = ast

  const strs = ['SELECT']

  // SELECT
  const flds = []
  for (const field of fields) {
    const {column, alias} = field
    const result = recurse(column)
    flds.push(alias ? `${result} AS "${alias}"` : result)
  }
  strs.push(flds.join(', '))

  // FROM
  if (sources.length) {
    strs.push('FROM')
    const srcs = []
    for (const source of sources) {
      // TODO subquery (type)
      const {database, retentionPolicy, name} = source
      srcs.push(`"${_.compact([database, retentionPolicy, name]).join('"."')}"`)
    }
    strs.push(srcs.join(', '))
  }

  // WHERE
  if (condition) {
    strs.push('WHERE')
    const result = recurse(condition)
    strs.push(result)
  }

  // GROUP BY
  if (groupBy) {
    strs.push('GROUP BY')

    const dimensions = []
    const {time, tags, fill} = groupBy
    if (time) {
      const {interval, offset} = time
      // _.compact([interval, offset]).join(', ')
      dimensions.push(`time(${_.compact([interval, offset]).join(', ')})`)
    }

    if (tags) {
      strs.push(dimensions.concat(tags).join(','))
    } else {
      strs.push(dimensions.join(','))
    }

    if (fill) {
      strs.push(`fill(${fill})`)
    }
  }

  // ORDER BY
  if (orderbys && orderbys.length) {
    strs.push('ORDER BY')
    strs.push(
      orderbys
        .map(({name, order}) => {
          return `${name} ${order === 'descending' ? 'DESC' : 'ASC'}`
        })
        .join(',')
    )
  }

  // LIMIT
  if (limits) {
    const {limit, offset, slimit, soffset} = limits
    if (limit) {
      strs.push(`LIMIT ${limit}`)
    }
    if (offset) {
      strs.push(`OFFSET ${offset}`)
    }
    if (slimit) {
      strs.push(`SLIMIT ${slimit}`)
    }
    if (soffset) {
      strs.push(`SOFFSET ${soffset}`)
    }
  }

  return strs.join(' ')
}
