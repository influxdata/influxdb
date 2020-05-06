/*
  Given a source and target object, produce an object that is logically
  equivalent to the target. The produced object will preserve the reference
  identity of nodes in the source object whenever the path of the node matches
  the path of a logically equivalent node in the target.

  This helper enables memoization of functions whose inputs often change their
  reference identity, but rarely change their logical identity.

  More about logical and reference identity: a primitive object is one of the
  following:

  - number
  - string
  - boolean
  - function
  - `undefined`
  - `null`
    
  If `a` and `b` are primitive objects, then `a` and `b` are logically
  equivalent iff `a === b`. Two non-primitive objects are logically equivalent
  iff they have logically equivalent children.

  For example, let

  ```
  const foo = {a: [4, 5, {b: 'six'}]}
  const bar = {a: [4, 5, {b: 'six'}]}
  const baz = {a: [4, 5]}
  ```

  Then `foo` and `bar` are logically equivalent even though `foo !== bar` (i.e.
  `foo` and `bar` do not have the same reference identity).  The objects `foo`
  and `baz` are neither logically nor referentially identical.
  
*/
export const identityMerge = <S extends object, T extends object>(
  source: S,
  target: T
): T => {
  const wrappedSource = {root: source}
  const wrappedTarget = {root: target}
  const paths = enumeratePaths(wrappedTarget)
  const result: any = {}

  for (const path of paths) {
    const sourceValue = getByPath(wrappedSource, path)
    const targetValue = getByPath(wrappedTarget, path)

    if (isEqual(sourceValue, targetValue)) {
      setByPath(result, path, sourceValue)
    } else {
      setByPath(result, path, targetValue)
    }
  }

  return result.root
}

type Path = string[]

export const enumeratePaths = (
  target: any,
  pathToTarget: Path = [],
  acc: Path[] = []
): Path[] => {
  if (target === undefined || target === null || typeof target === 'string') {
    return acc
  }

  for (const key of Object.keys(target)) {
    const currentPath = [...pathToTarget, key]

    acc.push(currentPath)
    enumeratePaths(target[key], currentPath, acc)
  }

  return acc
}

export const getByPath = (target: any, path: Path): any => {
  if (!path.length) {
    return
  }

  try {
    let i = 0
    let currentTarget = target[path[i]]

    while (currentTarget !== undefined) {
      if (i === path.length - 1) {
        return currentTarget
      }

      i += 1
      currentTarget = currentTarget[path[i]]
    }
  } catch {}
}

export const setByPath = (target: any, path: Path, value: any): void => {
  if (!path.length) {
    throw new Error('invalid path')
  }

  if (typeof target !== 'object') {
    throw new Error('target of setByPath must be object')
  }

  if (path.length === 1) {
    target[path[0]] = value
  }

  let i = -1
  let currentTarget = target

  while (i < path.length - 2) {
    let nextTarget = currentTarget[path[i + 1]]

    if (typeof nextTarget !== 'object') {
      const isArrayKey = isNaN(+path[i + 1])

      currentTarget[path[i + 1]] = isArrayKey ? {} : []
      nextTarget = currentTarget[path[i + 1]]
    }

    i += 1
    currentTarget = nextTarget
  }

  currentTarget[path[i + 1]] = value
}

export const isEqual = (a: any, b: any): boolean => {
  if (a === null || a === undefined || b === null || b === undefined) {
    return a === b
  }

  switch (typeof b) {
    case 'number':
    case 'boolean':
    case 'string':
    case 'function':
      return a === b
    default: {
      const aKeys = Object.keys(a)
      const bKeys = Object.keys(b)

      return (
        aKeys.length === bKeys.length && aKeys.every(k => isEqual(a[k], b[k]))
      )
    }
  }
}
