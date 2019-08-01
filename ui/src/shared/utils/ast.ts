// Libraries
import {isObject, isArray} from 'lodash'

// Types
import {Node} from 'src/types'

/*
  Find all nodes in a tree matching the `predicate` function. Each node in the
  tree is an object, which may contain objects or arrays of objects as children
  under any key.
*/
export const findNodes = (
  node: any,
  predicate: (node: Node) => boolean,
  acc: any[] = []
) => {
  if (predicate(node)) {
    acc.push(node)
  }

  for (const value of Object.values(node)) {
    if (isObject(value)) {
      findNodes(value, predicate, acc)
    } else if (isArray(value)) {
      for (const innerValue of value) {
        findNodes(innerValue, predicate, acc)
      }
    }
  }

  return acc
}
