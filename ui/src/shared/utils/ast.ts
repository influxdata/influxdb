/*
  Find all nodes in a tree matching the `predicate` function. Each node in the
  tree is an object, which may contain objects or arrays of objects as children
  under any key.
*/
export const findNodes = (
  node: any,
  predicate: (node: any) => boolean,
  acc: any[] = []
) => {
  if (predicate(node)) {
    acc.push(node)
  }

  for (const value of Object.values(node)) {
    if (Object.prototype.toString.call(value) === '[object Array]') {
      for (const innerValue of value as Array<any>) {
        findNodes(innerValue, predicate, acc)
      }
    } else if (typeof value === 'object') {
      findNodes(value, predicate, acc)
    }
  }

  return acc
}
