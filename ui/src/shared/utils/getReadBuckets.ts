// Libraries
import {parse} from '@influxdata/flux-parser'
import {get, flatMap} from 'lodash'

// Utils
import {findNodes} from 'src/shared/utils/ast'

// Types
import {
  File,
  CallExpression,
  Property,
  StringLiteral,
  Identifier,
} from 'src/types'

/*
  Given a Flux script, return a list of names of buckets that are read from in
  the script.

  For now, this means detecting each time something like

      from(bucket: "foo")

  appears in the script.
*/
export const getReadBuckets = (text: string): string[] => {
  try {
    const ast: File = parse(text)

    // Find every `from(bucket: "foo")` call in the script
    const fromCalls: CallExpression[] = findNodes(
      ast,
      n => n.type === 'CallExpression' && get(n, 'callee.name') === 'from'
    )

    // Extract the `bucket: "foo"` part from each call
    const bucketProperties: Property[] = flatMap(fromCalls, call =>
      findNodes(
        call,
        n => n.type === 'Property' && (n.key as Identifier).name === 'bucket'
      )
    )

    // Extract the `foo` from each object property
    const bucketNames = bucketProperties.map(
      prop => (prop.value as StringLiteral).value
    )

    return bucketNames
  } catch (e) {
    console.error('Failed to find buckets read in flux script', e)
  }
}
