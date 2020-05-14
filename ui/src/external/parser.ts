import {File} from 'src/types/ast'

/*
  NOTE: This is a work around for flux being generated (from rust) for the browser and jest tests running in
        a node environment (this is only for handling tests). If a test requires a specific AST result
        then you will need to mock that out in the test.
*/
export const parse = (script): File => {
  if (window) {
    return require('@influxdata/flux').parse(script)
  } else {
    return {
      type: 'File',
      package: {
        name: {
          name: 'fake',
          type: 'Identifier',
        },
        type: 'PackageClause',
      },
      imports: [],
      body: [],
    }
  }
}
