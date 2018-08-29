import {toString} from './ast'

const InfluxQL = ast => {
  return {
    // select: () =>
    toString: () => toString(ast),
  }
}

export default InfluxQL
