import {toString} from './ast'

const InfluxQL = ast => {
  return {
    toString: () => toString(ast),
  }
}

export default InfluxQL
