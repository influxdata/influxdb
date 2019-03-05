// Libraries
import {get} from 'lodash'

// APIs
import {client} from 'src/utils/api'

// Types
import {Package} from 'src/types/ast'

export const getAST = async (query: string): Promise<Package> => {
  try {
    const resp = await client.queries.ast(query)

    // TODO: update client Package and remove map
    return {
      type: 'Package',
      path: resp.path,
      files: resp.files,
      package: resp._package || '',
    }
  } catch (e) {
    const message = get(e, 'response.data.error')

    if (message) {
      throw new Error(message)
    } else {
      throw e
    }
  }
}
