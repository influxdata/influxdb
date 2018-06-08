import {proxy} from 'src/utils/queryUrlGenerator'
import {Namespace} from 'src/types'
import {TimeSeriesResponse} from 'src/types/series'

export const executeQueryAsync = async (
  proxyLink: string,
  namespace: Namespace,
  query: string
): Promise<TimeSeriesResponse> => {
  try {
    const {data} = await proxy({
      source: proxyLink,
      db: namespace.database,
      rp: namespace.retentionPolicy,
      query,
      tempVars: [],
      resolution: null,
    })

    return data
  } catch (error) {
    throw error
  }
}
