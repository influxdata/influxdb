// Utils
import AJAX from 'src/utils/ajax'
import {parseResponse} from 'src/shared/parsing/flux/response'

// Types
import {InfluxLanguages} from 'src/types/v2/dashboards'
import {FluxTable} from 'src/types'
import {SearchStatus} from 'src/types/logs'

export interface QueryResponse {
  tables: FluxTable[]
  status: SearchStatus
}

export const executeQueryAsync = async (
  link: string,
  query: string
): Promise<QueryResponse> => {
  try {
    const dialect = {
      header: true,
      annotations: ['datatype', 'group', 'default'],
      delimiter: ',',
    }

    const {data} = await AJAX({
      method: 'POST',
      url: link,
      data: {
        type: InfluxLanguages.Flux,
        query,
        dialect,
      },
    })

    const tables = parseResponse(data)
    const status = responseStatus(tables)

    return {tables, status}
  } catch (error) {
    console.error(error)
    return {
      tables: [],
      status: SearchStatus.SourceError,
    }
  }
}

const responseStatus = (tables: FluxTable[]): SearchStatus => {
  if (tables.length === 0) {
    return SearchStatus.NoResults
  } else if (tables[0].name === 'Error') {
    return SearchStatus.SourceError
  } else {
    return SearchStatus.Loaded
  }
}
