// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'

// Types
import {FluxTable} from 'src/types'
import {SearchStatus} from 'src/types/logs'
import {queryAPI} from 'src/utils/api'
import {Query, Dialect} from 'src/api'

export interface QueryResponse {
  tables: FluxTable[]
  status: SearchStatus
}

export const executeQueryAsync = async (
  query: string,
  type: Query.TypeEnum = Query.TypeEnum.Influxql
): Promise<QueryResponse> => {
  try {
    const dialect = {
      header: true,
      annotations: [
        Dialect.AnnotationsEnum.Datatype,
        Dialect.AnnotationsEnum.Group,
        Dialect.AnnotationsEnum.Default,
      ],
      delimiter: ',',
    }

    const {data} = await queryAPI.queryPost(
      'text/csv',
      'application/json',
      null,
      null,
      {type, query, dialect}
    )

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
