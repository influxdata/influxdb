// APIs
import {client} from 'src/utils/api'

// Types
import {CancelBox} from 'src/types/promises'
import {File} from '@influxdata/influx'

const MAX_RESPONSE_CHARS = 50000 * 160

export const runQuery = (
  orgID: string,
  query: string,
  extern?: File
): CancelBox<string> => {
  return client.queries.execute(orgID, query, {
    extern,
    limitChars: MAX_RESPONSE_CHARS,
  })
}
