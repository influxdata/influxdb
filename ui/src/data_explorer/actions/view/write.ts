import {writeLineProtocol as writeLineProtocolAJAX} from 'src/data_explorer/apis'

import {notify} from 'src/shared/actions/notifications'
import {Source} from 'src/types'

import {
  notifyDataWritten,
  notifyDataWriteFailed,
} from 'src/shared/copy/notifications'

export const writeLineProtocolAsync = (
  source: Source,
  db: string,
  data: string
) => async (dispatch): Promise<void> => {
  try {
    await writeLineProtocolAJAX(source, db, data)
    dispatch(notify(notifyDataWritten()))
  } catch (response) {
    dispatch(notify(notifyDataWriteFailed(response.data.error)))
    throw response
  }
}
