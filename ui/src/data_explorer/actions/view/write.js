import {writeLineProtocol as writeLineProtocolAJAX} from 'src/data_explorer/apis'

import {notify} from 'shared/actions/notifications'

import {
  notifyDataWritten,
  notifyDataWriteFailed,
} from 'shared/copy/notifications'

export const writeLineProtocolAsync = (source, db, data) => async dispatch => {
  try {
    await writeLineProtocolAJAX(source, db, data)
    dispatch(notify(notifyDataWritten()))
  } catch (response) {
    dispatch(notify(notifyDataWriteFailed(response.data.error)))
    throw response
  }
}
