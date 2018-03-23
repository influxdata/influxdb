import {writeLineProtocol as writeLineProtocolAJAX} from 'src/data_explorer/apis'

import {errorThrown} from 'shared/actions/errors'
import {notify} from 'shared/actions/notifications'

import {notifyDataWritten} from 'shared/copy/notifications'

export const writeLineProtocolAsync = (source, db, data) => async dispatch => {
  try {
    await writeLineProtocolAJAX(source, db, data)
    dispatch(notify(notifyDataWritten()))
  } catch (response) {
    const errorMessage = `Write failed: ${response.data.error}`
    dispatch(errorThrown(response, errorMessage))
    throw response
  }
}
