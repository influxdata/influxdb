import {writeLineProtocol as writeLineProtocolAJAX} from 'src/data_explorer/apis'

import {errorThrown} from 'shared/actions/errors'
import {publishNotification} from 'shared/actions/notifications'

import {dataExplorerWriteSuccess} from 'shared/copy/notificationsCopy'

export const writeLineProtocolAsync = (source, db, data) => async dispatch => {
  try {
    await writeLineProtocolAJAX(source, db, data)
    dispatch(publishNotification(dataExplorerWriteSuccess))
  } catch (response) {
    const errorMessage = `Write failed: ${response.data.error}`
    dispatch(errorThrown(response, errorMessage))
    throw response
  }
}
