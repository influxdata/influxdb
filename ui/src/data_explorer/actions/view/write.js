import {writeLineProtocol as writeLineProtocolAJAX} from 'src/data_explorer/apis'

import {errorThrown} from 'shared/actions/errors'
import {publishNotification} from 'shared/actions/notifications'

export const writeLineProtocolAsync = (source, db, data) => async dispatch => {
  try {
    await writeLineProtocolAJAX(source, db, data)
    dispatch(
      publishNotification({
        type: 'success',
        icon: 'checkmark',
        duration: 5000,
        message: 'Data was written successfully',
      })
    )
  } catch (response) {
    const errorMessage = `Write failed: ${response.data.error}`
    dispatch(errorThrown(response, errorMessage))
    throw response
  }
}
