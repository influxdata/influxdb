import {writeLineProtocol as writeLineProtocolAJAX} from 'src/data_explorer/apis'

import {errorThrown} from 'shared/actions/errors'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

export const writeLineProtocolAsync = (source, db, data) => async dispatch => {
  try {
    await writeLineProtocolAJAX(source, db, data)
    dispatch(
      publishAutoDismissingNotification(
        'success',
        'Data was written successfully'
      )
    )
  } catch (response) {
    const errorMessage = `Write failed: ${response.data.error}`
    dispatch(errorThrown(response, errorMessage))
    throw response
  }
}
