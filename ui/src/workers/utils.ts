import DB from 'src/workers/Database'
import uuid from 'uuid'

import {RequestMessage, ResponseMessage} from 'src/workers/types'

export const postSuccessResponse = async (requestID: string, payload: any) => {
  const id = uuid.v1()

  await DB.put(id, payload)

  const response: ResponseMessage = {
    id,
    requestID,
    result: 'success',
  }

  postMessage(response)
}

export const postErrorResponse = (requestID: string, err: Error) => {
  const id = uuid.v1()

  const response: ResponseMessage = {
    id,
    requestID,
    result: 'error',
    error: err.toString(),
  }

  postMessage(response)
}

export const removeData = async (msg: RequestMessage): Promise<void> => {
  await DB.del(msg.id)
}
