import {postSuccessResponse, postErrorResponse} from 'src/workers/utils'

import {RequestMessage, Job} from 'src/workers/types'

const jobMapping: {[key: string]: Job} = {}

const errorJob = async (data: RequestMessage) => {
  postErrorResponse(data.id, new Error('Unknown job type'))
}

onmessage = async (workerMessage: {data: RequestMessage}) => {
  const message = workerMessage.data
  const job: Job = jobMapping[message.type] || errorJob

  try {
    const result = await job(message)

    postSuccessResponse(message.id, result)
  } catch (e) {
    postErrorResponse(message.id, e)
  }
}
