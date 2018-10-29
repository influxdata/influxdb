// Libraries
import {times} from 'lodash'
import uuid from 'uuid'

// Utils
import Deferred from 'src/utils/Deferred'
import DB from 'src/workers/Database'

// Types
import {RequestMessage, ResponseMessage} from 'src/workers/types'

const workerCount = navigator.hardwareConcurrency - 1 || 2

class JobManager {
  private currentIndex: number = 0
  private workers: Worker[] = []
  private jobs: {[key: string]: Deferred} = {}

  constructor() {
    times(workerCount, () => {
      const worker = new Worker('./worker.ts')

      worker.onmessage = this.handleResponse
      worker.onerror = this.handleError

      this.workers.push(worker)
    })
  }

  private handleResponse = async (e: {data: ResponseMessage}) => {
    const message = e.data
    const deferred = this.jobs[message.requestID]

    if (!deferred) {
      return
    }

    if (message.result === 'error') {
      deferred.reject(new Error(message.error))
      delete this.jobs[origin]

      return
    }

    try {
      const payload = await DB.get(message.id)
      await DB.del(message.id)
      deferred.resolve(payload)
    } catch (e) {
      deferred.reject(e)
    }

    delete this.jobs[origin]
  }

  private handleError = err => {
    console.error(err)
  }

  private get worker(): Worker {
    return this.workers[this.currentIndex]
  }

  private rotateWorker(): void {
    this.currentIndex += 1
    this.currentIndex %= workerCount
  }

  // @ts-ignore
  private postJobRequest = async (type: string, payload: any) => {
    const id = uuid.v1()
    const deferred = new Deferred()

    this.jobs[id] = deferred

    await DB.put(id, payload)

    const requestMessage: RequestMessage = {id, type}

    this.worker.postMessage(requestMessage)
    this.rotateWorker()

    return deferred.promise
  }
}

export const manager = new JobManager()
