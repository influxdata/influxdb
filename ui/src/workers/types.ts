export interface RequestMessage {
  id: string
  type: string
}

export interface ResponseMessage {
  id: string
  requestID: string
  result: 'error' | 'success'
  error?: string
}

export type Job = (msg: RequestMessage) => Promise<any>
