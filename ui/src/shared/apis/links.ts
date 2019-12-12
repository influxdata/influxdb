import {request} from 'src/client'
import {Links} from 'src/types/links'

export const getLinks = async (): Promise<Links> => {
  const resp = await request('GET', '/api/v2')

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  return resp.data
}
