// Libraries
import _ from 'lodash'

// Utils
import {sourcesAPI} from 'src/utils/api'

export interface SetupParams {
  username: string
  password: string
  org: string
  bucket: string
}

export const trySources = async (): Promise<boolean> => {
  try {
    await sourcesAPI.sourcesGet('')
    return true
  } catch (error) {
    console.error('Sign in has failed', error)
    return false
  }
}
