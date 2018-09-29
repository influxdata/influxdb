import _ from 'lodash'

import AJAX from 'src/utils/ajax'

export const getSetupStatus = async (
  url: string
): Promise<{isAllowed: boolean}> => {
  try {
    const {data} = await AJAX({
      method: 'GET',
      url,
    })
    const {isAllowed} = data

    return {isAllowed}
  } catch (error) {
    console.error("Can't get setup status", error)
    throw error
  }
}

export interface SetupParams {
  username: string
  password: string
  org: string
  bucket: string
}

export const setSetupParams = async (
  url: string,
  setupParams: SetupParams
): Promise<{data: object}> => {
  try {
    const {data} = await AJAX({
      method: 'POST',
      url,
      data: setupParams,
    })
    return {data}
  } catch (error) {
    console.error("Can't set setup parameters", error)
    throw error
  }
}
