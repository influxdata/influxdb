import AJAX from 'src/utils/ajax'
import {Limits, LimitsStatus} from 'src/types'

export const getReadWriteCardinalityLimits = async (
  orgID: string
): Promise<LimitsStatus> => {
  try {
    const {data} = await AJAX({
      method: 'GET',
      url: `/api/v2private/orgs/${orgID}/limits/status`,
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getLimits = async (orgID: string): Promise<Limits> => {
  try {
    const {data} = await AJAX({
      method: 'GET',
      url: `/api/v2private/orgs/${orgID}/limits`,
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}
