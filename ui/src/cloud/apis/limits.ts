import {request} from 'src/client'
import {Limits, LimitsStatus} from 'src/types'

export const getReadWriteCardinalityLimits = async (
  orgID: string
): Promise<LimitsStatus> => {
  const resp = await request(
    'GET',
    `/api/v2private/orgs/${orgID}/limits/status`,
  )

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  return resp.data
}

export const getLimits = async (orgID: string): Promise<Limits> => {
  const resp = await request(
    'GET',
    `/api/v2private/orgs/${orgID}/limits`,
  )

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  return resp.data
}
