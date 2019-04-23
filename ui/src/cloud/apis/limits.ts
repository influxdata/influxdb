import AJAX from 'src/utils/ajax'

export const getReadWriteLimitsAJAX = async (orgID: string) => {
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

export const getLimitsAJAX = async (orgID: string) => {
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
