import AJAX from 'src/utils/ajax'

export const createAuthorization = async authorization => {
  try {
    const {data} = await AJAX({
      method: 'POST',
      url: '/api/v2/authorizations',
      data: authorization,
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const addLabelToAuthorization = async (
  authID: string,
  labelID: string
) => {
  try {
    return await AJAX({
      method: 'POST',
      url: `/api/v2/authorizations/${authID}/labels`,
      data: {labelID},
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
