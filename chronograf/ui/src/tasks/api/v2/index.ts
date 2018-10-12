import AJAX from 'src/utils/ajax'

export const submitNewTask = async (url, owner, org, flux) => {
  const request = {
    flux,
    organizationId: org.id,
    status: 'active',
    owner,
  }

  const {data} = await AJAX({url, data: request, method: 'POST'})

  return data
}

export const getUserTasks = async (url, user) => {
  const completeUrl = `${url}?user=${user.id}`

  const {data} = await AJAX({url: completeUrl})

  return data
}
