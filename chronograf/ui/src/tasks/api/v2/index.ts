import AJAX from 'src/utils/ajax'

export const submitNewTask = (url, owner, org, flux) => {
  const data = {
    flux,
    organizationId: org.id,
    status: 'enabled',
    owner,
  }

  return AJAX({url, data, method: 'POST'})
}
