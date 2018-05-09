import AJAX from 'utils/ajax'

export function getSources() {
  return AJAX({
    resource: 'sources',
  })
}

export function getSource(id) {
  return AJAX({
    resource: 'sources',
    id,
  })
}

export function createSource(attributes) {
  return AJAX({
    resource: 'sources',
    method: 'POST',
    data: attributes,
  })
}

export function updateSource(newSource) {
  return AJAX({
    url: newSource.links.self,
    method: 'PATCH',
    data: newSource,
  })
}

export function deleteSource(source) {
  return AJAX({
    url: source.links.self,
    method: 'DELETE',
  })
}

export const pingKapacitor = async kapacitor => {
  try {
    const data = await AJAX({
      method: 'GET',
      url: kapacitor.links.ping,
    })
    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getKapacitor = async (source, kapacitorID) => {
  try {
    const {data} = await AJAX({
      url: `${source.links.kapacitors}/${kapacitorID}`,
      method: 'GET',
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getActiveKapacitor = async source => {
  try {
    const {data} = await AJAX({
      url: source.links.kapacitors,
      method: 'GET',
    })

    const activeKapacitor = data.kapacitors.find(k => k.active)
    return activeKapacitor || data.kapacitors[0]
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getKapacitors = async source => {
  try {
    return await AJAX({
      method: 'GET',
      url: source.links.kapacitors,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const deleteKapacitor = async kapacitor => {
  try {
    return await AJAX({
      method: 'DELETE',
      url: kapacitor.links.self,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export function createKapacitor(
  source,
  {url, name = 'My Kapacitor', username, password, insecureSkipVerify}
) {
  return AJAX({
    url: source.links.kapacitors,
    method: 'POST',
    data: {
      name,
      url,
      username,
      password,
      insecureSkipVerify,
    },
  })
}

export function updateKapacitor({
  links,
  url,
  name = 'My Kapacitor',
  username,
  password,
  active,
  insecureSkipVerify,
}) {
  return AJAX({
    url: links.self,
    method: 'PATCH',
    data: {
      name,
      url,
      username,
      password,
      active,
      insecureSkipVerify,
    },
  })
}

export const getKapacitorConfig = async kapacitor => {
  try {
    return await kapacitorProxy(kapacitor, 'GET', '/kapacitor/v1/config', '')
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getKapacitorConfigSection = (kapacitor, section) => {
  return kapacitorProxy(kapacitor, 'GET', `/kapacitor/v1/config/${section}`, '')
}

<<<<<<< HEAD
export function updateKapacitorConfigSection(kapacitor, section, properties) {
  const params = {
=======
export function updateKapacitorConfigSection(
  kapacitor,
  section,
  properties,
  specificConfig
) {
  const config = specificConfig || ''
  const path = `/kapacitor/v1/config/${section}/${config}`

  return AJAX({
>>>>>>> Change updateKapacitorConfigSection function so that if a specific config is passed, it updates that specific config and not just the default
    method: 'POST',
    url: kapacitor.links.proxy,
    params: {
      path,
    },
    data: {
      set: properties,
    },
    headers: {
      'Content-Type': 'application/json',
    },
  }

  return AJAX(params)
}

export function addKapacitorConfigInSection(kapacitor, section, properties) {
  return AJAX({
    method: 'POST',
    url: kapacitor.links.proxy,
    params: {
      path: `/kapacitor/v1/config/${section}/`,
    },
    data: {
      add: properties,
    },
    headers: {
      'Content-Type': 'application/json',
    },
  })
}

export const testAlertOutput = async (kapacitor, outputName, options) => {
  try {
    const {
      data: {services},
    } = await kapacitorProxy(kapacitor, 'GET', '/kapacitor/v1/service-tests')
    const service = services.find(s => s.name === outputName)
    return kapacitorProxy(kapacitor, 'POST', service.link.href, options)
  } catch (error) {
    console.error(error)
  }
}

export const getAllServices = async kapacitor => {
  try {
    const {
      data: {services},
    } = await kapacitorProxy(kapacitor, 'GET', '/kapacitor/v1/service-tests')
    return services
  } catch (error) {
    console.error(error)
  }
}

export function createKapacitorTask(kapacitor, id, type, dbrps, script) {
  return kapacitorProxy(kapacitor, 'POST', '/kapacitor/v1/tasks', {
    id,
    type,
    dbrps,
    script,
    status: 'enabled',
  })
}

export function enableKapacitorTask(kapacitor, id) {
  return kapacitorProxy(kapacitor, 'PATCH', `/kapacitor/v1/tasks/${id}`, {
    status: 'enabled',
  })
}

export function disableKapacitorTask(kapacitor, id) {
  return kapacitorProxy(kapacitor, 'PATCH', `/kapacitor/v1/tasks/${id}`, {
    status: 'disabled',
  })
}

export function deleteKapacitorTask(kapacitor, id) {
  return kapacitorProxy(kapacitor, 'DELETE', `/kapacitor/v1/tasks/${id}`, '')
}

export function kapacitorProxy(kapacitor, method, path, body) {
  return AJAX({
    method,
    url: kapacitor.links.proxy,
    params: {
      path,
    },
    data: body,
  })
}

export const getQueryConfigAndStatus = (url, queries, tempVars) =>
  AJAX({
    url,
    method: 'POST',
    data: {queries, tempVars},
  })
