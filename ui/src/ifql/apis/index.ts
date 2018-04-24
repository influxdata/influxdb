import AJAX from 'src/utils/ajax'

export const getSuggestions = async (url: string) => {
  try {
    const {data} = await AJAX({
      url,
    })

    return data.funcs
  } catch (error) {
    console.error('Could not get suggestions', error)
    throw error
  }
}

interface ASTRequest {
  url: string
  body: string
}

export const getAST = async (request: ASTRequest) => {
  const {url, body} = request
  try {
    const {data} = await AJAX({
      method: 'POST',
      url,
      data: {body},
    })

    return data
  } catch (error) {
    console.error('Could not parse query', error)
    throw error
  }
}

// TODO: replace with actual requests to IFQL daemon
export const getDatabases = async () => {
  try {
    const response = {data: {dbs: ['telegraf', 'chronograf', '_internal']}}
    const {data} = await Promise.resolve(response)

    return data.dbs
  } catch (error) {
    console.error('Could not get databases', error)
    throw error
  }
}
