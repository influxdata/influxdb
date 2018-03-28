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
