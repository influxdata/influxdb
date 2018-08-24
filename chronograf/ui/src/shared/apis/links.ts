import {getAJAX} from 'src/utils/ajax'
import {Links} from 'src/types/v2/links'

const linksURI = '/v2'

export const getLinks = async (): Promise<Links> => {
  try {
    const {data} = await getAJAX(linksURI)

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}
