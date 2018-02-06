import {getAJAX, setAJAXLinks} from 'utils/ajax'

import {linksLink} from 'shared/constants'

export const getLinks = async () => {
  try {
    const response = await getAJAX(linksLink)
    // TODO: Remove use of links entirely from within AJAX function so that
    // call to setAJAXLinks is not necessary. See issue #1486.
    setAJAXLinks({updatedLinks: response.data})

    return response
  } catch (error) {
    console.error(error)
    throw error
  }
}
