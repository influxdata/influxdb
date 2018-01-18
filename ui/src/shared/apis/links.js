import {getAJAX, setAJAXLinks} from 'utils/ajax'

import {linksLink} from 'shared/constants'

export const getLinks = async () => {
  try {
    const res = await getAJAX(linksLink)

    // TODO: remove use of links entirely from within AJAX function so that
    // call to setAJAXLinks is not necessary. see issue #1486
    setAJAXLinks({updatedLinks: res.data})

    return res
  } catch (error) {
    console.error(error)
    throw error
  }
}
