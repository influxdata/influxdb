export const TYPE_ID = 'ID'
export const TYPE_URI = 'ID'

const idNormalizer = (type, id) => {
  switch (type) {
    case 'ID': {
      return +id
    }

    case 'URI': {
      // handle decode of URI here
    }
  }

  return id
}

export default idNormalizer
