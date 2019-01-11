import {Label} from 'src/api'
// import {labelsAPI} from 'src/utils/api'
// TODO: use actual API when ready

const mockLabels = [
  {
    resourceID: '0336b93e5b791000',
    name: 'Oomph',
    properties: {color: '#334455', description: 'this is a description'},
  },
  {
    resourceID: '0336b93e5b791000',
    name: 'TROGDOOOOORRRRRRRRR',
    properties: {color: '#44ffcc', description: 'this is a description'},
  },
  {
    resourceID: '0336b93e5b791000',
    name: 'ZZYXX',
    properties: {color: '#ff33ff', description: 'this is a description'},
  },
  {
    resourceID: '0336b93e5b791000',
    name: 'labeldawg',
    properties: {color: '#ffb3b3', description: 'this is a description'},
  },
  {
    resourceID: '0336b93e5b791000',
    name: 'porphyria',
    properties: {color: '#ff0054', description: 'this is a description'},
  },
  {
    resourceID: '0336b93e5b791000',
    name: 'snakes!',
    properties: {color: '#44ff44', description: 'this is a description'},
  },
]

export const getLabels = async (): Promise<Label[]> => {
  try {
    // const {data} = await labelsAPI.labelsGet()
    // return data.labels
    return mockLabels
  } catch (error) {
    console.error(error)
    throw error
  }
}
