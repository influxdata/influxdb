// Types
import {Label} from 'src/api'

export const getLabels = async (): Promise<Label[]> => {
  // Use try catch when accessing the actual API
  // TODO: Delete this silly mocks
  const mockLabels: Label[] = [
    {
      name: 'Swogglez',
      properties: {
        description: 'I am an example Label',
        color: '#ff0054',
      },
    },
    {
      name: 'Top Secret',
      properties: {
        description: 'Only admins can modify these resources',
        color: '#4a52f4',
      },
    },
    {
      name: 'Pineapples',
      properties: {
        description: 'Pineapples are in my head',
        color: '#f4c24a',
      },
    },
    {
      name: 'SWAT',
      properties: {
        description: 'Boots and cats and boots and cats',
        color: '#d6ff9c',
      },
    },
    {
      name: 'the GOAT',
      properties: {
        description: 'Gatsby obviously ate turnips',
        color: '#17d9f0',
      },
    },
    {
      name: 'My Spoon is Too Big',
      properties: {
        description: 'My Spooooooooon is Too Big',
        color: '#27c27e',
      },
    },
  ]

  return mockLabels
}

// TODO: implement with an actual API call
export const createLabel = async (label: Label): Promise<Label> => {
  return label
}

// TODO: implement with an actual API call
export const deleteLabel = async (___: Label): Promise<void> => {}

// TODO: implement with an actual API call
export const updateLabel = async (label: Label): Promise<Label> => {
  return label
}
