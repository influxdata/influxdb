import AJAX from 'src/utils/ajax'

const DEFAULT_ENVS = {
  telegrafSystemInterval: '1m',
}

export const getEnv = async url => {
  try {
    const {data} = await AJAX({
      method: 'GET',
      url,
    })

    return data
  } catch (error) {
    console.error('Error retreieving envs: ', error)
    return DEFAULT_ENVS
  }
}
