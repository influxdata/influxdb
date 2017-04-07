import {proxy} from 'utils/queryUrlGenerator'

const fetchTimeSeries = async (source, database, query) => {
  try {
    return await proxy({source, query, database})
  } catch (error) {
    console.error('error from proxy: ', error)
    throw error
  }
}

export default fetchTimeSeries
