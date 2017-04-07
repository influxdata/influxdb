import {fetchTimeSeries} from 'shared/apis/timeSeries'
import {editQueryStatus} from 'src/data_explorer/actions/view'

export const fetchTimeSeriesAsync = (source, database, query) => async (dispatch) => {
  try {
    const {data} = await fetchTimeSeries(source, database, query)
  } catch (error) {
    console.error(error)
    throw error
  }
}
