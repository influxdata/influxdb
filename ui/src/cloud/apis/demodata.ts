import AJAX from 'src/utils/ajax'

export const getDemoDataBuckets = async () => {
  try {
    const {data} = await AJAX({
      method: 'GET',
      url: `/api/v2/experimental/sampledata/buckets`,
    })
    // filter out the 'system buckets'
    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}
