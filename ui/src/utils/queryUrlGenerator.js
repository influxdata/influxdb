import AJAX from 'utils/ajax'

export const proxy = async ({source, query, db, rp, params}) => {
  try {
    return await AJAX({
      method: 'POST',
      url: source,
      data: {
        params,
        query,
        db,
        rp,
      },
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
