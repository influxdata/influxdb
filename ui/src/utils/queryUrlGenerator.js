import AJAX from 'utils/ajax'

export const proxy = async ({source, query, db, rp, tempVars, resolution}) => {
  try {
    return await AJAX({
      method: 'POST',
      url: source,
      data: {
        tempVars,
        query,
        resolution,
        db,
        rp,
      },
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
