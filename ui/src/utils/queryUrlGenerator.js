import AJAX from 'utils/ajax'

export const proxy = async ({source, query, db, rp, templates}) => {
  try {
    return await AJAX({
      method: 'POST',
      url: source,
      data: {
        templates,
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
