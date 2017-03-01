import AJAX from 'utils/ajax';

export const proxy = async ({source, query, db, rp}) => {
  try {
    return await AJAX({
      method: 'POST',
      url: source,
      data: {
        query,
        db,
        rp,
      },
    })
  } catch (error) {
    console.error(error) // eslint-disable-line no-console
  }
}
