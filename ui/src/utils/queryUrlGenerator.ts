import AJAX from 'src/utils/ajax'

interface ProxyQuery {
  source: string
  query: string
  db?: string
  rp?: string
}

export async function proxy<T = any>({source, query, db, rp}: ProxyQuery) {
  try {
    return await AJAX<T>({
      method: 'POST',
      url: source,
      data: {
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
