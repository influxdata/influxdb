import AJAX from 'src/utils/ajax'

interface ProxyQuery {
  source: string
  query: string
  db: string
  rp?: string
  tempVars?: string
  resolution?: string
}

export async function proxy<T = any>({
  source,
  query,
  db,
  rp,
  tempVars,
  resolution,
}: ProxyQuery) {
  try {
    return await AJAX<T>({
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
