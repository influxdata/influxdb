import AJAX from 'src/utils/ajax'

interface ProxyQuery {
  source: string | string[]
  query: string
  db?: string
  rp?: string
  tempVars?: any[]
  resolution?: number
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
