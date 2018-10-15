import AJAX from 'src/utils/ajax'

interface AnalyzeQueriesObject {
  id: string
  query: string
  duration: string
}

export const analyzeQueries = async (
  url: string,
  queries: Array<{query: string}>
): Promise<AnalyzeQueriesObject[]> => {
  try {
    const {data} = await AJAX({
      url,
      method: 'POST',
      data: {queries},
    })

    return data.queries
  } catch (error) {
    console.error(error)
    throw error
  }
}
