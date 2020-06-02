import {useState, useEffect} from 'react'

import {ResponseHandler} from 'src/client/generatedRoutes'
import {RemoteDataState} from 'src/types'

type Response = ReturnType<ResponseHandler>

export default function useClient<T>(requests) {
  const [status, setStatus] = useState<RemoteDataState>(
    RemoteDataState.NotStarted
  )
  const [error, setError] = useState(null)
  const [data, setData] = useState<T[]>(null)

  useEffect(() => {
    const fetchData = async () => {
      setStatus(RemoteDataState.Loading)
      try {
        const responses: Response[] = await Promise.all(requests)

        for (let i = 0; i < responses.length; i++) {
          const resp = responses[i]
          if (Math.floor(resp.status / 100) !== 2) {
            setStatus(RemoteDataState.Error)
          }
        }

        setStatus(RemoteDataState.Done)
        setData(responses.map(resp => resp.data))
      } catch (error) {
        console.error(error)
        setStatus(RemoteDataState.Error)
        setError(error)
      }
    }

    fetchData()
  }, [])

  return [status, data, error]
}
