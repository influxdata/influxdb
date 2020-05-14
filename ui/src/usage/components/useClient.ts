import {useState, useEffect} from 'react'

import {RemoteDataState} from 'src/types'

export default function useClient<T>(request) {
  const [status, setStatus] = useState<RemoteDataState>(
    RemoteDataState.NotStarted
  )
  const [error, setError] = useState(null)
  const [data, setData] = useState<T>(null)

  useEffect(() => {
    const fetchData = async () => {
      setStatus(RemoteDataState.Loading)
      try {
        const response = await request()

        if (Math.floor(response.status / 100) !== 2) {
          return setStatus(RemoteDataState.Error)
        }

        setStatus(RemoteDataState.Done)
        setData(response.data)
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
