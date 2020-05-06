import React, {useState, useEffect} from 'react'

// Types
import {RemoteDataState} from 'src/types'

const url = '/api/experimental/quartz/usage'

const UsagePage = () => {
  const [status, setStatus] = useState(RemoteDataState.NotStarted)

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch(url, {
          headers: {
            'Content-Type': 'application/json',
          },
        })

        if (!response.ok) {
          return setStatus(RemoteDataState.Error)
        }

        setStatus(RemoteDataState.Done)
      } catch (error) {
        console.error('FETCHING ERROR')
        console.error(error)
        setStatus(RemoteDataState.Error)
      }
    }

    fetchData()
  }, [])

  let content = <div>You've got a problem</div>

  if (
    status === RemoteDataState.Loading ||
    status === RemoteDataState.NotStarted
  ) {
    content = <p>Loading...</p>
  }

  if (status === RemoteDataState.Error) {
    content = <p>Error...</p>
  }

  if (status === RemoteDataState.Done) {
    content = <p>Quartz is serving you some sweet rocks</p>
  }

  return (
    <div
      style={{
        width: '100%',
        height: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontWeight: 'bolder',
        fontSize: 'XXL',
      }}
    >
      {content}
    </div>
  )
}

export default UsagePage
