// Libraries
import React, {useState, useEffect} from 'react'
import {Page, Grid, Columns} from '@influxdata/clockface'

// Components
import UsageToday from 'src/usage/components/UsageToday'

// Types
import {RemoteDataState} from 'src/types'

// Constants
import {usageProps} from 'src/usage/components/constants/stubs'

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

  // if (status === RemoteDataState.Error) {
  //   content = <p>Error...</p>
  // }

  // if (status === RemoteDataState.Done) {
  //   content = <p>Quartz is serving you some sweet rocks</p>
  // }

  return (
    <Page titleTag="Usage">
      <Page.Contents scrollable={true} fullWidth={true}>
        <UsageToday {...usageProps} />
      </Page.Contents>
    </Page>
  )
}

export default UsagePage
