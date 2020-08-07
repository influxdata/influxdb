// Libraries
import React, {useContext, FC} from 'react'

// Components
import PrecisionDropdown from 'src/buckets/components/lineProtocol/configure/PrecisionDropdown'
import TabSelector from 'src/buckets/components/lineProtocol/configure/TabSelector'
import TabBody from 'src/buckets/components/lineProtocol/configure/TabBody'
import {Context} from 'src/buckets/components/lineProtocol/LineProtocolWizard'

// Types
import {LineProtocolTab, RemoteDataState, WritePrecision} from 'src/types'

// Actions
import {
  setBody,
  setTab,
  setPrecision,
} from 'src/buckets/components/lineProtocol/LineProtocol.creators'
import StatusIndicator from '../verify/StatusIndicator'

interface OwnProps {
  tabs: LineProtocolTab[]
  onSubmit: () => void
}

type Props = OwnProps

const LineProtocolTabs: FC<Props> = ({tabs, onSubmit}) => {
  const [state, dispatch] = useContext(Context)
  const {tab, precision, writeStatus} = state

  const handleTabClick = (tab: LineProtocolTab) => {
    dispatch(setBody(''))
    dispatch(setTab(tab))
  }

  const handleSetPrecision = (p: WritePrecision) => {
    dispatch(setPrecision(p))
  }

  if (writeStatus !== RemoteDataState.NotStarted) {
    return <StatusIndicator />
  }

  return (
    <>
      <div className="line-protocol--header">
        <TabSelector activeLPTab={tab} tabs={tabs} onClick={handleTabClick} />
        <PrecisionDropdown setPrecision={handleSetPrecision} precision={precision} />
      </div>
      <TabBody onSubmit={onSubmit} />
    </>
  )
}

export default LineProtocolTabs
