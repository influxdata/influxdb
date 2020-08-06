// Libraries
import React, {useContext, FC} from 'react'

// Components
import PrecisionDropdown from 'src/buckets/components/lineProtocol/configure/PrecisionDropdown'
import TabSelector from 'src/buckets/components/lineProtocol/configure/TabSelector'
import TabBody from 'src/buckets/components/lineProtocol/configure/TabBody'
import {Context} from 'src/buckets/components/lineProtocol/LineProtocolWizard'

// Types
import {LineProtocolTab} from 'src/types'

// Actions
import {
  setBody,
  setTab,
  setPrecision,
} from 'src/buckets/components/lineProtocol/LineProtocol.creators'

interface OwnProps {
  tabs: LineProtocolTab[]
}

type Props = OwnProps

const LineProtocolTabs: FC<Props> = ({tabs}) => {
  const [state, dispatch] = useContext(Context)
  const {tab, precision} = state

  const handleTabClick = (tab: LineProtocolTab) => {
    dispatch(setBody(''))
    dispatch(setTab(tab))
  }

  return (
    <>
      <div className="line-protocol--header">
        <TabSelector activeLPTab={tab} tabs={tabs} onClick={handleTabClick} />
        <PrecisionDropdown setPrecision={setPrecision} precision={precision} />
      </div>
      <TabBody />
    </>
  )
}

export default LineProtocolTabs
