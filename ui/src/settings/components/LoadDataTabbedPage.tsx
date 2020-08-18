// Libraries
import React, {FC, ReactNode} from 'react'
import _ from 'lodash'
import {connect, ConnectedProps} from 'react-redux'

// Components
import LoadDataNavigation from 'src/settings/components/LoadDataNavigation'
import {Tabs, Orientation, Page} from '@influxdata/clockface'

// Utils
import {getOrg} from 'src/organizations/selectors'

// Types
import {AppState} from 'src/types'

interface ComponentProps {
  activeTab: string
  children?: ReactNode
}

type StateProps = ConnectedProps<typeof connector>

type Props = ComponentProps & StateProps

const LoadDataTabbedPage: FC<Props> = ({activeTab, orgID, children}) => {
  return (
    <Page.Contents fullWidth={false} scrollable={true}>
      <Tabs.Container orientation={Orientation.Horizontal}>
        <LoadDataNavigation activeTab={activeTab} orgID={orgID} />
        <Tabs.TabContents>{children}</Tabs.TabContents>
      </Tabs.Container>
    </Page.Contents>
  )
}

const mstp = (state: AppState) => {
  const org = getOrg(state)

  return {orgID: org.id}
}

const connector = connect(mstp)

export default connector(LoadDataTabbedPage)
