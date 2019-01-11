// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import {IndexList, Alignment} from 'src/clockface'
import SourcesListRow from 'src/sources/components/SourcesListRow'

// Utils
import {getSources} from 'src/sources/selectors'

// Styles
import './SourcesList.scss'

// Types
import {AppState, Source} from 'src/types/v2'

interface StateProps {
  sources: Source[]
}

type Props = StateProps

const SourcesList: SFC<Props> = props => {
  const rows = props.sources.map(source => (
    <SourcesListRow key={source.id} source={source} />
  ))

  return (
    <div className="sources-list col-xs-12">
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="" width="10%" />
          <IndexList.HeaderCell columnName="Name" width="20%" />
          <IndexList.HeaderCell columnName="Type" width="10%" />
          <IndexList.HeaderCell columnName="URL" width="30%" />
          <IndexList.HeaderCell
            columnName=""
            width="30%"
            alignment={Alignment.Right}
          />
        </IndexList.Header>
        <IndexList.Body emptyState={<div />} columnCount={4}>
          {rows}
        </IndexList.Body>
      </IndexList>
    </div>
  )
}

const mstp = (state: AppState) => {
  const sources = getSources(state)

  return {sources}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(SourcesList)
