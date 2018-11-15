// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import {
  IndexList,
  Alignment,
  Button,
  ComponentColor,
  ComponentSize,
} from 'src/clockface'
import DeleteSourceButton from 'src/sources/components/DeleteSourceButton'

// Actions
import {setActiveSource, deleteSource} from 'src/sources/actions'

// Styles
import 'src/sources/components/SourcesListRow.scss'

// Types
import {AppState} from 'src/types/v2'
import {Source} from 'src/types/v2'

interface StateProps {
  activeSourceID: string
}

interface DispatchProps {
  onSetActiveSource: typeof setActiveSource
  onDeleteSource: (sourceID: string) => Promise<void>
}

interface OwnProps {
  source: Source
}

type Props = StateProps & DispatchProps & OwnProps

const SourcesListRow: SFC<Props> = ({
  source,
  activeSourceID,
  onSetActiveSource,
  onDeleteSource,
}) => {
  const canDelete = source.type !== 'self'
  const isActiveSource = source.id === activeSourceID
  const onButtonClick = () => onSetActiveSource(source.id)
  const onDeleteClick = () => onDeleteSource(source.id)

  let buttonText
  let buttonColor

  if (isActiveSource) {
    buttonText = 'Connected'
    buttonColor = ComponentColor.Success
  } else {
    buttonText = 'Connect'
    buttonColor = ComponentColor.Default
  }

  return (
    <IndexList.Row
      key={source.id}
      disabled={false}
      customClass="sources-list-row"
    >
      <IndexList.Cell>
        <Button
          text={buttonText}
          color={buttonColor}
          size={ComponentSize.ExtraSmall}
          customClass="sources-list-row--connect-btn"
          onClick={onButtonClick}
        />
      </IndexList.Cell>
      <IndexList.Cell>{source.name}</IndexList.Cell>
      <IndexList.Cell>{source.type}</IndexList.Cell>
      <IndexList.Cell>{source.url ? source.url : 'N/A'}</IndexList.Cell>
      <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
        {canDelete && <DeleteSourceButton onClick={onDeleteClick} />}
      </IndexList.Cell>
    </IndexList.Row>
  )
}

const mstp = (state: AppState) => {
  return {
    activeSourceID: state.sources.activeSourceID,
  }
}

const mdtp = dispatch => ({
  onSetActiveSource: activeSourceID =>
    dispatch(setActiveSource(activeSourceID)),
  onDeleteSource: sourceID => dispatch(deleteSource(sourceID)),
})

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(SourcesListRow)
