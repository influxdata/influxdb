// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {IndexList, Overlay} from 'src/clockface'
import UpdateLabelOverlay from 'src/labels/components/UpdateLabelOverlay'
import LabelRow from 'src/labels/components/LabelRow'

// Utils
import {validateLabelUniqueness} from 'src/labels/utils/'

// Types
import {ILabel} from '@influxdata/influx'
import {OverlayState} from 'src/types'
import {Sort} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/selectors/sort'
import {AppState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Selectors
import {getSortedResource} from 'src/shared/selectors/sort'

type SortKey = keyof ILabel

interface OwnProps {
  labels: ILabel[]
  emptyState: JSX.Element
  onUpdateLabel: (label: ILabel) => void
  onDeleteLabel: (labelID: string) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (mextSort: Sort, sortKey: SortKey) => void
}

interface StateProps {
  sortedLabels: ILabel[]
}

type Props = OwnProps & StateProps

interface State {
  labelID: string
  overlayState: OverlayState
  sortedLabels: ILabel[]
}

@ErrorHandling
class LabelList extends PureComponent<Props, State> {
  public state: State = {
    labelID: null,
    overlayState: OverlayState.Closed,
    sortedLabels: this.props.sortedLabels,
  }

  componentDidUpdate(prevProps) {
    const {labels, sortedLabels, sortKey, sortDirection} = this.props

    if (
      prevProps.sortDirection !== sortDirection ||
      prevProps.sortKey !== sortKey ||
      prevProps.labels.length !== labels.length
    ) {
      this.setState({sortedLabels})
    }
  }

  public render() {
    const {sortKey, sortDirection, onClickColumn} = this.props
    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell
              sortKey={this.headerKeys[0]}
              sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
              columnName="Name"
              width="20%"
              onClick={onClickColumn}
            />
            <IndexList.HeaderCell columnName="Description" width="55%" />
            <IndexList.HeaderCell width="25%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={this.props.emptyState}>
            {this.rows}
          </IndexList.Body>
        </IndexList>
        <Overlay visible={this.isOverlayVisible}>
          <UpdateLabelOverlay
            label={this.label}
            onDismiss={this.handleCloseModal}
            onUpdateLabel={this.handleUpdateLabel}
            onNameValidation={this.handleNameValidation}
          />
        </Overlay>
      </>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name']
  }

  private get rows(): JSX.Element[] {
    const {onDeleteLabel} = this.props
    const {sortedLabels} = this.state

    return sortedLabels.map((label, index) => (
      <LabelRow
        key={label.id || `label-${index}`}
        onDelete={onDeleteLabel}
        onClick={this.handleStartEdit}
        label={label}
      />
    ))
  }

  private get label(): ILabel | null {
    if (this.state.labelID) {
      return this.props.labels.find(l => l.id === this.state.labelID)
    }
  }

  private handleCloseModal = () => {
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleStartEdit = (labelID: string): void => {
    this.setState({labelID, overlayState: OverlayState.Open})
  }

  private get isOverlayVisible(): boolean {
    const {labelID, overlayState} = this.state
    return !!labelID && overlayState === OverlayState.Open
  }

  private handleUpdateLabel = async (updatedLabel: ILabel) => {
    await this.props.onUpdateLabel(updatedLabel)
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleNameValidation = (name: string): string | null => {
    const {labels} = this.props

    const names = labels.map(label => label.name).filter(l => l !== name)

    return validateLabelUniqueness(names, name)
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  return {
    sortedLabels: getSortedResource(state.labels.list, props),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(LabelList)
