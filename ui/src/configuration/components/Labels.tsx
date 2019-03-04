// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import CreateLabelOverlay from 'src/configuration/components/CreateLabelOverlay'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import {
  Button,
  IconFont,
  ComponentSize,
  ComponentColor,
} from '@influxdata/clockface'
import {EmptyState, Input, InputType} from 'src/clockface'
import LabelList from 'src/configuration/components/LabelList'
import FilterList from 'src/shared/components/Filter'

// API
import {client} from 'src/utils/api'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {createLabel} from 'src/labels/actions'

// Utils
import {validateLabelName} from 'src/configuration/utils/labels'

// Constants
import {
  labelDeleteFailed,
  labelCreateFailed,
  labelUpdateFailed,
} from 'src/shared/copy/v2/notifications'

// Types
import {LabelType} from 'src/clockface'
import {Label, AppState} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface LabelProperties {
  color: string
  description: string
}

interface StateProps {
  labels: Label[]
}

interface State {
  searchTerm: string
  isOverlayVisible: boolean
  labelTypes: LabelType[]
}

interface DispatchProps {
  notify: typeof notifyAction
  createLabel: typeof createLabel
}

type Props = DispatchProps & StateProps

@ErrorHandling
class Labels extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
      isOverlayVisible: false,
      labelTypes: this.labelTypes,
    }
  }

  public render() {
    const {searchTerm, isOverlayVisible} = this.state

    return (
      <>
        <TabbedPageHeader>
          <Input
            icon={IconFont.Search}
            widthPixels={290}
            type={InputType.Text}
            value={searchTerm}
            onBlur={this.handleFilterBlur}
            onChange={this.handleFilterChange}
            placeholder="Filter Labels..."
          />
          <Button
            text="Create Label"
            color={ComponentColor.Primary}
            icon={IconFont.Plus}
            onClick={this.handleShowOverlay}
          />
        </TabbedPageHeader>
        <FilterList<LabelType>
          list={this.labelTypes}
          searchKeys={['name', 'description']}
          searchTerm={searchTerm}
        >
          {ls => (
            <LabelList
              labels={ls}
              emptyState={this.emptyState}
              onUpdateLabel={this.handleUpdateLabel}
            />
          )}
        </FilterList>
        <CreateLabelOverlay
          isVisible={isOverlayVisible}
          onDismiss={this.handleDismissOverlay}
          onCreateLabel={this.handleCreateLabel}
          onNameValidation={this.handleNameValidation}
        />
      </>
    )
  }

  private handleShowOverlay = (): void => {
    this.setState({isOverlayVisible: true})
  }

  private handleDismissOverlay = (): void => {
    this.setState({isOverlayVisible: false})
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleCreateLabel = async (labelType: LabelType) => {
    this.props.createLabel(labelType.name, this.labelProperties(labelType))
  }

  private handleUpdateLabel = async (labelType: LabelType) => {
    try {
      const label = await client.labels.update(
        labelType.id,
        this.labelProperties(labelType)
      )

      const labelTypes = this.state.labelTypes.map(l => {
        if (l.id === labelType.id) {
          return this.labelType(label)
        }

        return l
      })

      this.setState({labelTypes})
    } catch (error) {
      this.props.notify(labelUpdateFailed())
    }
  }

  private handleNameValidation = (name: string): string | null => {
    return validateLabelName(this.state.labelTypes, name)
  }

  private get labelTypes(): LabelType[] {
    return this.props.labels.map(this.labelType)
  }

  private labelType = (label: Label): LabelType => {
    const {properties} = label

    return {
      id: label.id,
      name: label.name,
      description: properties.description,
      colorHex: properties.color,
      onDelete: this.handleDelete,
    }
  }

  private handleDelete = async (id: string) => {
    const labelType = this.state.labelTypes.find(label => label.id === id)

    try {
      await client.labels.delete(labelType.id)
      const labelTypes = this.state.labelTypes.filter(l => l.id !== id)

      this.setState({labelTypes})
    } catch (error) {
      this.props.notify(labelDeleteFailed())
    }
  }

  private labelProperties(labelType: LabelType): LabelProperties {
    return {
      description: labelType.description,
      color: labelType.colorHex,
    }
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (searchTerm) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text text="No Labels match your search term" />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text
          text="Looks like you haven't created any Labels , why not create one?"
          highlightWords={['Labels']}
        />
        <Button
          text="Create Label"
          color={ComponentColor.Primary}
          icon={IconFont.Plus}
          onClick={this.handleShowOverlay}
        />
      </EmptyState>
    )
  }
}

const mstp = ({labels}: AppState): StateProps => {
  return {
    labels: labels.list,
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  createLabel: createLabel,
}

export default connect(
  mstp,
  mdtp
)(Labels)
