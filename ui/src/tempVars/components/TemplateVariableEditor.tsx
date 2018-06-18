import React, {
  PureComponent,
  ComponentClass,
  ChangeEvent,
  KeyboardEvent,
} from 'react'
import {connect} from 'react-redux'

import {ErrorHandling} from 'src/shared/decorators/errors'
import Dropdown from 'src/shared/components/Dropdown'
import ConfirmButton from 'src/shared/components/ConfirmButton'
import {getDeep} from 'src/utils/wrappers'
import {notify as notifyActionCreator} from 'src/shared/actions/notifications'

import DatabasesTemplateBuilder from 'src/tempVars/components/DatabasesTemplateBuilder'
import CSVTemplateBuilder from 'src/tempVars/components/CSVTemplateBuilder'
import MeasurementsTemplateBuilder from 'src/tempVars/components/MeasurementsTemplateBuilder'
import FieldKeysTemplateBuilder from 'src/tempVars/components/FieldKeysTemplateBuilder'
import TagKeysTemplateBuilder from 'src/tempVars/components/TagKeysTemplateBuilder'
import TagValuesTemplateBuilder from 'src/tempVars/components/TagValuesTemplateBuilder'

import {
  Template,
  TemplateType,
  TemplateBuilderProps,
  Source,
  RemoteDataState,
  Notification,
} from 'src/types'
import {
  TEMPLATE_TYPES_LIST,
  DEFAULT_TEMPLATES,
  RESERVED_TEMPLATE_NAMES,
} from 'src/tempVars/constants'
import {FIVE_SECONDS} from 'src/shared/constants/index'

interface Props {
  // We will assume we are creating a new template if none is passed in
  template?: Template
  source: Source
  onCancel: () => void
  onCreate?: (template: Template) => Promise<any>
  onUpdate?: (template: Template) => Promise<any>
  onDelete?: () => Promise<any>
  notify: (n: Notification) => void
}

interface State {
  nextTemplate: Template
  isNew: boolean
  savingStatus: RemoteDataState
  deletingStatus: RemoteDataState
}

const TEMPLATE_BUILDERS = {
  [TemplateType.Databases]: DatabasesTemplateBuilder,
  [TemplateType.CSV]: CSVTemplateBuilder,
  [TemplateType.Measurements]: MeasurementsTemplateBuilder,
  [TemplateType.FieldKeys]: FieldKeysTemplateBuilder,
  [TemplateType.TagKeys]: TagKeysTemplateBuilder,
  [TemplateType.TagValues]: TagValuesTemplateBuilder,
}

const formatName = name => `:${name.replace(/:/g, '')}:`

const DEFAULT_TEMPLATE = DEFAULT_TEMPLATES[TemplateType.Databases]

@ErrorHandling
class TemplateVariableEditor extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    const defaultState = {
      savingStatus: RemoteDataState.NotStarted,
      deletingStatus: RemoteDataState.NotStarted,
    }

    const {template} = this.props

    if (template) {
      this.state = {
        ...defaultState,
        nextTemplate: {...template},
        isNew: false,
      }
    } else {
      this.state = {
        ...defaultState,
        nextTemplate: DEFAULT_TEMPLATE(),
        isNew: true,
      }
    }
  }

  public render() {
    const {source, onCancel} = this.props
    const {nextTemplate, isNew} = this.state
    const TemplateBuilder = this.templateBuilder

    return (
      <div className="edit-temp-var">
        <div className="edit-temp-var--header">
          <h1 className="page-header__title">Edit Template Variable</h1>
          <div className="edit-temp-var--header-controls">
            <button
              className="btn btn-default"
              type="button"
              onClick={onCancel}
            >
              Cancel
            </button>
            <button
              className="btn btn-success"
              type="button"
              onClick={this.handleSave}
              disabled={!this.canSave}
            >
              {this.isSaving ? 'Saving...' : 'Save'}
            </button>
          </div>
        </div>
        <div className="edit-temp-var--body">
          <div className="edit-temp-var--body-row">
            <div className="form-group name">
              <label>Name</label>
              <input
                type="text"
                className="form-control"
                value={nextTemplate.tempVar}
                onChange={this.handleChangeName}
                onKeyPress={this.handleNameKeyPress}
                onBlur={this.formatName}
              />
            </div>
            <div className="form-group template-type">
              <label>Type</label>
              <Dropdown
                items={TEMPLATE_TYPES_LIST}
                onChoose={this.handleChooseType}
                selected={this.dropdownSelection}
                buttonSize=""
              />
            </div>
          </div>
          <div className="edit-temp-var--body-row">
            <TemplateBuilder
              template={nextTemplate}
              source={source}
              onUpdateTemplate={this.handleUpdateTemplate}
            />
          </div>
          <ConfirmButton
            text={this.isDeleting ? 'Deleting...' : 'Delete'}
            confirmAction={this.handleDelete}
            type="btn-danger"
            size="btn-xs"
            customClass="delete"
            disabled={isNew || this.isDeleting}
          />
        </div>
      </div>
    )
  }

  private get templateBuilder(): ComponentClass<TemplateBuilderProps> {
    const {
      nextTemplate: {type},
    } = this.state

    const component = TEMPLATE_BUILDERS[type]

    if (!component) {
      throw new Error(`Could not find template builder for type "${type}"`)
    }

    return component
  }

  private handleUpdateTemplate = (nextTemplate: Template): void => {
    this.setState({nextTemplate})
  }

  private handleChooseType = ({type}) => {
    const {
      nextTemplate: {id},
    } = this.state

    const nextNextTemplate = {...DEFAULT_TEMPLATES[type](), id}

    this.setState({nextTemplate: nextNextTemplate})
  }

  private handleChangeName = (e: ChangeEvent<HTMLInputElement>): void => {
    const {value} = e.target
    const {nextTemplate} = this.state

    this.setState({
      nextTemplate: {
        ...nextTemplate,
        tempVar: value,
      },
    })
  }

  private formatName = (): void => {
    const {nextTemplate} = this.state
    const tempVar = formatName(nextTemplate.tempVar)

    this.setState({nextTemplate: {...nextTemplate, tempVar}})
  }

  private handleSave = async (): Promise<any> => {
    if (!this.canSave) {
      return
    }

    const {onUpdate, onCreate, notify} = this.props
    const {nextTemplate, isNew} = this.state

    nextTemplate.tempVar = formatName(nextTemplate.tempVar)

    this.setState({savingStatus: RemoteDataState.Loading})

    try {
      if (isNew) {
        await onCreate(nextTemplate)
      } else {
        await onUpdate(nextTemplate)
      }
    } catch (error) {
      notify({
        message: `Error saving template: ${error}`,
        type: 'error',
        icon: 'alert-triangle',
        duration: FIVE_SECONDS,
      })
      console.error(error)
    }
  }

  private handleNameKeyPress = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      this.handleSave()
    }
  }

  private get isSaving(): boolean {
    return this.state.savingStatus === RemoteDataState.Loading
  }

  private get canSave(): boolean {
    const {
      nextTemplate: {tempVar},
    } = this.state

    return (
      tempVar !== '' &&
      !RESERVED_TEMPLATE_NAMES.includes(formatName(tempVar)) &&
      !this.isSaving
    )
  }

  private get dropdownSelection(): string {
    const {
      nextTemplate: {type},
    } = this.state

    return getDeep(
      TEMPLATE_TYPES_LIST.filter(t => t.type === type),
      '0.text',
      ''
    )
  }

  private handleDelete = (): void => {
    const {onDelete} = this.props

    this.setState({deletingStatus: RemoteDataState.Loading}, onDelete)
  }

  private get isDeleting(): boolean {
    return this.state.deletingStatus === RemoteDataState.Loading
  }
}

const mapDispatchToProps = {notify: notifyActionCreator}

export default connect(null, mapDispatchToProps)(TemplateVariableEditor)
