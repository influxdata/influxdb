import React, {PureComponent} from 'react'

import ConfirmOrCancel from 'src/shared/components/ConfirmOrCancel'
import Dropdown from 'src/shared/components/Dropdown'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Organization {
  id: string
  name: string
}

interface Scheme {
  text: string
}

interface Props {
  organizations: Organization[]
  schemes?: Scheme[]
  rowIndex?: number
  onCreate: (state: State) => void
  onCancel: () => void
}

interface State {
  scheme: string
  provider: string
  providerOrganization: string
  organizationId: string
}

@ErrorHandling
class ProvidersTableRowNew extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      organizationId: 'default',
      provider: null,
      providerOrganization: null,
      scheme: '*',
    }

    this.handleChooseScheme = this.handleChooseScheme.bind(this)
    this.handleChangeProvider = this.handleChangeProvider.bind(this)
    this.handleChangeProviderOrg = this.handleChangeProviderOrg.bind(this)
    this.handleChooseOrganization = this.handleChooseOrganization.bind(this)
    this.handleSaveNewMapping = this.handleSaveNewMapping.bind(this)
  }

  public render() {
    const {scheme, provider, providerOrganization, organizationId} = this.state

    const {organizations, onCancel, schemes, rowIndex} = this.props

    const selectedOrg = organizations.find(o => o.id === organizationId)

    const dropdownItems = organizations.map(role => ({
      ...role,
      text: role.name,
    }))

    const preventCreate = !provider || !providerOrganization

    return (
      <div className="fancytable--row">
        <div className="fancytable--td provider--scheme">
          <Dropdown
            items={schemes}
            onChoose={this.handleChooseScheme}
            selected={scheme}
            className="dropdown-stretch"
          />
        </div>
        <InputClickToEdit
          value={provider}
          wrapperClass="fancytable--td provider--provider"
          onChange={this.handleChangeProvider}
          onBlur={this.handleChangeProvider}
          tabIndex={rowIndex}
          placeholder="google"
        />
        <InputClickToEdit
          value={providerOrganization}
          wrapperClass="fancytable--td provider--providerorg"
          onChange={this.handleChangeProviderOrg}
          onBlur={this.handleChangeProviderOrg}
          tabIndex={rowIndex}
          placeholder="*"
        />
        <div className="fancytable--td provider--arrow">
          <span />
        </div>
        <div className="fancytable--td provider--redirect creating">
          <Dropdown
            items={dropdownItems}
            onChoose={this.handleChooseOrganization}
            selected={selectedOrg.name}
            className="dropdown-stretch"
          />
        </div>
        <ConfirmOrCancel
          onCancel={onCancel}
          onConfirm={this.handleSaveNewMapping}
          isDisabled={preventCreate}
        />
      </div>
    )
  }

  private handleChooseScheme(scheme: Scheme) {
    this.setState({scheme: scheme.text})
  }

  private handleChangeProvider(provider: string) {
    this.setState({provider})
  }

  private handleChangeProviderOrg(providerOrganization: string) {
    this.setState({providerOrganization})
  }

  private handleChooseOrganization(org: Organization) {
    this.setState({organizationId: org.id})
  }

  private handleSaveNewMapping() {
    const {onCreate} = this.props
    onCreate(this.state)
  }
}

export default ProvidersTableRowNew
