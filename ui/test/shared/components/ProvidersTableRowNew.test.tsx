import React from 'react'
import ProvidersTableRowNew from 'src/admin/components/chronograf/ProvidersTableRowNew'
import ConfirmButtons from 'src/shared/components/ConfirmButtons'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    organizations: [],
    schemes: [],
    rowIndex: 0,
    onCreate: () => {},
    onCancel: jest.fn(),
    ...override,
  }

  const defaultState = {
    scheme: '*',
    provider: null,
    providerOrganization: null,
    organizationId: 'default',
  }

  const row = shallow(<ProvidersTableRowNew {...props} />)
  return {
    props,
    defaultState,
    row,
  }
}

describe('Components.Shared.ProvidersTableRowNew', () => {
  describe('user interaction', () => {
    describe('provider and providerOrganization in state are null', () => {
      it('should have a disabled confirm button', () => {
        const organizations = [{id: 'default', name: 'default'}]
        const {row} = setup({organizations})

        const confirmButtons = row.find(ConfirmButtons)
        const confirmButtonsDisabled = confirmButtons.prop('isDisabled')

        expect(confirmButtonsDisabled).toBe(true)
      })
    })

    describe('provider has a value in state and providerOrganization is null', () => {
      it('should have a disabled confirm button', () => {
        const organizations = [{id: 'default', name: 'default'}]
        const {row} = setup({organizations})
        const inputClickToEdits = row.find(InputClickToEdit)

        const providerInput = inputClickToEdits.first()
        providerInput.simulate('click')
        providerInput.simulate('change', '*')

        const confirmButtons = row.find(ConfirmButtons)
        const confirmButtonsDisabled = confirmButtons.prop('isDisabled')

        expect(confirmButtonsDisabled).toBe(true)
      })
    })

    describe('providerOrganization has a value in state and provider is null', () => {
      it('should have a disabled confirm button', () => {
        const organizations = [{id: 'default', name: 'default'}]
        const {row} = setup({organizations})
        const inputClickToEdits = row.find(InputClickToEdit)

        const providerOrganizationInput = inputClickToEdits.last()
        providerOrganizationInput.simulate('click')
        providerOrganizationInput.simulate('change', '*')

        const confirmButtons = row.find(ConfirmButtons)
        const confirmButtonsDisabled = confirmButtons.prop('isDisabled')

        expect(confirmButtonsDisabled).toBe(true)
      })
    })

    describe('provider and providerOrganization have values in state', () => {
      it('should not have a disabled confirm button', () => {
        const organizations = [{id: 'default', name: 'default'}]
        const {row} = setup({organizations})
        const inputClickToEdits = row.find(InputClickToEdit)

        const providerInput = inputClickToEdits.first()
        providerInput.simulate('click')
        providerInput.simulate('change', '*')

        const providerOrganizationInput = inputClickToEdits.last()
        providerOrganizationInput.simulate('click')
        providerOrganizationInput.simulate('change', '*')

        const confirmButtons = row.find(ConfirmButtons)
        const confirmButtonsDisabled = confirmButtons.prop('isDisabled')

        expect(confirmButtonsDisabled).toBe(false)
      })
    })
  })
})
