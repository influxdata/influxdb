import React from 'react'
import ProvidersTableRowNew from 'src/admin/components/chronograf/ProvidersTableRowNew'
import ConfirmOrCancel from 'src/shared/components/ConfirmOrCancel'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    onCancel: jest.fn(),
    onCreate: () => {},
    organizations: [],
    rowIndex: 0,
    schemes: [],
    ...override,
  }

  const defaultState = {
    organizationId: 'default',
    provider: null,
    providerOrganization: null,
    scheme: '*',
  }

  const row = shallow(<ProvidersTableRowNew {...props} />)
  return {
    defaultState,
    props,
    row,
  }
}

describe('Components.Shared.ProvidersTableRowNew', () => {
  describe('user interaction', () => {
    describe('provider and providerOrganization in state are null', () => {
      it('should have a disabled confirm button', () => {
        const organizations = [{id: 'default', name: 'default'}]
        const {row} = setup({organizations})

        const confirmOrCancel = row.find(ConfirmOrCancel)
        const confirmOrCancelDisabled = confirmOrCancel.prop('isDisabled')

        expect(confirmOrCancelDisabled).toBe(true)
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

        const confirmOrCancel = row.find(ConfirmOrCancel)
        const confirmOrCancelDisabled = confirmOrCancel.prop('isDisabled')

        expect(confirmOrCancelDisabled).toBe(true)
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

        const confirmOrCancel = row.find(ConfirmOrCancel)
        const confirmOrCancelDisabled = confirmOrCancel.prop('isDisabled')

        expect(confirmOrCancelDisabled).toBe(true)
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

        const confirmOrCancel = row.find(ConfirmOrCancel)
        const confirmOrCancelDisabled = confirmOrCancel.prop('isDisabled')

        expect(confirmOrCancelDisabled).toBe(false)
      })
    })
  })
})
