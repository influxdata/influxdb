// Reducer
import {trueFieldOptions} from 'src/timeMachine/reducers/index'

describe('trueFieldOptions utility function', () => {
  it('should return defaultProps if no fieldOptions are passed: ', () => {
    const defaultOptions = [
      {
        internalName: 'Applications',
        displayName: 'Applications',
        visible: true,
      },
    ]
    const result = trueFieldOptions(defaultOptions, [])
    expect(result).toEqual(defaultOptions)
  })

  it("should return the new fieldOptions if the defaultOptions don't exist: ", () => {
    const fieldOptions = [
      {
        internalName: 'Applications',
        displayName: 'Applications',
        visible: true,
      },
    ]
    const result = trueFieldOptions([], fieldOptions)
    expect(result).toEqual(fieldOptions)
  })

  it('should return the new aliased fields for fieldOptions with the same internalName: ', () => {
    const fieldOptions = [
      {
        internalName: 'Applications',
        displayName: 'New Alias',
        visible: true,
      },
    ]
    const defaultOptions = [
      {
        internalName: 'Applications',
        displayName: 'Applications',
        visible: true,
      },
    ]
    const result = trueFieldOptions(defaultOptions, fieldOptions)
    expect(result).toEqual(fieldOptions)
  })

  it('should add aliased fieldOptions onto defaultOptions if they no longer exist: ', () => {
    const fieldOptions = [
      {
        internalName: 'Applications',
        displayName: 'New Alias',
        visible: true,
      },
    ]
    const defaultOptions = [
      {
        internalName: 'Table',
        displayName: 'Table',
        visible: true,
      },
    ]
    const result = trueFieldOptions(defaultOptions, fieldOptions)
    expect(result).toEqual([defaultOptions[0], fieldOptions[0]])
  })

  it(`should add aliased fieldOptions onto defaultOptions if they no longer exist
    and use the aliased versions of ones that do exist: `, () => {
    const fieldOptions = [
      {
        internalName: 'Applications',
        displayName: 'New Alias',
        visible: true,
      },
    ]
    const defaultOptions = [
      {
        internalName: 'Applications',
        displayName: 'Applications',
        visible: true,
      },
      {
        internalName: 'Table',
        displayName: 'Table',
        visible: true,
      },
    ]
    const result = trueFieldOptions(defaultOptions, fieldOptions)
    expect(result).toEqual([fieldOptions[0], defaultOptions[1]])
  })
})
