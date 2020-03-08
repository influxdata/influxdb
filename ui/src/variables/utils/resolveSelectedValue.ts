import {VariableMapObject} from 'src/types'

export const resolveSelectedKey = (
  values: string[],
  prevSelection?: string,
  defaultSelection?: string
): string => {
  if (values.includes(prevSelection)) {
    return prevSelection
  }

  if (values.includes(defaultSelection)) {
    return defaultSelection
  }

  return values[0]
}

export const resolveSelectedValue = (
  values: VariableMapObject,
  selectedKey: string,
  defaultSelection?: string
): string => {
  if (
    typeof values === 'object' &&
    Object.prototype.toString.call(values) !== '[object Array]'
  ) {
    if (selectedKey in values) {
      return values[selectedKey]
    }

    if (defaultSelection in values) {
      return values[defaultSelection]
    }
    // return get first value
    const first = Object.keys(values)[0]
    return values[first]
  }

  if (values.includes(selectedKey)) {
    return selectedKey
  }

  if (values.includes(defaultSelection)) {
    return defaultSelection
  }

  return values[0]
}
