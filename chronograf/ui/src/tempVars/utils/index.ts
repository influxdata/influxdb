import {Template} from 'src/types'

export const trimAndRemoveQuotes = elt => {
  const trimmed = elt.trim()
  const dequoted = trimmed.replace(/(^")|("$)/g, '')

  return dequoted
}

export const formatTempVar = name =>
  `:${name.replace(/:/g, '').replace(/\s/g, '')}:`

export const getSelectedValue = (template: Template): string | null => {
  const selected = template.values.find(v => v.selected)

  if (selected) {
    return selected.value
  }

  return null
}

export const getLocalSelectedValue = (template: Template): string | null => {
  const selected = template.values.find(v => v.localSelected)

  if (selected) {
    return selected.value
  }

  return null
}
