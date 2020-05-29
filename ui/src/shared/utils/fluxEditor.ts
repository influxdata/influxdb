import {Doc} from 'codemirror'

export const onTab = (cm: Doc & {indentSelection: (a: any) => any}) => {
  if (cm.somethingSelected()) {
    cm.indentSelection('add')
  } else {
    cm.replaceSelection('  ', 'end')
  }
}
