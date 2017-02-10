export default function dataExplorerUI(state = {}, action) {
  switch (action.type) {
    case 'ACTIVATE_PANEL':
    case 'CREATE_PANEL': {
      const {panelID} = action.payload;
      return {...state, activePanel: panelID};
    }
  }

  return state;
}
