import update from 'react-addons-update';

export default function panels(state = {}, action) {
  switch (action.type) {
    case 'CREATE_PANEL': {
      const {panelID, queryID} = action.payload;
      return {
        ...state,
        [panelID]: {id: panelID, queryIds: [queryID]},
      };
    }

    case 'RENAME_PANEL': {
      const {panelId, name} = action.payload;
      return update(state, {
        [panelId]: {
          name: {$set: name},
        },
      });
    }

    case 'DELETE_PANEL': {
      const {panelId} = action.payload;
      return update(state, {$apply: (p) => {
        const panelsCopy = Object.assign({}, p);
        delete panelsCopy[panelId];
        return panelsCopy;
      }});
    }
  }

  return state;
}
